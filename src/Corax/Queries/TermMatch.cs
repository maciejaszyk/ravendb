using System.Runtime.CompilerServices;
using Sparrow.Server.Compression;
using Voron.Data.Sets;
using Voron.Data.Containers;
using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Corax.Queries
{
    public unsafe struct TermMatch : IQueryMatch
    {
        private readonly delegate*<ref TermMatch, Span<long>, int> _fillFunc;
        private readonly delegate*<ref TermMatch, Span<long>, int> _andWithFunc;

        private readonly long _totalResults;
        private long _currentIdx;
        private long _baselineIdx;
        private long _current;

        private Container.Item _container;
        private Set.Iterator _set;

        public long Count => _totalResults;

        public QueryCountConfidence Confidence => QueryCountConfidence.High;

        private TermMatch(
            delegate*<ref TermMatch, Span<long>, int> fillFunc,
            delegate*<ref TermMatch, Span<long>, int> andWithFunc,
            long totalResults)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;
            _baselineIdx = QueryMatch.Start;
            _fillFunc = fillFunc;
            _andWithFunc = andWithFunc;

            _container = default;
            _set = default;
        }

        public static TermMatch CreateEmpty()
        {
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                term._currentIdx = QueryMatch.Invalid;
                term._current = QueryMatch.Invalid;
                return 0;
            }

            return new TermMatch(&FillFunc, &FillFunc, 0);
        }

        public static TermMatch YieldOnce(long value)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                if (term._currentIdx == QueryMatch.Start)
                {
                    term._currentIdx = term._current;
                    matches[0] = term._current;
                    return 1;
                }

                term._currentIdx = QueryMatch.Invalid;
                return 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc(ref TermMatch term, Span<long> matches)
            {
                // TODO: If matches is too big, we should use quicksort
                long current = term._current;
                for (int i = 0; i < matches.Length; i++)
                {
                    if (matches[i] == current)
                    {
                        matches[0] = current;
                        return 1;
                    }
                }
                return 0;
            }


            return new TermMatch(&FillFunc, &AndWithFunc, 1)
            {
                _current = value,
                _currentIdx = QueryMatch.Start
            };
        }

        public static TermMatch YieldSmall(Container.Item containerItem)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                // Fill needs to store resume capability.

                var stream = term._container.ToSpan();
                if (term._currentIdx == QueryMatch.Invalid || term._currentIdx >= stream.Length)
                {
                    term._currentIdx = QueryMatch.Invalid;
                    return 0;
                }

                int i = 0;
                for (; i < matches.Length; i++)
                {
                    term._current += ZigZagEncoding.Decode<long>(stream, out var len, (int)term._currentIdx);
                    term._currentIdx += len;
                    matches[i] = term._current;

                    if (term._currentIdx >= stream.Length)
                    {
                        i++;
                        break;
                    }                        
                }

                return i;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc(ref TermMatch term, Span<long> matches)
            {
                // AndWith has to start from the start.
                // TODO: Support Seek for the small set in order to have better behavior.

                var stream = term._container.ToSpan();

                // need to seek from start
                long current = 0;
                int currentIdx = (int)term._baselineIdx;

                int i = 0;
                int matchedIdx = 0;
                while (currentIdx < stream.Length && i < matches.Length)
                {
                    current += ZigZagEncoding.Decode<long>(stream, out var len, currentIdx);
                    currentIdx += len;

                    while (matches[i] < current)
                    {                        
                        i++;
                        if (i >= matches.Length)
                            goto End;
                    }                    

                    // If there is a match we advance. 
                    if (matches[i] == current)
                    {
                        matches[matchedIdx++] = current;
                        i++;
                    }
                }

                End:  return matchedIdx;
            }

            var itemsCount = ZigZagEncoding.Decode<int>(containerItem.ToSpan(), out var len);
            return new TermMatch(&FillFunc, &AndWithFunc, itemsCount)
            {
                _container = containerItem,
                _currentIdx = len,
                _baselineIdx = len,
                _current = 0
            };
        }

        public static TermMatch YieldSet(Set set, bool useAccelerated = true)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc(ref TermMatch term, Span<long> matches)
            {
                int matchedIdx = 0;

                var it = term._set;

                it.MaybeSeek(matches[0] - 1);

                // We update the current value we want to work with.
                var current = it.Current;

                // Check if there are matches left to process or is any posibility of a match to be available in this block.
                int i = 0;
                while (i < matches.Length && current <= matches[^1])
                {
                    // While the current match is smaller we advance.
                    while (matches[i] < current)
                    {
                        i++;
                        if (i >= matches.Length)
                            goto End;
                    }

                    // We are guaranteed that matches[i] is at least equal if not higher than current.
                    Debug.Assert(matches[i] >= current);

                    // We have a match, we include it into the matches and go on. 
                    if (current == matches[i])
                    {
                        matches[matchedIdx++] = current;
                        i++;
                    }

                    // We look into the next.
                    if (it.MoveNext() == false)
                        goto End;

                    current = it.Current;
                }

            End:
                term._set = it;
                term._current = current;
                return matchedIdx;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithVectorizedFunc(ref TermMatch term, Span<long> matches)
            {
                const int BlockSize = 4096;

                term._set.MaybeSeek(matches[0] - 1);

                // This is effectively a constant. 
                int N = Vector256<long>.Count;                

                long* dstStartPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(matches));                
                long* blockStartPtr = stackalloc long[BlockSize]; // The size of this array is fixed to improve cache locality. 
                long* inputStartPtr = stackalloc long[matches.Length];
                long* inputEndPtr = inputStartPtr + matches.Length;                

                // Copy array to temporary memory.
                Unsafe.CopyBlockUnaligned(
                    ref Unsafe.AsRef<byte>(inputStartPtr),
                    ref Unsafe.AsRef<byte>(dstStartPtr),
                    (uint)(matches.Length * sizeof(long)));

                int i = 0;
                long* inputPtr = inputStartPtr;
                long* dstPtr = dstStartPtr;
                Span<long> tmpMatches = new Span<long>(blockStartPtr, BlockSize);

                long maxMatchNumber = matches[^1];
                while (inputPtr < inputEndPtr)
                {
                    var result = term._set.Fill(tmpMatches, out int read, pruneGreaterThan: maxMatchNumber);
                    if (result == false)
                        break;

                    if (read == 0)
                        continue;

                    // Console.WriteLine($"Read: {read}|[{tmpMatches[0]},{tmpMatches[read-1]}], Matches: [{matches[0]},{matches[^1]}]");

                    int blocks = read / N;
                    long* blockPtr = blockStartPtr;
                    long* blockEndPtr = blockStartPtr + read;
                    while (blocks > 0 && inputPtr < inputEndPtr)
                    {
                        // TODO: In here we can do SIMD galloping with gather operations. Therefore we will be able to do
                        //       multiple checks at once and find the right amount of skipping using a table. 

                        // If the value to compare is bigger than the biggest element in the block, we advance the block. 
                        if (*inputPtr > *(blockPtr + N - 1))
                        {
                            blocks--;
                            blockPtr += N;
                            continue;
                        }

                        // If the value to compare is smaller than the smallest element in the block, we advance the scalar value.
                        if (*inputPtr < *blockPtr)
                        {
                            inputPtr++;
                            continue;
                        }

                        Vector256<long> value = Vector256.Create(*inputPtr);
                        Vector256<long> blockValues = Avx.LoadVector256(blockPtr);

                        // We are going to select which direction we are going to be moving forward. 
                        if (!Avx2.CompareEqual(value, blockValues).Equals(Vector256<long>.Zero))
                        {
                            // We found the value, therefore we need to store this value in the destination.
                            *dstPtr = *inputPtr;
                            dstPtr++;
                        }

                        inputPtr++;
                    }

                    // The scalar version. This shouldnt cost much either way. 
                    while (inputPtr < inputEndPtr && blockPtr < blockEndPtr)
                    {
                        long leftValue = *inputPtr;
                        long rightValue = *blockPtr;

                        if (leftValue > rightValue)
                        {
                            blockPtr++;
                        }
                        else if (leftValue < rightValue)
                        {
                            inputPtr++;
                        }
                        else
                        {
                            *dstPtr = leftValue;
                            dstPtr++;
                            inputPtr++;
                            blockPtr++;
                        }
                    }
                }

                return (int)( dstPtr - dstStartPtr );
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                int i = 0;
                var set = term._set;

                set.Fill(matches, out i);

                term._set = set;
                return i;
            }

            if (!Avx2.IsSupported)
                useAccelerated = false;

            useAccelerated = false;

            // We will select the AVX version if supported.             
            return new TermMatch(&FillFunc, useAccelerated ? &AndWithVectorizedFunc : &AndWithFunc, set.State.NumberOfEntries)
            {
                _set = set.Iterate(),
                _current = long.MinValue
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            return _fillFunc(ref this, matches);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> matches)
        {
            return _andWithFunc(ref this, matches);
        }
    }
}
