using Sparrow.Server.Compression;
using Voron.Data.PostingLists;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Corax.Utils;
using Sparrow.Server;
using Sparrow.Server.Binary;
using Voron.Data.Containers;
using Voron.Data.Sets;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct TermMatch : IQueryMatch
    {
        private readonly delegate*<ref TermMatch, Span<long>, int> _fillFunc;
        private readonly delegate*<ref TermMatch, Span<long>, int, int> _andWithFunc;
        private readonly delegate*<ref TermMatch, Span<long>, Span<float>, void> _scoreFunc;
        private readonly delegate*<ref TermMatch, QueryInspectionNode> _inspectFunc;

        private readonly long _totalResults;
        private long _currentIdx;
        private long _numOfReturnedItems;
        private long _baselineIdx;
        private long _current;
        internal Bm25 _bm25;
        private Container.Item _container;
        private PostingList.Iterator _set;
        private ByteStringContext _ctx;
        private IndexSearcher _indexSearcher;
        public bool IsBoosting => _scoreFunc != null;
        public long Count => _totalResults;
        private float _currentFreq;

#if DEBUG
        public string Term;
#endif

        public QueryCountConfidence Confidence => QueryCountConfidence.High;

        private TermMatch(
            IndexSearcher indexSearcher,
            ByteStringContext ctx,
            long totalResults,
            delegate*<ref TermMatch, Span<long>, int> fillFunc,
            delegate*<ref TermMatch, Span<long>, int, int> andWithFunc,
            delegate*<ref TermMatch, Span<long>, Span<float>, void> scoreFunc = null,
            delegate*<ref TermMatch, QueryInspectionNode> inspectFunc = null)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;
            _baselineIdx = QueryMatch.Start;
            _fillFunc = fillFunc;
            _andWithFunc = andWithFunc;
            _scoreFunc = scoreFunc;
            _inspectFunc = inspectFunc;
            _ctx = ctx;
            _indexSearcher = indexSearcher;
            _numOfReturnedItems = 0;
            _container = default;
            _set = default;
#if DEBUG
            Term = null;
#endif
        }

        public static TermMatch CreateEmpty(IndexSearcher indexSearcher, ByteStringContext ctx)
        {
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                term._currentIdx = QueryMatch.Invalid;
                term._current = QueryMatch.Invalid;
                return 0;
            }

            static int AndWithFunc(ref TermMatch term, Span<long> buffer, int matches)
            {
                term._currentIdx = QueryMatch.Invalid;
                term._current = QueryMatch.Invalid;
                return 0;
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [Empty]",
                    parameters: new Dictionary<string, string>()
                    {
                        {nameof(term.IsBoosting), term.IsBoosting.ToString()}, {nameof(term.Count), $"{term.Count} [{term.Confidence}]"}
                    });
            }

            return new TermMatch(indexSearcher, ctx, 0, &FillFunc, &AndWithFunc, inspectFunc: &InspectFunc)
            {
                _indexSearcher = indexSearcher
#if DEBUG
                ,
                Term = "<empty>"
#endif
            };
        }

        public static TermMatch YieldOnceNoFreq(IndexSearcher indexSearcher, ByteStringContext ctx, long value)
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
            static int AndWithFunc(ref TermMatch term, Span<long> buffer, int matches)
            {
                uint bot = 0;
                uint top = (uint)matches;

                long current = term._current;
                while (top > 1)
                {
                    uint mid = top / 2;

                    if (current >= Unsafe.Add(ref MemoryMarshal.GetReference(buffer), bot + mid))
                        bot += mid;
                    top -= mid;
                }

                return 0;
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [Once]",
                    parameters: new Dictionary<string, string>()
                    {
                        {nameof(term.IsBoosting), term.IsBoosting.ToString()}, {nameof(term.Count), $"{term.Count} [{term.Confidence}]"}
                    });
            }

            return new TermMatch(indexSearcher, ctx, 1, &FillFunc, &AndWithFunc, inspectFunc: &InspectFunc)
            {
                _indexSearcher = indexSearcher, _current = FrequencyUtils.RemoveFrequency(value), _currentIdx = QueryMatch.Start
            };
        }

        public static TermMatch YieldSmallNoFreq(IndexSearcher indexSearcher, ByteStringContext ctx, Container.Item containerItem)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                // Fill needs to store resume capability.

                var stream = term._container.ToSpan();
                if (term._currentIdx == QueryMatch.Invalid)
                {
                    term._currentIdx = QueryMatch.Invalid;
                    return 0;
                }

                int i = 0;
                for (; i < matches.Length && term._numOfReturnedItems < term._totalResults; i++)
                {
                    term._current += ZigZagEncoding.Decode<long>(stream, out var len, (int)term._currentIdx);
                    term._currentIdx += len;
                    matches[i] = term._current;
                    term._numOfReturnedItems++;
                }

                FrequencyUtils.RemoveFrequencies(matches.Slice(0, i));

                return i;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc(ref TermMatch term, Span<long> buffer, int matches)
            {
                // AndWith has to start from the start.
                // TODO: Support Seek for the small set in order to have better behavior.

                var stream = term._container.ToSpan();

                // need to seek from start
                long current = 0;
                int currentIdx = (int)term._baselineIdx;
                var itemsScanned = 0L;
                var currentWithoutFreq = 0L;
                int i = 0;
                int matchedIdx = 0;
                while (itemsScanned < term._totalResults && i < matches)
                {
                    current += ZigZagEncoding.Decode<long>(stream, out var len, currentIdx);
                    currentIdx += len;
                    itemsScanned++;
                    currentWithoutFreq = FrequencyUtils.RemoveFrequency(current);
                    while (buffer[i] < currentWithoutFreq)
                    {
                        i++;
                        if (i >= matches)
                            goto End;
                    }

                    // If there is a match we advance. 
                    if (buffer[i] == currentWithoutFreq)
                    {
                        buffer[matchedIdx++] = currentWithoutFreq;
                        i++;
                    }
                }

                End:
                return matchedIdx;
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [SmallSet]",
                    parameters: new Dictionary<string, string>()
                    {
                        {nameof(term.IsBoosting), term.IsBoosting.ToString()}, {nameof(term.Count), $"{term.Count} [{term.Confidence}]"}
                    });
            }

            var itemsCount = ZigZagEncoding.Decode<int>(containerItem.ToSpan(), out var len);
            return new TermMatch(indexSearcher, ctx, itemsCount, &FillFunc, &AndWithFunc, inspectFunc: &InspectFunc)
            {
                _indexSearcher = indexSearcher,
                _container = containerItem,
                _currentIdx = len,
                _baselineIdx = len,
                _current = 0
            };
        }

        public static TermMatch YieldSetNoFreq(IndexSearcher indexSearcher, ByteStringContext ctx, PostingList postingList, bool useAccelerated = true)
        {
            [SkipLocalsInit]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc(ref TermMatch term, Span<long> buffer, int matches)
            {
                int matchedIdx = 0;

                var it = term._set;

                ref long start = ref MemoryMarshal.GetReference(buffer);
                it.Seek(start - 1);
                if (it.MoveNext() == false)
                    goto Fail;

                // We update the current value we want to work with.
                var current = FrequencyUtils.RemoveFrequency(it.Current);

                // Check if there are matches left to process or is any possibility of a match to be available in this block.
                int i = 0;
                while (i < matches && current <= buffer[matches - 1])
                {
                    // While the current match is smaller we advance.
                    while (Unsafe.Add(ref start, i) < current)
                    {
                        i++;
                        if (i >= matches)
                            goto End;
                    }

                    // We are guaranteed that matches[i] is at least equal if not higher than current.
                    Debug.Assert(buffer[i] >= current);

                    // We have a match, we include it into the matches and go on. 
                    if (current == Unsafe.Add(ref start, i))
                    {
                        ref long location = ref Unsafe.Add(ref start, matchedIdx++);
                        location = current;
                        i++;
                    }

                    // We look into the next.
                    if (it.MoveNext() == false)
                        goto End;

                    current = FrequencyUtils.RemoveFrequency(it.Current);
                }

                End:
                term._set = it;
                term._current = current;
                return matchedIdx;

                Fail:
                term._set = it;
                return 0;
            }

            [SkipLocalsInit]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithVectorizedFunc(ref TermMatch term, Span<long> buffer, int matches)
            {
                const int BlockSize = 4096;
                uint N = (uint)Vector256<long>.Count;

                Debug.Assert(Vector256<long>.Count == 4);

                term._set.Seek(buffer[0] - 1);
                
                // PERF: The AND operation can be performed in place, because we end up writing the same value that we already read. 
                fixed (long* inputStartPtr = buffer)
                {
                    long* inputEndPtr = inputStartPtr + matches;

                    // The size of this array is fixed to improve cache locality.
                    using var _ = term._ctx.Allocate(BlockSize * sizeof(long), out var bufferHolder);
                    var blockMatches = MemoryMarshal.Cast<byte, long>(bufferHolder.ToSpan());
                    Debug.Assert(blockMatches.Length == BlockSize);

                    long* blockStartPtr = (long*)bufferHolder.Ptr;

                    long* inputPtr = inputStartPtr;
                    long* dstPtr = inputStartPtr;
                    while (inputPtr < inputEndPtr)
                    {
                        var result = term._set.Fill(blockMatches, out int read);//, pruneGreaterThanOptimization: buffer[matches - 1]); //todo: ask federico 'bout that 
                        FrequencyUtils.RemoveFrequencies(blockMatches.Slice(0, read));
                        MemoryExtensions.Sort(blockMatches.Slice(0, read));

                        if (result == false)
                            break;
                        
                        Debug.Assert(read <= BlockSize);

                        if (read == 0)
                            continue;

                        long* smallerPtr, largerPtr;
                        long* smallerEndPtr, largerEndPtr;

                        bool applyVectorization;

                        // See: MergeHelper.AndVectorized
                        // read => leftLength
                        // matches => rightLength
                        bool isSmallerInput;
                        if (read < (inputEndPtr - inputPtr))
                        {
                            smallerPtr = blockStartPtr;
                            smallerEndPtr = blockStartPtr + read;
                            isSmallerInput = false;
                            largerPtr = inputPtr;
                            largerEndPtr = inputEndPtr;
                            applyVectorization = matches > N && read > 0;
                        }
                        else
                        {
                            smallerPtr = inputPtr;
                            smallerEndPtr = inputEndPtr;
                            isSmallerInput = true;
                            largerPtr = blockStartPtr;
                            largerEndPtr = blockStartPtr + read;
                            applyVectorization = read > N && matches > 0;
                        }

                        Debug.Assert((ulong)(smallerEndPtr - smallerPtr) <= (ulong)(largerEndPtr - largerPtr));

                        if (applyVectorization)
                        {
                            while (true)
                            {
                                // TODO: In here we can do SIMD galloping with gather operations. Therefore we will be able to do
                                //       multiple checks at once and find the right amount of skipping using a table. 

                                // If the value to compare is bigger than the biggest element in the block, we advance the block. 
                                if ((ulong)*smallerPtr > (ulong)*(largerPtr + N - 1))
                                {
                                    if (largerPtr + N >= largerEndPtr)
                                        break;

                                    largerPtr += N;
                                    continue;
                                }

                                // If the value to compare is smaller than the smallest element in the block, we advance the scalar value.
                                if ((ulong)*smallerPtr < (ulong)*largerPtr)
                                {
                                    smallerPtr++;
                                    if (smallerPtr >= smallerEndPtr)
                                        break;

                                    continue;
                                }

                                Vector256<ulong> value = Vector256.Create((ulong)*smallerPtr);
                                Vector256<ulong> blockValues = Avx.LoadVector256((ulong*)largerPtr);

                                // We are going to select which direction we are going to be moving forward. 
                                if (!Avx2.CompareEqual(value, blockValues).Equals(Vector256<ulong>.Zero))
                                {
                                    // We found the value, therefore we need to store this value in the destination.
                                    *dstPtr = *smallerPtr;
                                    dstPtr++;
                                }

                                smallerPtr++;
                                if (smallerPtr >= smallerEndPtr)
                                    break;
                            }
                        }

                        // The scalar version. This shouldn't cost much either way. 
                        while (smallerPtr < smallerEndPtr && largerPtr < largerEndPtr)
                        {
                            ulong leftValue = (ulong)*smallerPtr;
                            ulong rightValue = (ulong)*largerPtr;

                            if (leftValue > rightValue)
                            {
                                largerPtr++;
                            }
                            else if (leftValue < rightValue)
                            {
                                smallerPtr++;
                            }
                            else
                            {
                                *dstPtr = (long)leftValue;
                                dstPtr++;
                                smallerPtr++;
                                largerPtr++;
                            }
                        }

                        inputPtr = isSmallerInput ? smallerPtr : largerPtr;

                        ////Debug.Assert(inputPtr >= dstPtr);
                        ///Debug.Assert((isSmallerInput ? largerPtr : smallerPtr) - blockStartPtr <= BlockSize);
                    }

                    return (int)((ulong)dstPtr - (ulong)inputStartPtr) / sizeof(ulong);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                int i = 0;
                var set = term._set;

                set.Fill(matches, out i);
                FrequencyUtils.RemoveFrequencies(matches.Slice(0, i));

                term._set = set;
                return i;
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [Set]",
                    parameters: new Dictionary<string, string>()
                    {
                        {nameof(term.IsBoosting), term.IsBoosting.ToString()}, {nameof(term.Count), $"{term.Count} [{term.Confidence}]"}
                    });
            }

            if (Avx2.IsSupported == false)
                useAccelerated = false;

            // We will select the AVX version if supported.             
            return new TermMatch(indexSearcher, ctx, postingList.State.NumberOfEntries, &FillFunc,
                useAccelerated
                    ? &AndWithVectorizedFunc
                    : &AndWithFunc,
                inspectFunc: &InspectFunc) {_indexSearcher = indexSearcher, _set = postingList.Iterate(), _current = long.MinValue};
        }

        public static TermMatch YieldOnceWithFreq(IndexSearcher indexSearcher, ByteStringContext ctx, long value)
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
            static int AndWithFunc(ref TermMatch term, Span<long> buffer, int matches)
            {
                // TODO: If matches is too big, we should use quicksort
                long current = term._current;
                for (int i = 0; i < matches; i++)
                {
                    if (buffer[i] == current)
                    {
                        buffer[0] = current;
                        return 1;
                    }
                }

                return 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static void ScoreFunc(ref TermMatch term, Span<long> matches, Span<float> scores)
            {
                term._bm25.Score(matches, scores);
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [Once]",
                    parameters: new Dictionary<string, string>()
                    {
                        {nameof(term.IsBoosting), term.IsBoosting.ToString()}, {nameof(term.Count), $"{term.Count} [{term.Confidence}]"}
                    });
            }

            Bm25 bm25 = new(indexSearcher, 1, ctx, 1);
            bm25.Add(value);

            return new TermMatch(indexSearcher, ctx, 1, &FillFunc, &AndWithFunc, scoreFunc: &ScoreFunc, inspectFunc: &InspectFunc)
            {
                _indexSearcher = indexSearcher, _current = bm25.Matches[0], _currentIdx = QueryMatch.Start, _bm25 = bm25
            };
        }

        public static TermMatch YieldSmallWithFreq(IndexSearcher indexSearcher, ByteStringContext ctx, Container.Item containerItem)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                // Fill needs to store resume capability.

                var stream = term._container.ToSpan();
                if (term._currentIdx == QueryMatch.Invalid)
                {
                    term._currentIdx = QueryMatch.Invalid;
                    return 0;
                }

                int i = 0;
                for (; i < matches.Length && term._numOfReturnedItems < term._totalResults; i++)
                {
                    term._current += ZigZagEncoding.Decode<long>(stream, out var len, (int)term._currentIdx);
                    term._currentIdx += len;
                    matches[i] = term._current;
                    term._numOfReturnedItems++;
                }

                //Save the frequencies
                term._bm25.Process(matches, i);

                return i;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc(ref TermMatch term, Span<long> buffer, int matches)
            {
                // AndWith has to start from the start.
                // TODO: Support Seek for the small set in order to have better behavior.

                var stream = term._container.ToSpan();

                // need to seek from start
                long current = 0;
                int currentIdx = (int)term._baselineIdx;
                var itemsScanned = 0L;

                int i = 0;
                int matchedIdx = 0;
                long decodedEntryId;
                while (itemsScanned < term._totalResults && i < matches)
                {
                    current += ZigZagEncoding.Decode<long>(stream, out var len, currentIdx);
                    currentIdx += len;
                    itemsScanned++;
                    decodedEntryId = term._bm25.Add(current);

                    while (buffer[i] < decodedEntryId)
                    {
                        i++;
                        if (i >= matches)
                        {
                            //no match, we should discard last item.
                            term._bm25.Remove();
                            goto End;
                        }
                    }

                    // If there is a match we advance. 
                    if (buffer[i] == decodedEntryId)
                    {
                        buffer[matchedIdx++] = decodedEntryId;
                        i++;
                    }
                }

                End:
                return matchedIdx;
            }

            static void ScoreFunc(ref TermMatch term, Span<long> matches, Span<float> scores)
            {
                term._bm25.Score(matches, scores);
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [SmallSet]",
                    parameters: new Dictionary<string, string>()
                    {
                        {nameof(term.IsBoosting), term.IsBoosting.ToString()}, {nameof(term.Count), $"{term.Count} [{term.Confidence}]"}
                    });
            }

            var itemsCount = ZigZagEncoding.Decode<int>(containerItem.ToSpan(), out var len);
            return new TermMatch(indexSearcher, ctx, itemsCount, &FillFunc, &AndWithFunc, inspectFunc: &InspectFunc, scoreFunc: &ScoreFunc)
            {
                _indexSearcher = indexSearcher,
                _bm25 = new(indexSearcher, itemsCount, ctx, itemsCount),
                _container = containerItem,
                _currentIdx = len,
                _baselineIdx = len,
                _current = 0
            };
        }
        
        public static TermMatch YieldSetWithFreq(IndexSearcher indexSearcher, ByteStringContext ctx, PostingList postingList, bool useAccelerated = true)
        {
            [SkipLocalsInit]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc(ref TermMatch term, Span<long> buffer, int matches)
            {
                int matchedIdx = 0;

                var it = term._set;

                it.MaybeSeek(buffer[0] - 1);
                if (it.MoveNext() == false)
                    goto Fail;

                // We update the current value we want to work with.
                var current = term._bm25.Add(it.Current);

                // Check if there are matches left to process or is any posibility of a match to be available in this block.
                int i = 0;
                while (i < matches && current <= buffer[matches - 1])
                {
                    // While the current match is smaller we advance.
                    while (buffer[i] < current)
                    {
                        i++;
                        if (i >= matches)
                            goto End;
                    }

                    // We are guaranteed that matches[i] is at least equal if not higher than current.
                    Debug.Assert(buffer[i] >= current);

                    // We have a match, we include it into the matches and go on. 
                    if (current == buffer[i])
                    {
                        buffer[matchedIdx++] = current;
                        i++;
                    }

                    // We look into the next.
                    if (it.MoveNext() == false)
                        goto End;

                    current = term._bm25.Add(it.Current);
                }

                End:
                term._set = it;
                term._current = current;
                return matchedIdx;

                Fail:
                term._set = it;
                return 0;
            }

            [SkipLocalsInit]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithVectorizedFunc(ref TermMatch term, Span<long> buffer, int matches)
            {
                const int BlockSize = 4096;
                uint N = (uint)Vector256<long>.Count;

                Debug.Assert(Vector256<long>.Count == 4);

                term._set.MaybeSeek(buffer[0] - 1);

                // PERF: The AND operation can be performed in place, because we end up writing the same value that we already read. 
                fixed (long* inputStartPtr = buffer)
                {
                    long* inputEndPtr = inputStartPtr + matches;

                    // The size of this array is fixed to improve cache locality.
                    using var _ = term._ctx.Allocate(BlockSize * sizeof(long), out var bufferHolder);
                    var blockMatches = MemoryMarshal.Cast<byte, long>(bufferHolder.ToSpan());
                    Debug.Assert(blockMatches.Length == BlockSize);

                    long* blockStartPtr = (long*)bufferHolder.Ptr;

                    long* inputPtr = inputStartPtr;
                    long* dstPtr = inputStartPtr;
                    while (inputPtr < inputEndPtr)
                    {
                        var result = term._set.Fill(blockMatches, out int read, pruneGreaterThanOptimization: buffer[matches - 1]);
                        if (result == false)
                            break;

                        term._bm25.Process(blockMatches, read);

                        Debug.Assert(read < BlockSize);

                        if (read == 0)
                            continue;

                        long* smallerPtr, largerPtr;
                        long* smallerEndPtr, largerEndPtr;

                        bool applyVectorization;

                        // See: MergeHelper.AndVectorized
                        // read => leftLength
                        // matches => rightLength
                        bool isSmallerInput;
                        if (read < (inputEndPtr - inputPtr))
                        {
                            smallerPtr = blockStartPtr;
                            smallerEndPtr = blockStartPtr + read;
                            isSmallerInput = false;
                            largerPtr = inputPtr;
                            largerEndPtr = inputEndPtr;
                            applyVectorization = matches > N && read > 0;
                        }
                        else
                        {
                            smallerPtr = inputPtr;
                            smallerEndPtr = inputEndPtr;
                            isSmallerInput = true;
                            largerPtr = blockStartPtr;
                            largerEndPtr = blockStartPtr + read;
                            applyVectorization = read > N && matches > 0;
                        }

                        Debug.Assert((ulong)(smallerEndPtr - smallerPtr) <= (ulong)(largerEndPtr - largerPtr));

                        if (applyVectorization)
                        {
                            while (true)
                            {
                                // TODO: In here we can do SIMD galloping with gather operations. Therefore we will be able to do
                                //       multiple checks at once and find the right amount of skipping using a table. 

                                // If the value to compare is bigger than the biggest element in the block, we advance the block. 
                                if ((ulong)*smallerPtr > (ulong)*(largerPtr + N - 1))
                                {
                                    if (largerPtr + N >= largerEndPtr)
                                        break;

                                    largerPtr += N;
                                    continue;
                                }

                                // If the value to compare is smaller than the smallest element in the block, we advance the scalar value.
                                if ((ulong)*smallerPtr < (ulong)*largerPtr)
                                {
                                    smallerPtr++;
                                    if (smallerPtr >= smallerEndPtr)
                                        break;

                                    continue;
                                }

                                Vector256<ulong> value = Vector256.Create((ulong)*smallerPtr);
                                Vector256<ulong> blockValues = Avx.LoadVector256((ulong*)largerPtr);

                                // We are going to select which direction we are going to be moving forward. 
                                if (!Avx2.CompareEqual(value, blockValues).Equals(Vector256<ulong>.Zero))
                                {
                                    // We found the value, therefore we need to store this value in the destination.
                                    *dstPtr = *smallerPtr;
                                    dstPtr++;
                                }

                                smallerPtr++;
                                if (smallerPtr >= smallerEndPtr)
                                    break;
                            }
                        }

                        // The scalar version. This shouldnt cost much either way. 
                        while (smallerPtr < smallerEndPtr && largerPtr < largerEndPtr)
                        {
                            ulong leftValue = (ulong)*smallerPtr;
                            ulong rightValue = (ulong)*largerPtr;

                            if (leftValue > rightValue)
                            {
                                largerPtr++;
                            }
                            else if (leftValue < rightValue)
                            {
                                smallerPtr++;
                            }
                            else
                            {
                                *dstPtr = (long)leftValue;
                                dstPtr++;
                                smallerPtr++;
                                largerPtr++;
                            }
                        }

                        inputPtr = isSmallerInput ? smallerPtr : largerPtr;

                        Debug.Assert(inputPtr >= dstPtr);
                        Debug.Assert((isSmallerInput ? largerPtr : smallerPtr) - blockStartPtr <= BlockSize);
                    }

                    return (int)((ulong)dstPtr - (ulong)inputStartPtr) / sizeof(ulong);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                int i = 0;
                var set = term._set;

                set.Fill(matches, out i);
                term._bm25.Process(matches, i);

                term._set = set;
                return i;
            }

            static QueryInspectionNode InspectFunc(ref TermMatch term)
            {
                return new QueryInspectionNode($"{nameof(TermMatch)} [Set]",
                    parameters: new Dictionary<string, string>()
                    {
                        {nameof(term.IsBoosting), term.IsBoosting.ToString()}, {nameof(term.Count), $"{term.Count} [{term.Confidence}]"}
                    });
            }

            static void ScoreFunc(ref TermMatch term, Span<long> matches, Span<float> scores)
            {
                term._bm25.Score(matches, scores);
            }
            
            if (Avx2.IsSupported == false)
                useAccelerated = false;
            
            // We will select the AVX version if supported.             
            return new TermMatch(indexSearcher, ctx, postingList.State.NumberOfEntries, &FillFunc,
                useAccelerated
                    ? &AndWithVectorizedFunc
                    : &AndWithFunc,
                inspectFunc: &InspectFunc,
                scoreFunc: &ScoreFunc)
            {
                _indexSearcher = indexSearcher, 
                _set = postingList.Iterate(), 
                _current = long.MinValue,
                _bm25 = new Bm25(indexSearcher, set.State.NumberOfEntries, ctx, (int)set.State.NumberOfEntries),
            };
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            return _fillFunc(ref this, matches);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            return _andWithFunc(ref this, buffer, matches);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores)
        {
            if (_scoreFunc == null)
                return; // We ignore. Nothing to do here. 

            _scoreFunc(ref this, matches, scores);
        }

        public QueryInspectionNode Inspect()
        {
            return _inspectFunc is null ? QueryInspectionNode.NotInitializedInspectionNode(nameof(TermMatch)) : _inspectFunc(ref this);
        }

        string DebugView => Inspect().ToString();
    }
}
