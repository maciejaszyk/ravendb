using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Voron;

namespace Corax.Queries
{
    unsafe partial struct UnaryMatch<TInner, TValueType>
    {
        private interface IUnaryMatchComparer
        {
            bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy);
            bool Compare<T>(T sx, T sy) where T : unmanaged;
        }

        [SkipLocalsInit]
        private static int AndWith(ref UnaryMatch<TInner, TValueType> match, Span<long> buffer, int matches)
        {
            var bufferHolder = QueryContext.MatchesRawPool.Rent(sizeof(long) * buffer.Length);
            var innerBuffer = MemoryMarshal.Cast<byte, long>(bufferHolder).Slice(0, buffer.Length);
            Debug.Assert(innerBuffer.Length == buffer.Length);

            var count = match._fillFunc(ref match, innerBuffer);            
            
            var matchesPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buffer));
            var baseMatchesPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(innerBuffer));
            var result = MergeHelper.And(matchesPtr, buffer.Length, matchesPtr, matches, baseMatchesPtr, count);

            QueryContext.MatchesRawPool.Return(bufferHolder);
            return result;
        }

        [SkipLocalsInit]
        private static int FillFuncSequence<TComparer>(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
            where TComparer : struct, IUnaryMatchComparer
        {
            var searcher = match._searcher;
            var currentType = ((Slice)(object)match._value).AsReadOnlySpan();

            var comparer = default(TComparer);
            var currentMatches = matches;
            int totalResults = 0;
            int storeIdx = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;

            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);

                if (results == 0)
                    return totalResults;

                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetReaderFor(freeMemory[i]);
                    var type = reader.GetFieldType(match._fieldId, out var intOffset);
                    var isMatch = false;

                    if (type is IndexEntryFieldType.Tuple or IndexEntryFieldType.None)
                    {
                        var read = reader.Read(match._fieldId, out var resultX);
                        if (read && comparer.Compare(currentType, resultX))
                        {
                            // We found a match.
                            isMatch = true;
                        }
                    }
                    else if (type is IndexEntryFieldType.List)
                    {
                        var iterator = reader.ReadMany(match._fieldId);
                        var unacceptableItem = false;
                        while (iterator.ReadNext())
                        {
                            if (comparer.Compare(currentType, iterator.Sequence) == false)
                            {
                                unacceptableItem = true;
                                break;
                            }
                        }

                        isMatch = unacceptableItem == false;
                    }

                    if (isMatch)
                    {
                        currentMatches[storeIdx] = freeMemory[i];
                        storeIdx++;
                        totalResults++;
                    }
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            return storeIdx;
        }

        [SkipLocalsInit]
        private static int FillFuncNumerical<TComparer>(ref UnaryMatch<TInner, TValueType> match, Span<long> matches)
            where TComparer : struct, IUnaryMatchComparer
        {
            var currentType = match._value;

            var comparer = default(TComparer);
            var searcher = match._searcher;
            var currentMatches = matches;
            int totalResults = 0;
            int maxUnusedMatchesSlots = matches.Length >= 64 ? matches.Length / 8 : 1;
            int storeIdx = 0;

            int results;
            do
            {
                var freeMemory = currentMatches.Slice(storeIdx);
                results = match._inner.Fill(freeMemory);
                if (results == 0)
                    return totalResults;

                for (int i = 0; i < results; i++)
                {
                    var reader = searcher.GetReaderFor(freeMemory[i]);
                    var type = reader.GetFieldType(match._fieldId, out _);

                    bool isMatch = false;
                    if (typeof(TValueType) == typeof(long))
                    {
                        if (type is IndexEntryFieldType.Tuple or IndexEntryFieldType.None)
                        {
                            var read = reader.Read<long>(match._fieldId, out var resultX);
                            if (read)
                                isMatch = comparer.Compare((long)(object)currentType, resultX);
                        }
                        else if (type is IndexEntryFieldType.List)
                        {
                            var iterator = reader.ReadMany(match._fieldId);
                            var unacceptableItem = false;
                            while (iterator.ReadNext())
                            {
                                if (comparer.Compare((long)(object)currentType, iterator.Long) == false)
                                {
                                    unacceptableItem = true;
                                    break;
                                }
                            }

                            isMatch = unacceptableItem == false;
                        }
                    }
                    else if (typeof(TValueType) == typeof(double))
                    {
                        if (type is IndexEntryFieldType.Tuple or IndexEntryFieldType.None)
                        {
                            var read = reader.Read<double>(match._fieldId, out var resultX);
                            if (read)
                                isMatch = comparer.Compare((double)(object)currentType, resultX);
                        }
                        else if (type is IndexEntryFieldType.List)
                        {
                            var iterator = reader.ReadMany(match._fieldId);
                            var unacceptableItem = false;
                            while (iterator.ReadNext())
                            {
                                if (comparer.Compare((double)(object)currentType, iterator.Double) == false)
                                {
                                    unacceptableItem = true;
                                    break;
                                }
                            }

                            isMatch = unacceptableItem == false;
                        }
                    }

                    if (isMatch)
                    {
                        // We found a match.
                        currentMatches[storeIdx] = freeMemory[i];
                        storeIdx++;
                        totalResults++;
                    }
                }
            } while (results >= totalResults + maxUnusedMatchesSlots);

            matches = currentMatches.Slice(0, storeIdx);
            return totalResults;
        }

        public static UnaryMatch<TInner, TValueType> YieldGreaterThan(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThan,
                    searcher, fieldId, value,
                    &FillFuncSequence<GreaterThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThan,
                    searcher, fieldId, value,
                    &FillFuncNumerical<GreaterThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldGreaterThanOrEqualMatch(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThanOrEqual,
                    searcher, fieldId, value,
                    &FillFuncSequence<GreaterThanOrEqualMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.GreaterThanOrEqual,
                    searcher, fieldId, value,
                    &FillFuncNumerical<GreaterThanOrEqualMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldLessThan(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThan,
                    searcher, fieldId, value,
                    &FillFuncSequence<LessThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThan,
                    searcher, fieldId, value,
                    &FillFuncNumerical<LessThanMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldLessThanOrEqualMatch(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThanOrEqual,
                    searcher, fieldId, value,
                    &FillFuncSequence<LessThanOrEqualMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.LessThanOrEqual,
                    searcher, fieldId, value,
                    &FillFuncNumerical<LessThanOrEqualMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldNotEqualsMatch(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotEquals,
                    searcher, fieldId, value,
                    &FillFuncSequence<NotEqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.NotEquals,
                    searcher, fieldId, value,
                    &FillFuncNumerical<NotEqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
        }

        public static UnaryMatch<TInner, TValueType> YieldEqualsMatch(in TInner inner, IndexSearcher searcher, int fieldId, TValueType value, int take = -1)
        {
            if (typeof(TValueType) == typeof(Slice))
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.Equals,
                    searcher, fieldId, value,
                    &FillFuncSequence<EqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
            else
            {
                return new UnaryMatch<TInner, TValueType>(
                    in inner, UnaryMatchOperation.Equals,
                    searcher, fieldId, value,
                    &FillFuncNumerical<EqualsMatchComparer>, &AndWith,
                    inner.Count, inner.Confidence.Min(QueryCountConfidence.Normal), take);
            }
        }

        private struct GreaterThanMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) > 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy - (long)(object)sx) > 0;
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy - (double)(object)sx) > 0;

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }

        private struct GreaterThanOrEqualMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) >= 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy - (long)(object)sx) >= 0;
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy - (double)(object)sx) >= 0;

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }

        private struct LessThanMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) < 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy - (long)(object)sx) < 0;
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy - (double)(object)sx) < 0;

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }

        private struct LessThanOrEqualMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) <= 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy - (long)(object)sx) <= 0;
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy - (double)(object)sx) <= 0;

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }

        private struct NotEqualsMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) != 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy != (long)(object)sx);
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy != (double)(object)sx);

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }

        private struct EqualsMatchComparer : IUnaryMatchComparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return sy.SequenceCompareTo(sx) == 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            public bool Compare<T>(T sx, T sy) where T : unmanaged
            {
                if (typeof(T) == typeof(long))
                    return ((long)(object)sy == (long)(object)sx);
                if (typeof(T) == typeof(double))
                    return ((double)(object)sy == (double)(object)sx);

                throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
            }
        }
    }
}
