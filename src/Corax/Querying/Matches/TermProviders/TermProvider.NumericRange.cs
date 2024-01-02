﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow.Extensions;
using Voron.Data.Lookups;
using Range = Corax.Querying.Matches.Meta.Range;


namespace Corax.Querying.Matches.TermProviders
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct TermNumericRangeProvider<TLookupIterator, TLow, THigh, TVal> : ITermProvider
        where TLookupIterator : struct, ILookupIterator
        where TLow : struct, Range.Marker
        where THigh  : struct, Range.Marker
        where TVal : struct, ILookupKey
    {
        private readonly Querying.IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private TVal _low, _high;
        private TLookupIterator _iterator;
        private readonly bool _skipRangeCheck;
        private long _lastTermId = -1;
        private bool _includeLastTerm = true;
        private bool _isEmpty;

        public unsafe TermNumericRangeProvider(Querying.IndexSearcher searcher, Lookup<TVal> set, FieldMetadata field, TVal low, TVal high)
        {
            _searcher = searcher;
            _field = field;
            _iterator = set.Iterate<TLookupIterator>();
            _low = low;
            _high = high;

            // if (false && typeof(TVal) == typeof(DoubleLookupKey))
            // {
            //     var lowAsByteArray = ((DoubleLookupKey)(object)low).Value;
            //     var highAsByteArray = ((DoubleLookupKey)(object)high).Value;
            //     var ptrLow = new Span<byte>((byte*)(&lowAsByteArray), sizeof(double)/sizeof(byte));
            //     var ptrHigh = new Span<byte>((byte*)(&highAsByteArray), sizeof(double)/sizeof(byte));
            //     Console.WriteLine($"L: {low} ({string.Join(", ", ptrLow.ToArray())}) | L: {high} ({string.Join(", ", ptrHigh.ToArray())})");
            // }
            
            //Unbounded query can skip checking range after first element (since we're taking ALL possible results from starting point)
            _skipRangeCheck = _iterator.IsForward switch
            {
                true when typeof(THigh) == typeof(Range.Inclusive) && typeof(TVal) == typeof(Int64LookupKey) && ((Int64LookupKey)(object)high).Value == long.MaxValue => true,
                true when typeof(THigh) == typeof(Range.Inclusive) && typeof(TVal) == typeof(DoubleLookupKey) && ((DoubleLookupKey)(object)high).Value.AlmostEquals(double.MaxValue) => true,
                false when typeof(TLow) == typeof(Range.Inclusive) && typeof(TVal) == typeof(Int64LookupKey) && ((Int64LookupKey)(object)low).Value == long.MinValue => true,
                false when typeof(TLow) == typeof(Range.Inclusive) && typeof(TVal) == typeof(DoubleLookupKey) && ((DoubleLookupKey)(object)low).Value.AlmostEquals(double.MinValue) => true,
                _ => false
            };

            PrepareKeys();
            Reset();
        }

        private void PrepareKeys()
        {
            TVal startKey = _iterator.IsForward ? _low : _high;
            TVal finalKey = _iterator.IsForward ? _high : _low;

            _iterator.Seek(startKey);
            if (_iterator.MoveNext(out TVal key, out _, out _) == false)
            {
                _isEmpty = true;
                return;
            }

            bool skipFirst = false;
            if (_iterator.IsForward && typeof(TLow) == typeof(Range.Exclusive) && key.IsEqual(_low))
            {
                skipFirst = true;
            } else if (_iterator.IsForward == false)
            {
                if ((typeof(THigh) == typeof(Range.Exclusive) && _high.CompareTo(key) <= 0) || typeof(THigh) == typeof(Range.Inclusive) && _high.CompareTo(key) < 0)
                {
                    skipFirst = true;
                }
            }

            // var skipFirst = _iterator.IsForward switch
            // {
            //     true when typeof(TLow) == typeof(Range.Exclusive) && key.IsEqual(_low) => true,
            //     false when typeof(THigh) == typeof(Range.Exclusive) && _high.CompareTo(key) <= 0 => true,
            //     false when typeof(THigh) == typeof(Range.Inclusive) && _high.CompareTo(key) < 0 => true,
            //     _ => false
            // };

            if (skipFirst)
            {
                if (_iterator.MoveNext(out key, out _, out _) == false)
                {
                    _isEmpty = true;
                    return;
                }
                
                if (_iterator.IsForward)
                    _low = key;
                else
                    _high = key;
            }
            
            if (_skipRangeCheck)
                return; // to the end
            
            //Now seek to the last key
            _iterator.Seek(finalKey);
            if (_iterator.MoveNext(out key, out _lastTermId, out var hasPreviousValue) == false)
            {
                _includeLastTerm = false;
                return;
            }

            _includeLastTerm = true;
            if (_iterator.IsForward)
            {
                var cmp = _high.CompareTo(key);
                if (typeof(THigh) == typeof(Range.Exclusive) && cmp <= 0 ||
                    typeof(THigh) == typeof(Range.Inclusive) && cmp < 0)
                {
                    _includeLastTerm = false;
                }
            }
            else
            {
                var cmp = _low.CompareTo(key);
                if (typeof(TLow) == typeof(Range.Exclusive) && cmp >= 0 ||
                    typeof(TLow) == typeof(Range.Inclusive) && cmp > 0)
                    _includeLastTerm = false;
            }
        }

        public bool IsFillSupported => true;

        public int Fill(Span<long> containers)
        {
            if (_isEmpty) return 0;
            return _iterator.Fill(containers, _lastTermId, _includeLastTerm);
        }

        public void Reset()
        {
            if (_isEmpty)
                return;
            
            _iterator.Reset();
            _iterator.Seek(_iterator.IsForward ? _low : _high);
        }
        
        public bool Next(out TermMatch term)
        {
            if (_isEmpty)
            {
                term = default;
                return false;
            }
            bool hasNext = _iterator.MoveNext(out var termId);
            if (hasNext == false)
                goto Empty;


            if (termId == _lastTermId)
            {
                _isEmpty = true;
                if (_includeLastTerm == false)
                    goto Empty;
            }
            
            // Ratio will be always 1 (sizeof(T)/sizeof(T))
            term = _searcher.TermQuery(_field, termId, 1);
            return true;

            Empty:
            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"TermNumericRangeProvider<{typeof(TLookupIterator).Name}, {typeof(TLow).Name}, {typeof(THigh).Name}, {typeof(TVal).Name}>",
                            parameters: new Dictionary<string, string>()
                            {
                                { "Field", _field.ToString() },
                                { "Low", _low.ToString()},
                                { "High", _high.ToString()}
                            });
        }

        public string DebugView => Inspect().ToString();
    }
}
