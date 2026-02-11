using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow.Server;
using Sparrow.Server.Utils;

namespace Corax.Querying.Matches
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct AndNotMatch<TInner, TOuter> : IQueryMatch
    where TInner : IQueryMatch
    where TOuter : IQueryMatch
    {
        private TInner _inner;
        private TOuter _outer;

        private long _totalResults;
        private QueryCountConfidence _confidence;
        private readonly CancellationToken _token;
        public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.Possible;

        public bool IsBoosting => _inner.IsBoosting || _outer.IsBoosting;
        public long Count => _totalResults;

        
        /// <summary>
        /// Indicates that the buffer is used by the AndWith method.
        /// </summary>
        private bool _isAndWithBuffer;

        private GrowableBitArray _results;
        private bool _loaded;
        private bool _done;

        private bool _doNotSortResults;
        private readonly IndexSearcher _indexSearcher;
        private long _lastReturnedEntryId;

        public SkipSortingResult AttemptToSkipSorting()
        {
            var r = _inner.AttemptToSkipSorting();
            // if the inner requires sorting, we also require it
            _doNotSortResults = r != SkipSortingResult.SortingIsRequired;
            return r;
        }

        public QueryCountConfidence Confidence => _confidence;

        private AndNotMatch(IndexSearcher searcher, 
            in TInner inner, in TOuter outer,
            long totalResults, QueryCountConfidence confidence, CancellationToken token)
        {
            _totalResults = totalResults;

            _inner = inner;
            _outer = outer;
            _confidence = confidence;
            _token = token;
            _indexSearcher = searcher;
            _isAndWithBuffer = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            if (_loaded == false && _done == false)
            {
                _results = new GrowableBitArray(_indexSearcher.Allocator, _indexSearcher.LastEntryId);
                using var outerResult = new GrowableBitArray(_indexSearcher.Allocator, _indexSearcher.LastEntryId); 
                
                while (_inner.Fill(matches) is var read and > 0)
                    _results.Add(matches[..read]);
                while (_outer.Fill(matches) is var read and > 0)
                    outerResult.Add(matches[..read]);
                
                outerResult.Invert();
                _results.And(outerResult);
                _loaded = true;
                _lastReturnedEntryId = 0;
            }
            
            var iterator = _results.GetIterator(_lastReturnedEntryId);
            var currentIdx = iterator.Fill(matches);

            if (currentIdx == 0)
            {
                _done = true;
                _results.Dispose();
                return 0;
            }
            
            _lastReturnedEntryId = matches[currentIdx - 1] + 1;
            return currentIdx;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            throw new NotSupportedException($"{nameof(AndNotMatch)} does not support the operation of {nameof(AndWith)}.");
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(BinaryMatch)} [AndNot]",
                children: new List<QueryInspectionNode> { _inner.Inspect(), _outer.Inspect() },
                parameters: new Dictionary<string, string>()
                {
                    { Constants.QueryInspectionNode.IsBoosting, IsBoosting.ToString() },
                    { Constants.QueryInspectionNode.Count, Count.ToString() },
                    { Constants.QueryInspectionNode.CountConfidence, Confidence.ToString() }
                });
        }

        string DebugView => Inspect().ToString();


        public static AndNotMatch<TInner, TOuter> Create(IndexSearcher searcher, in TInner inner, in TOuter outer, in CancellationToken token)
        {
            // Estimate Confidence values.
            QueryCountConfidence confidence;
            if (inner.Count < outer.Count / 2)
                confidence = inner.Confidence;
            else if (outer.Count < inner.Count / 2)
                confidence = outer.Confidence;
            else
                confidence = inner.Confidence.Min(outer.Confidence);

            return new AndNotMatch<TInner, TOuter>(searcher, in inner, in outer, inner.Count, confidence, token);
        }
    }
}
