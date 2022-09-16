﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Search;
using Raven.Server.Documents.Handlers.Debugging;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Explanation;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class IndexReadOperationBase : IndexOperationBase
    {
        protected readonly QueryBuilderFactories QueryBuilderFactories;
        private readonly MemoryInfo _memoryInfo;

        protected IndexReadOperationBase(Index index, Logger logger, QueryBuilderFactories queryBuilderFactories, IndexQueryServerSide query) : base(index, logger)
        {
            QueryBuilderFactories = queryBuilderFactories;


            if (_logger.IsInfoEnabled && query != null)
            {
                _memoryInfo = new MemoryInfo
                {
                    AllocatedBefore = GC.GetAllocatedBytesForCurrentThread(),
                    ManagedThreadId = NativeMemory.CurrentThreadStats.ManagedThreadId,
                    Query = query.Metadata.Query
                };
            }
        }

        public abstract long EntriesCount();

        public abstract IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
            Reference<int> totalResults, Reference<int> skippedResults, Reference<int> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext,
            Func<string, SpatialField> getSpatialField, CancellationToken token);

        public abstract IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults,
            Reference<int> skippedResults, Reference<int> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField,
            CancellationToken token);

        public abstract HashSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token);

        public abstract IEnumerable<QueryResult> MoreLikeThis(
            IndexQueryServerSide query,
            IQueryResultRetriever retriever,
            DocumentsOperationContext context,
            CancellationToken token);

        public abstract IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<int> totalResults, DocumentsOperationContext documentsContext,
            Func<string, SpatialField> getSpatialField, bool ignoreLimit, CancellationToken token);

        public abstract IEnumerable<string> DynamicEntriesFields(HashSet<string> staticFields);
        
        public override void Dispose()
        {
            if (_logger.IsInfoEnabled && _memoryInfo != null && _memoryInfo.ManagedThreadId == NativeMemory.CurrentThreadStats.ManagedThreadId)
            {
                var diff = GC.GetAllocatedBytesForCurrentThread() - _memoryInfo.AllocatedBefore;
                if (diff > 0)
                {
                    _logger.Info($"Query for index `{_indexName}` for query: `{_memoryInfo.Query}`, allocated: {new Size(diff, SizeUnit.Bytes)}");
                }
            }
        }

        public struct QueryResult
        {
            public Document Result;
            public Dictionary<string, Dictionary<string, string[]>> Highlightings;
            public ExplanationResult Explanation;
        }

        private class MemoryInfo
        {
            public long AllocatedBefore { get; init; }
            public int ManagedThreadId { get; init; }
            public Queries.AST.Query Query { get; init; }
        }
    }
}
