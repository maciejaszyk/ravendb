﻿using System.Buffers;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class IndexWriteOperationBase : IndexOperationBase
    {
        protected byte[] _buffer;
        protected IndexingStatsScope _statsInstance;
        protected readonly IndexWriteOperationStats Stats = new IndexWriteOperationStats();

        protected IndexWriteOperationBase(Index index, Logger logger) : base(index, logger)
        {
        }

        public abstract void Commit(IndexingStatsScope stats);

        public abstract void Optimize();

        public abstract void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
            JsonOperationContext indexContext);

        public abstract int EntriesCount();

        public abstract (long RamSizeInBytes, long FilesAllocationsInBytes) GetAllocations();

        public abstract void Delete(LazyStringValue key, IndexingStatsScope stats);

        public abstract void DeleteBySourceDocument(LazyStringValue sourceDocumentId, IndexingStatsScope stats);

        public abstract void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats);

        protected class IndexWriteOperationStats
        {
            public IndexingStatsScope DeleteStats;
            public IndexingStatsScope ConvertStats;
            public IndexingStatsScope AddStats;
            public IndexingStatsScope SuggestionStats;
        }
    }

}
