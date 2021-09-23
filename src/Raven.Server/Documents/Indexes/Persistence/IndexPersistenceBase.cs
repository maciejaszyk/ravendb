﻿using System;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Indexing;
using Sparrow.Json;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class IndexPersistenceBase : IDisposable
    {
        protected readonly Index _index;

        // this is used to remember the positions of files in the database
        // always points to the latest valid transaction and is updated by
        // the write tx on commit, thread safety is inherited from the voron
        // transaction
        protected IndexTransactionCache _streamsCache = new IndexTransactionCache();

        internal abstract LuceneVoronDirectory LuceneDirectory { get; }
        protected IndexPersistenceBase(Index index)
        {
            _index = index;
        }

        public abstract bool HasWriter { get; }

        public abstract void CleanWritersIfNeeded();

        public abstract void Clean(IndexCleanup mode);

        public abstract void Initialize(StorageEnvironment environment);

        public abstract void PublishIndexCacheToNewTransactions(IndexTransactionCache transactionCache);

        internal abstract IndexTransactionCache BuildStreamCacheAfterTx(Transaction tx);

        public abstract IndexWriteOperationBase OpenIndexWriter(Transaction writeTransaction, JsonOperationContext indexContext);

        public abstract IndexReadOperationBase OpenIndexReader(Transaction readTransaction);

        public abstract bool ContainsField(string field);
        public abstract IndexFacetedReadOperation OpenFacetedIndexReader(Transaction readTransaction);
        public abstract SuggestionIndexReaderBase OpenSuggestionIndexReader(Transaction readTransaction, string field);
        internal abstract void RecreateSearcher(Transaction asOfTx);
        internal abstract void RecreateSuggestionsSearchers(Transaction asOfTx);
        public abstract void DisposeWriters();
        public abstract void Dispose();

        protected void SetStreamCacheInTx(LowLevelTransaction tx)
        {
            tx.ImmutableExternalState = _streamsCache;
        }

    }


}
