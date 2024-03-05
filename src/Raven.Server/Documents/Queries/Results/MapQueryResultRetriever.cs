﻿using System;
using System.Collections.Generic;
using System.Threading;
using Corax;
using Lucene.Net.Store;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapQueryResultRetriever<TDocument> : QueryResultRetrieverBase<TDocument>
    where TDocument : Document, new()
    {
        private QueryTimingsScope _storageScope;

        public MapQueryResultRetriever(DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsStorage documentsStorage, DocumentsOperationContext context, SearchEngineType searchEngineType, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand)
            : base(database, query, queryTimings, searchEngineType, fieldsToFetch, documentsStorage, context, documentContext: context, false, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand: includeRevisionsCommand)
        {
        }

        public override (TDocument Document, List<TDocument> List) Get(ref RetrieverInput retrieverInput, CancellationToken token)
        {

            using (RetrieverScope?.Start())
            {
                string id = string.Empty;
                switch (SearchEngineType)
                {
                    case SearchEngineType.Corax:
                        id = retrieverInput.DocumentId;
                        break;
                    case SearchEngineType.None:
                    case SearchEngineType.Lucene:
                        if (TryGetKeyLucene(ref retrieverInput, out id) == false)
                            throw new InvalidOperationException($"Could not extract '{Constants.Documents.Indexing.Fields.DocumentIdFieldName}' from index.");
                        break;
                }

                if (FieldsToFetch.IsProjection)
                    return GetProjection(ref retrieverInput, id, token);

                using (_storageScope = _storageScope?.Start() ?? RetrieverScope?.For(nameof(QueryTimingsScope.Names.Storage)))
                {
                    var doc = DirectGet(ref retrieverInput, id, DocumentFields);

                    FinishDocumentSetup(doc, retrieverInput.Score);
                    return (doc, null);
                }
            }
        }

        public override bool TryGetKeyLucene(ref RetrieverInput retrieverInput, out string key)
        {
            //Lucene method
            key = retrieverInput.LuceneDocument.Get(Constants.Documents.Indexing.Fields.DocumentIdFieldName, retrieverInput.State);
            return string.IsNullOrEmpty(key) == false;
        }

        public override bool TryGetKeyCorax(IndexSearcher searcher, long id, out UnmanagedSpan key)
        {
            key = searcher.GetRawIdentityFor(id);
            return key.Length > 0;
        }
        
        protected override long? GetCounter(string docId, string name)
        {
            var value = DocumentsStorage.CountersStorage.GetCounterValue(DocumentContext, docId, name);
            return value?.Value;
        }

        protected override DynamicJsonValue GetCounterRaw(string docId, string name)
        {
            var djv = new DynamicJsonValue();

            foreach (var partialValue in DocumentsStorage.CountersStorage.GetCounterPartialValues(DocumentContext, docId, name))
            {
                djv[partialValue.ChangeVector] = partialValue.PartialValue;
            }

            return djv;
        }
    }
}
