﻿using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax;
using Corax.Pipeline;
using Corax.Queries;
using Raven.Client.Documents.Queries.Explanation;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Voron.Impl;
using CoraxConstants = Corax.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexReadOperation : IndexReadOperationBase
    {
        private readonly IndexFieldsMapping _fieldMappings;
        private readonly IndexSearcher _indexSearcher;
        private readonly ByteStringContext _allocator;
        private long _entriesCount = 0;
        private const int BufferSize = 4096;

        public CoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction) : base(index, logger)
        {
            _allocator = readTransaction.Allocator;
            _fieldMappings = CoraxDocumentConverterBase.GetKnownFields(_allocator, index);
            _fieldMappings.UpdateAnalyzersInBindings(CoraxIndexingHelpers.CreateCoraxAnalyzers(_allocator, index, index.Definition, true));
            _indexSearcher = new IndexSearcher(readTransaction, _fieldMappings);
        }

        public override long EntriesCount() => _entriesCount;
        
        public override IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
            Reference<int> totalResults, Reference<int> skippedResults,
            Reference<int> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField,
            CancellationToken token)
        {
            var pageSize = query.PageSize;
            var isDistinctCount = pageSize == 0 && query.Metadata.IsDistinct;
            if (isDistinctCount)
                pageSize = int.MaxValue;

            var position = query.Start;

            var take = pageSize + position;
            if (take > _indexSearcher.NumberOfEntries || fieldsToFetch.IsDistinct)
                take = CoraxConstants.IndexSearcher.TakeAll;

            QueryTimingsScope coraxScope = null;
            QueryTimingsScope highlightingScope = null;
            if (queryTimings != null)
            {
                coraxScope = queryTimings.For(nameof(QueryTimingsScope.Names.Corax), start: false);
                highlightingScope = query.Metadata.HasHighlightings
                    ? queryTimings.For(nameof(QueryTimingsScope.Names.Highlightings), start: false)
                    : null;
            }
            
            IQueryMatch queryMatch;
            Dictionary<string, object> queryData = new();
            using (coraxScope?.Start())
            {             
                if ((queryMatch = CoraxQueryBuilder.BuildQuery(_indexSearcher, null, null, query.Metadata, _index, query.QueryParameters, null,
                    _fieldMappings, fieldsToFetch, take: take)) is null)
                yield break;
            }

            var ids = ArrayPool<long>.Shared.Rent(CoraxGetPageSize(_indexSearcher, BufferSize, query));
            int docsToLoad = pageSize;
            int queryStart = query.Start;
            bool hasHighlights = query.Metadata.HasHighlightings;
            if (hasHighlights)
            {
                using (highlightingScope?.For(nameof(QueryTimingsScope.Names.Setup)))
                    SetupHighlighter(query, documentsContext);
            }


            using var queryScope = new CoraxIndexQueryingScope(_index.Type, query, fieldsToFetch, retriever, _indexSearcher, _fieldMappings);
            using var queryFilter = GetQueryFilter(_index, query, documentsContext, skippedResults, scannedDocuments, retriever, queryTimings);

            while (true)
            {
                if (hasHighlights)
                {
                    foreach (var highlightings in query.Metadata.Highlightings)
                    {
                        var fieldName = highlightings.Field.Value;
                        var fieldId = _fieldMappings.GetByFieldName(fieldName);
                    }
                }

                token.ThrowIfCancellationRequested();
                int i = queryScope.RecordAlreadyPagedItemsInPreviousPage(ids.AsSpan(), queryMatch, totalResults, out var read, ref queryStart, token);
                for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                {
                    token.ThrowIfCancellationRequested();
                    if (queryScope.WillProbablyIncludeInResults(_indexSearcher.GetRawIdentityFor(ids[i])) == false)
                    {
                        docsToLoad++;
                        skippedResults.Value++;
                        continue;
                    }

                    var retrieverInput = new RetrieverInput(_fieldMappings, _indexSearcher.GetReaderAndIdentifyFor(ids[i], out var key), key);
                    
                    var filterResult = queryFilter?.Apply(ref retrieverInput, key);
                    if (filterResult is not null and not FilterResult.Accepted)
                    {
                        docsToLoad++;
                        if (filterResult is FilterResult.Skipped)
                            continue;
                        if (filterResult is FilterResult.LimitReached)
                            break;
                    }

                    bool markedAsSkipped = false;
                    var fetchedDocument = retriever.Get(ref retrieverInput, token);
                    
                    if (fetchedDocument.Document != null)
                    {
                        var qr = GetQueryResult(fetchedDocument.Document, ref markedAsSkipped);
                        if (qr.Result is null)
                        {
                            docsToLoad++;
                            continue;
                        }

                        yield return qr;
                    }
                    else if (fetchedDocument.List != null)
                    {
                        foreach (Document item in fetchedDocument.List)
                        {
                            var qr = GetQueryResult(item, ref markedAsSkipped);
                            if (qr.Result is null)
                            {
                                docsToLoad++;
                                continue;
                            }

                            yield return qr;
                        }
                    }
                }

                if ((read = queryMatch.Fill(ids)) == 0)
                    break;
                totalResults.Value += read;
            }


            if (isDistinctCount)
                totalResults.Value -= skippedResults.Value;

            QueryResult GetQueryResult(Document document, ref bool markedAsSkipped)
            {
                if (queryScope.TryIncludeInResults(document) == false)
                {
                    document?.Dispose();

                    if (markedAsSkipped == false)
                    {
                        skippedResults.Value++;
                        markedAsSkipped = true;
                    }

                    return default;
                }

                Dictionary<string, Dictionary<string, string[]>> highlightings = null;

                if (isDistinctCount == false)
                {
                    if (query.Metadata.HasHighlightings)
                    {
                        using (highlightingScope?.For(nameof(QueryTimingsScope.Names.Setup)))
                        {
                            highlightings = new();

                            // If we have highlightings then we need to setup the Corax objects that will attach to the evaluator in order
                            // to retrieve the fields and perform the transformations required by Highlightings. 
                            foreach (var current in query.Metadata.Highlightings)
                            {
                                var fieldName = current.Field.Value;
                                if (queryData.TryGetValue(fieldName, out var value) == false)
                                    continue;

                                //Highlight
                            }
                        }

                        throw new NotImplementedException($"{nameof(Corax)} doesn't support {nameof(Highlightings)} yet.");
                    }

                    if (query.Metadata.HasExplanations)
                    {
                        throw new NotImplementedException($"{nameof(Corax)} doesn't support {nameof(Explanations)} yet.");
                    }

                    return new QueryResult {Result = document, Highlightings = highlightings, Explanation = null};
                }

                return default;
            }

            ArrayPool<long>.Shared.Return(ids);
        }
        
        private Dictionary<string, (string[] PreTags, string[] PostTags, string Term)> _tagsPerField;

        private void SetupHighlighter(IndexQueryServerSide query, JsonOperationContext context)
        {
            _tagsPerField = null;
            foreach (var highlighting in query.Metadata.Highlightings)
            {
                var options = highlighting.GetOptions(context, query.QueryParameters);
                if (options == null)
                    continue;

                var numberOfPreTags = options.PreTags?.Length ?? 0;
                var numberOfPostTags = options.PostTags?.Length ?? 0;

                if (numberOfPreTags != numberOfPostTags)
                    throw new InvalidOperationException("Number of pre-tags and post-tags must match.");

                if (numberOfPreTags == 0)
                    continue;

                if (_tagsPerField == null)
                    _tagsPerField = new();

                var fieldName = query.Metadata.IsDynamic
                    ? throw new NotSupportedException("AutoIndex dynamic field is not supported yet.")
                    : highlighting.Field.Value;
                
                // TODO: get the terms.

                var term = string.Empty;

                _tagsPerField[fieldName] = (options.PreTags, options.PostTags, term);
            }
        }

        public override IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults,
            Reference<int> skippedResults, Reference<int> scannedDocuments, IQueryResultRetriever retriever,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override HashSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token)
        {
            HashSet<string> results = new();
            var terms = _indexSearcher.GetTermsOfField(field);

            if (fromValue is not null)
            {
                Span<byte> fromValueBytes = Encodings.Utf8.GetBytes(fromValue);
                while (terms.GetNextTerm(out var termSlice))
                {
                    token.ThrowIfCancellationRequested();
                    if (termSlice.SequenceEqual(fromValueBytes))
                        break;
                }
            }

            while (pageSize > 0 && terms.GetNextTerm(out var termSlice))
            {
                token.ThrowIfCancellationRequested();
                results.Add(Encodings.Utf8.GetString(termSlice));
                pageSize--;
            }

            return results;
        }

        public override IEnumerable<QueryResult> MoreLikeThis(IndexQueryServerSide query, IQueryResultRetriever retriever, DocumentsOperationContext context,
            CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<int> totalResults,
            DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField, bool ignoreLimit, CancellationToken token)
        {
            var pageSize = query.PageSize;
            var position = query.Start;

            if (query.Metadata.IsDistinct)
                throw new NotSupportedException("We don't support Distinct in \"Show Raw Entry\" of Index.");
            if (query.Metadata.FilterScript != null)
                throw new NotSupportedException(
                    "Filter isn't supported in Raw Index View.");

            var take = pageSize + position;
            if (take > _indexSearcher.NumberOfEntries)
                take = CoraxConstants.IndexSearcher.TakeAll;

            IQueryMatch queryMatch;
            if ((queryMatch = CoraxQueryBuilder.BuildQuery(_indexSearcher, null, null, query.Metadata, _index, query.QueryParameters, null,
                    _fieldMappings, null, take: take)) is null)
                yield break;

            var ids = ArrayPool<long>.Shared.Rent(CoraxGetPageSize(_indexSearcher, BufferSize, query));

            List<string> itemList = new(32);
            var bufferSizes = GetMaximumSizeOfBuffer();
            var tokensBuffer = ArrayPool<Token>.Shared.Rent(bufferSizes.TokenSize);
            var encodedBuffer = ArrayPool<byte>.Shared.Rent(bufferSizes.OutputSize);

            int docsToLoad = pageSize;

            int read;
            int i = Skip();
            while (true)
            {
                token.ThrowIfCancellationRequested();
                for (; docsToLoad != 0 && i < read; ++i, --docsToLoad)
                {
                    token.ThrowIfCancellationRequested();
                    var reader = _indexSearcher.GetReaderAndIdentifyFor(ids[i], out var id);
                    yield return documentsContext.ReadObject(GetRawDocument(reader), id);
                }

                if ((read = queryMatch.Fill(ids)) == 0)
                    break;
                totalResults.Value += read;
            }

            ArrayPool<long>.Shared.Return(ids);
            ArrayPool<byte>.Shared.Return(encodedBuffer);
            ArrayPool<Token>.Shared.Return(tokensBuffer);

            DynamicJsonValue GetRawDocument(in IndexEntryReader reader)
            {
                var doc = new DynamicJsonValue();
                foreach (var binding in _fieldMappings)
                {
                    if (binding.FieldIndexingMode is FieldIndexingMode.No || binding.FieldNameAsString is Client.Constants.Documents.Indexing.Fields.AllStoredFields)
                        continue;
                    var type = reader.GetFieldType(binding.FieldId, out _);
                    if ((type & IndexEntryFieldType.List) != 0)
                    {
                        reader.TryReadMany(binding.FieldId, out var iterator);
                        var enumerableEntries = new List<object>();
                        while (iterator.ReadNext())
                        {
                            if (binding.FieldIndexingMode is FieldIndexingMode.Exact)
                            {
                                enumerableEntries.Add(Encodings.Utf8.GetString(iterator.Sequence));
                                continue;
                            }

                            enumerableEntries.Add(GetAnalyzedItem(binding, iterator.Sequence));
                        }

                        doc[binding.FieldNameAsString] = enumerableEntries.ToArray();
                    }
                    else
                    {
                        reader.Read(binding.FieldId, out Span<byte> value);
                        if (binding.FieldIndexingMode is FieldIndexingMode.Exact)
                        {
                            doc[binding.FieldNameAsString] = Encodings.Utf8.GetString(value);
                            continue;
                        }

                        doc[binding.FieldNameAsString] = GetAnalyzedItem(binding, value);
                    }
                }

                return doc;
            }

            object GetAnalyzedItem(IndexFieldBinding binding, ReadOnlySpan<byte> value)
            {
                var tokens = tokensBuffer.AsSpan();
                var encoded = encodedBuffer.AsSpan();
                itemList?.Clear();
                binding.Analyzer.Execute(value, ref encoded, ref tokens);
                for (var index = 0; index < tokens.Length; ++index)
                {
                    token.ThrowIfCancellationRequested();
                    itemList.Add(Encodings.Utf8.GetString(encoded.Slice(tokens[index].Offset, (int)tokens[index].Length)));
                }

                return itemList.Count switch
                {
                    1 => itemList[0],
                    > 1 => itemList.ToArray(),
                    _ => string.Empty
                };
            }

            (int OutputSize, int TokenSize) GetMaximumSizeOfBuffer()
            {
                int outputSize = 512;
                int tokenSize = 512;
                foreach (var binding in _fieldMappings)
                {
                    token.ThrowIfCancellationRequested();
                    if (binding.Analyzer is null)
                        continue;

                    binding.Analyzer.GetOutputBuffersSize(512, out int tempOutputSize, out int tempTokenSize);
                    tokenSize = Math.Max(tempTokenSize, tokenSize);
                    outputSize = Math.Max(tempOutputSize, outputSize);
                }

                return (outputSize, tokenSize);
            }


            int Skip()
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    read = queryMatch.Fill(ids);
                    totalResults.Value += read;

                    if (position > read)
                    {
                        position -= read;
                        continue;
                    }

                    if (position == read)
                    {
                        read = queryMatch.Fill(ids);
                        totalResults.Value += read;
                        return 0;
                    }

                    return position;
                }
            }
        }

        public override IEnumerable<string> DynamicEntriesFields(HashSet<string> staticFields)
        {
            foreach (var field in _index.Definition.IndexFields.Values)
            {
                if (staticFields.Contains(field.Name))
                    continue;
                yield return field.Name;
            }
        }

        public override void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator($"Could not dispose {nameof(CoraxIndexReadOperation)} of {_index.Name}");
            exceptionAggregator.Execute(() => _indexSearcher?.Dispose());
            exceptionAggregator.ThrowIfNeeded();
        }
    }
}
