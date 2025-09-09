using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal abstract class AbstractQueriesHandlerProcessorForGet<TRequestHandler, TOperationContext, TQueryContext, TQueryResult, TQueryResultsContainer> : AbstractQueriesHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TQueryContext : IDisposable
    where TQueryResultsContainer : QueryResultServerSide<TQueryResult>, IDisposable
{
    protected AbstractQueriesHandlerProcessorForGet([NotNull] TRequestHandler requestHandler, QueryMetadataCache queryMetadataCache, HttpMethod method) : base(requestHandler, queryMetadataCache)
    {
        QueryMethod = method;
    }

    protected abstract IDisposable AllocateContextForQueryOperation(out TQueryContext queryContext, out TOperationContext context);

    private async ValueTask HandleDebugAsync(IndexQueryServerSide query, TQueryContext queryContext, TOperationContext context, QueryStringParameters parameters, long? existingResultEtag, OperationCancelToken token)
    {
        var debug = parameters.Debug;
        if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
        {
            var ignoreLimit = parameters.IgnoreLimit;
            await IndexEntriesAsync(queryContext, context, query, existingResultEtag, ignoreLimit, token);
            return;
        }

        if (string.Equals(debug, "explain", StringComparison.OrdinalIgnoreCase))
        {
            await ExplainAsync(queryContext, query, token);
            return;
        }

        if (string.Equals(debug, "serverSideQuery", StringComparison.OrdinalIgnoreCase))
        {
            var serverSideQueryTask = ServerSideQueryAsync(context, query);
            if (serverSideQueryTask.IsCompletedSuccessfully == false)
                await serverSideQueryTask;

            return;
        }

        throw new NotSupportedException($"Not supported query debug operation: '{debug}'");
    }

    protected abstract ValueTask<IndexEntriesQueryResult> GetIndexEntriesAsync(TQueryContext queryContext, TOperationContext context, IndexQueryServerSide query, long? existingResultEtag, bool ignoreLimit, OperationCancelToken token);

    private async ValueTask IndexEntriesAsync(TQueryContext queryContext, TOperationContext context, IndexQueryServerSide query, long? existingResultEtag, bool ignoreLimit, OperationCancelToken token)
    {
        var result = await GetIndexEntriesAsync(queryContext, context, query, existingResultEtag, ignoreLimit, token);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
        {
            await writer.WriteIndexEntriesQueryResultAsync(context, result, token.Token);
        }
    }

    protected abstract Task ExplainAsync(TQueryContext queryContext, IndexQueryServerSide query, OperationCancelToken token);

    protected abstract Task<FacetedQueryResult> GetFacetedQueryResultAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag, OperationCancelToken token);

    protected abstract Task<SuggestionQueryResult> GetSuggestionQueryResultAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag, OperationCancelToken token);

    protected abstract Task<TQueryResultsContainer> GetQueryResultsAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag,
        bool metadataOnly,
        OperationCancelToken token);

    protected override HttpMethod QueryMethod { get; }

    private TQueryContext _queryContext;
    private TOperationContext _context;
    private IDisposable _queryContextDisposer;
    private RequestTimeTracker _timeTracker;
    private OperationCancelToken _token;
    private QueryStringParameters _parameters;
    private IndexQueryServerSide _indexQuery;
    private bool _doNotContinue;
    private TQueryResultsContainer _result = null;
    private long? _existingResultEtag;

    public override void Dispose()
    {
        using (_queryContextDisposer)
        using (_timeTracker)
        using (_token)
        {
        }

        base.Dispose();
    }

    public Task Build()
    {
        return GetIndexAndParametersAsync(this)
            .ContinueWith(RetrieveDocuments, this)
            .ContinueWith(WriteResults, this)
            .ContinueWith(DisposableTask, this);
    }

    public Task DisposableTask(Task task, object state)
    {
        var processor = state as AbstractQueriesHandlerProcessorForGet<TRequestHandler, TOperationContext, TQueryContext, TQueryResult, TQueryResultsContainer>;
        Debug.Assert(processor != null);
        processor!.Dispose();
        return Task.CompletedTask;
    }

    private static async Task WriteResults(Task task, [CanBeNull] object state)
    {
        var processor = state as AbstractQueriesHandlerProcessorForGet<TRequestHandler, TOperationContext, TQueryContext, TQueryResult, TQueryResultsContainer>;

        if (processor!._doNotContinue || task.IsCompletedSuccessfully == false)
            await task;

        try
        {
            using (processor!._result)
            {
                if (processor!._result.NotModified)
                {
                    processor!.HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                processor!.HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(processor!._result.ResultEtag);

                long numberOfResults;
                long totalDocumentsSizeInBytes;
                await using (var writer = new AsyncBlittableJsonTextWriter(processor!._context, processor!.RequestHandler.ResponseBodyStream(), processor!._token.Token))
                {
                    processor!._result.Timings = processor!._indexQuery.Timings?.ToTimings();

                    var writeDocumentQueryTask = writer.WriteDocumentQueryResultAsync(processor!._context, processor!._result, processor!._parameters.MetadataOnly,
                        WriteAdditionalData(processor!._indexQuery, processor!._parameters.IncludeServerSideQuery), processor!._token.Token);

                    (numberOfResults, totalDocumentsSizeInBytes) = writeDocumentQueryTask.IsCompletedSuccessfully
                        ? writeDocumentQueryTask.Result
                        : await writeDocumentQueryTask;

                    var flushTask = writer.MaybeFlushAsync(processor!._token.Token);
                    if (flushTask.IsCompletedSuccessfully == false)
                        await flushTask;
                }


                processor!.QueryMetadataCache.MaybeAddToCache(processor!._indexQuery.Metadata, processor!._result.IndexName);

                if (processor!.RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
                {
                    processor!.RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"Query ({processor!._result.IndexName})",
                        $"{processor!._indexQuery.Metadata.QueryText}\n{processor!._indexQuery.QueryParameters}", numberOfResults, processor!._indexQuery.PageSize, processor!._result.DurationInMs,
                        totalDocumentsSizeInBytes);
                }

                processor!.AddQueryTimingsToTrafficWatch(processor!._indexQuery);
            }
        }
        catch (Exception e)
        {
            HandleExceptionFromQuery(processor, e);
            throw;
        }
    }

    private static async Task RetrieveDocuments(Task task, [CanBeNull] object state)
    {
        var processor = state as AbstractQueriesHandlerProcessorForGet<TRequestHandler, TOperationContext, TQueryContext, TQueryResult, TQueryResultsContainer>;
        Debug.Assert(processor != null);
        try
        {
            Debug.Assert(processor != null);
            try
            {
                processor!._result = await processor.GetQueryResultsAsync(processor._indexQuery, processor._queryContext, processor._existingResultEtag, processor._parameters.MetadataOnly, processor._token);
            }
            catch (IndexDoesNotExistException)
            {
                processor!._result?.Dispose();
                processor.HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }
            catch (Exception)
            {
                processor!._result?.Dispose();
                throw;
            }
        }
        catch (Exception e)
        {
            HandleExceptionFromQuery(processor, e);
            throw;
        }
    }

    private static async Task GetIndexAndParametersAsync(AbstractQueriesHandlerProcessorForGet<TRequestHandler, TOperationContext, TQueryContext, TQueryResult, TQueryResultsContainer> processor)
    {
        processor._queryContextDisposer = processor.AllocateContextForQueryOperation(out processor._queryContext, out processor._context);
        processor._timeTracker = processor.CreateRequestTimeTracker();

        try
        {
            processor._token = processor.RequestHandler.CreateHttpRequestBoundTimeLimitedOperationTokenForQuery();
            processor._parameters = QueryStringParameters.Create(processor.HttpContext.Request);
            var getIndexQueryTask = processor.GetIndexQueryAsync(processor._context, processor.QueryMethod, processor._timeTracker, processor._parameters.AddSpatialProperties);
            processor._indexQuery = getIndexQueryTask.IsCompletedSuccessfully
                ? getIndexQueryTask.Result
                : await getIndexQueryTask;

            processor._indexQuery.Diagnostics = processor._parameters.Diagnostics ? new List<string>() : null;
            processor._indexQuery.AddTimeSeriesNames = processor._parameters.AddTimeSeriesNames;
            processor._indexQuery.DisableAutoIndexCreation = processor._parameters.DisableAutoIndexCreation;

            if (processor.RequestHandler.HttpContext.Request.IsFromOrchestrator())
                processor._indexQuery.ReturnOptions = IndexQueryServerSide.QueryResultReturnOptions.CreateForSharding(processor._indexQuery);

            processor.AssertIndexQuery(processor._indexQuery);

            processor._existingResultEtag = processor.RequestHandler.GetLongFromHeaders(Constants.Headers.IfNoneMatch);

            processor.EnsureQueryContextInitialized(processor._queryContext, processor._indexQuery);

            if (string.IsNullOrWhiteSpace(processor._parameters.Debug) == false)
            {
                await processor.HandleDebugAsync(processor._indexQuery, processor._queryContext, processor._context, processor._parameters, processor._existingResultEtag, processor._token);

                processor._doNotContinue = true;
                return;
            }

            if (TrafficWatchManager.HasRegisteredClients)
                processor.RequestHandler.TrafficWatchQuery(processor._indexQuery);
        }
        catch (Exception e)
        {
            HandleExceptionFromQuery(processor, e);
            throw;
        }
    }

    private static void HandleExceptionFromQuery(AbstractQueriesHandlerProcessorForGet<TRequestHandler, TOperationContext, TQueryContext, TQueryResult, TQueryResultsContainer> processor, Exception e)
    {
        if (processor._timeTracker.Query == null)
        {
            string errorMessage;
            if (e is EndOfStreamException || e is ArgumentException)
            {
                errorMessage = $"Failed: {e.Message}";
            }
            else
            {
                errorMessage = $"Failed: {processor.HttpContext.Request.Path.Value} {e}";
            }

            processor._timeTracker.Query = errorMessage;

            if (TrafficWatchManager.HasRegisteredClients)
                processor.RequestHandler.AddStringToHttpContext(errorMessage, TrafficWatchChangeType.Queries);
        }
    }

    public async Task ExecuteAsTaskAsync()
    {
        return Build();
        using (this)
        using (AllocateContextForQueryOperation(out var queryContext, out var context))
        using (var tracker = CreateRequestTimeTracker())
        {
            try
            {
                using (var token = RequestHandler.CreateHttpRequestBoundTimeLimitedOperationTokenForQuery())
                {
                    var parameters = QueryStringParameters.Create(HttpContext.Request);
                    var getIndexQueryTask = GetIndexQueryAsync(context, QueryMethod, tracker, parameters.AddSpatialProperties);
                    var indexQuery = getIndexQueryTask.IsCompletedSuccessfully ? getIndexQueryTask.Result : await getIndexQueryTask;

                    indexQuery.Diagnostics = parameters.Diagnostics ? new List<string>() : null;
                    indexQuery.AddTimeSeriesNames = parameters.AddTimeSeriesNames;
                    indexQuery.DisableAutoIndexCreation = parameters.DisableAutoIndexCreation;

                    if (RequestHandler.HttpContext.Request.IsFromOrchestrator())
                        indexQuery.ReturnOptions = IndexQueryServerSide.QueryResultReturnOptions.CreateForSharding(indexQuery);

                    AssertIndexQuery(indexQuery);

                    var existingResultEtag = RequestHandler.GetLongFromHeaders(Constants.Headers.IfNoneMatch);

                    EnsureQueryContextInitialized(queryContext, indexQuery);

                    if (string.IsNullOrWhiteSpace(parameters.Debug) == false)
                    {
                        await HandleDebugAsync(indexQuery, queryContext, context, parameters, existingResultEtag, token);
                        return;
                    }

                    if (TrafficWatchManager.HasRegisteredClients)
                        RequestHandler.TrafficWatchQuery(indexQuery);

                    if (indexQuery.Metadata.HasFacet)
                    {
                        await HandleFacetedQueryAsync(indexQuery, queryContext, context, existingResultEtag, token);
                        return;
                    }

                    if (indexQuery.Metadata.HasSuggest)
                    {
                        await HandleSuggestQueryAsync(indexQuery, queryContext, context, existingResultEtag, token);
                        return;
                    }

                    TQueryResultsContainer result = null;
                    try
                    {
                        result = await GetQueryResultsAsync(indexQuery, queryContext, existingResultEtag, parameters.MetadataOnly, token);
                    }
                    catch (IndexDoesNotExistException)
                    {
                        result?.Dispose();
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }
                    catch (Exception)
                    {
                        result?.Dispose();
                        throw;
                    }

                    using (result)
                    {
                        if (result.NotModified)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                            return;
                        }

                        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

                        long numberOfResults;
                        long totalDocumentsSizeInBytes;
                        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
                        {
                            result.Timings = indexQuery.Timings?.ToTimings();

                            var writeDocumentQueryTask = writer.WriteDocumentQueryResultAsync(context, result, parameters.MetadataOnly,
                                WriteAdditionalData(indexQuery, parameters.IncludeServerSideQuery), token.Token);

                            (numberOfResults, totalDocumentsSizeInBytes) = writeDocumentQueryTask.IsCompletedSuccessfully
                                ? writeDocumentQueryTask.Result
                                : await writeDocumentQueryTask;

                            var flushTask = writer.MaybeFlushAsync(token.Token);
                            if (flushTask.IsCompletedSuccessfully == false)
                                await flushTask;
                        }


                        QueryMetadataCache.MaybeAddToCache(indexQuery.Metadata, result.IndexName);

                        if (RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
                        {
                            RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"Query ({result.IndexName})",
                                $"{indexQuery.Metadata.QueryText}\n{indexQuery.QueryParameters}", numberOfResults, indexQuery.PageSize, result.DurationInMs,
                                totalDocumentsSizeInBytes);
                        }

                        AddQueryTimingsToTrafficWatch(indexQuery);
                    }
                }
            }
            catch (Exception e)
            {
                if (tracker.Query == null)
                {
                    string errorMessage;
                    if (e is EndOfStreamException || e is ArgumentException)
                    {
                        errorMessage = $"Failed: {e.Message}";
                    }
                    else
                    {
                        errorMessage = $"Failed: {HttpContext.Request.Path.Value} {e}";
                    }

                    tracker.Query = errorMessage;

                    if (TrafficWatchManager.HasRegisteredClients)
                        RequestHandler.AddStringToHttpContext(errorMessage, TrafficWatchChangeType.Queries);
                }

                throw;
            }
        }
    }

    public override ValueTask ExecuteAsync() => throw new NotSupportedException();

    protected virtual void AssertIndexQuery(IndexQueryServerSide indexQuery)
    {
    }

    protected virtual void EnsureQueryContextInitialized(TQueryContext queryContext, IndexQueryServerSide indexQuery)
    {
    }

    private static Action<AbstractBlittableJsonTextWriter> WriteAdditionalData(IndexQueryServerSide indexQuery, bool shouldReturnServerSideQuery)
    {
        if (indexQuery.Diagnostics == null && shouldReturnServerSideQuery == false)
            return null;

        return w =>
        {
            if (shouldReturnServerSideQuery)
            {
                w.WriteComma();
                w.WritePropertyName(nameof(indexQuery.ServerSideQuery));
                w.WriteString(indexQuery.ServerSideQuery);
            }

            if (indexQuery.Diagnostics != null)
            {
                w.WriteComma();
                w.WriteArray(nameof(indexQuery.Diagnostics), indexQuery.Diagnostics);
            }
        };
    }

    private async ValueTask ServerSideQueryAsync(TOperationContext context, IndexQueryServerSide indexQuery)
    {
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(indexQuery.ServerSideQuery));
            writer.WriteString(indexQuery.ServerSideQuery);

            writer.WriteEndObject();
        }
    }

    private async ValueTask HandleSuggestQueryAsync(IndexQueryServerSide query, TQueryContext queryContext, TOperationContext operationContext, long? existingResultEtag, OperationCancelToken token)
    {
        var result = await GetSuggestionQueryResultAsync(query, queryContext, existingResultEtag, token);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        long numberOfResults;
        long totalDocumentsSizeInBytes;
        await using (var writer = new AsyncBlittableJsonTextWriter(operationContext, RequestHandler.ResponseBodyStream(), token.Token))
        {
            var writeSuggestionQueryResult = writer.WriteSuggestionQueryResultAsync(operationContext, result, token.Token);
            (numberOfResults, totalDocumentsSizeInBytes) = writeSuggestionQueryResult.IsCompletedSuccessfully
                ? writeSuggestionQueryResult.Result
                : await writeSuggestionQueryResult;
        }

        if (RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
            RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"SuggestQuery ({result.IndexName})", query.Query, numberOfResults, query.PageSize, result.DurationInMs, totalDocumentsSizeInBytes);
    }

    private async Task HandleFacetedQueryAsync(IndexQueryServerSide query, TQueryContext queryContext, TOperationContext operationContext, long? existingResultEtag, OperationCancelToken token)
    {
        var result = await GetFacetedQueryResultAsync(query, queryContext, existingResultEtag, token);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        long numberOfResults;
        await using (var writer = new AsyncBlittableJsonTextWriter(operationContext, RequestHandler.ResponseBodyStream(), token.Token))
        {
            result.Timings = query.Timings?.ToTimings();
            var writeFacetedQueryResultTask = writer.WriteFacetedQueryResultAsync(operationContext, result, token.Token);
            numberOfResults = writeFacetedQueryResultTask.IsCompletedSuccessfully
                ? writeFacetedQueryResultTask.Result
                : await writeFacetedQueryResultTask;
        }

        QueryMetadataCache.MaybeAddToCache(query.Metadata, result.IndexName);

        if (RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
            RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"FacetedQuery ({result.IndexName})", $"{query.Metadata.QueryText}\n{query.QueryParameters}", numberOfResults, query.PageSize, result.DurationInMs, -1);

        AddQueryTimingsToTrafficWatch(query);
    }

    private void AddQueryTimingsToTrafficWatch(IndexQueryServerSide indexQuery)
    {
        if (TrafficWatchManager.HasRegisteredClients && indexQuery.Timings != null)
            HttpContext.Items[nameof(QueryTimings)] = indexQuery.Timings.ToTimings();
    }

    private sealed class QueryStringParameters : AbstractQueryStringParameters
    {
        public bool MetadataOnly;

        public bool AddSpatialProperties;

        public bool IncludeServerSideQuery;

        public bool Diagnostics;

        public bool AddTimeSeriesNames;

        public bool DisableAutoIndexCreation;

        public string Debug;

        public bool IgnoreLimit;

        private QueryStringParameters([NotNull] HttpRequest httpRequest)
            : base(httpRequest)
        {
        }

        protected override void OnFinalize()
        {
        }

        protected override void OnValue(QueryStringEnumerable.EncodedNameValuePair pair)
        {
            var name = pair.EncodedName;

            switch (name.Length)
            {
                case 5:
                {
                    if (IsMatch(name, DebugQueryStringName))
                        Debug = pair.DecodeValue().ToString();
                    return;
                }
                case 11:
                {
                    if (IsMatch(name, IgnoreLimitQueryStringName))
                    {
                        IgnoreLimit = GetBoolValue(name, pair.EncodedValue);
                        return;
                    }

                    if (IsMatch(name, DiagnosticsQueryStringName))
                        Diagnostics = GetBoolValue(name, pair.EncodedValue);

                    return;
                }
                case 12:
                {
                    if (IsMatch(name, MetadataOnlyQueryStringName))
                        MetadataOnly = GetBoolValue(name, pair.EncodedValue);
                    return;
                }
                case 18:
                {
                    if (IsMatch(name, AddTimeSeriesNamesQueryStringName))
                        AddTimeSeriesNames = GetBoolValue(name, pair.EncodedValue);
                    return;
                }
                case 20:
                {
                    if (IsMatch(name, AddSpatialPropertiesQueryStringName))
                        AddSpatialProperties = GetBoolValue(name, pair.EncodedValue);
                    return;
                }
                case 22:
                {
                    if (IsMatch(name, IncludeServerSideQueryQueryStringName))
                        IncludeServerSideQuery = GetBoolValue(name, pair.EncodedValue);
                    return;
                }
                case 24:
                {
                    if (IsMatch(name, DisableAutoIndexCreationQueryStringName))
                        DisableAutoIndexCreation = GetBoolValue(name, pair.EncodedValue);
                    return;
                }
            }
        }

        public static QueryStringParameters Create(HttpRequest httpRequest)
        {
            var parameters = new QueryStringParameters(httpRequest);
            parameters.Parse();

            return parameters;
        }
    }
}
