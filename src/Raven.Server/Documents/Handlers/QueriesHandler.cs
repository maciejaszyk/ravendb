using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Handlers.Processors.Queries;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public sealed class QueriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/queries", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task Post()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForGet(this, HttpMethod.Post))
                await processor.ExecuteAsync().ConfigureAwait(false);
        }

        // [RavenAction("/databases/*/queries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        // public async Task Get()
        // {
        //     using (var processor = new DatabaseQueriesHandlerProcessorForGet(this, HttpMethod.Get))
        //         await processor.ExecuteAsync().ConfigureAwait(false);
        // }

        
        [RavenAction("/databases/*/queries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public Task Get()
        {
            return HandleQuery(HttpMethod.Get);
        }
        
        public async Task HandleQuery(HttpMethod httpMethod)
        {
            using (var tracker = new RequestTimeTracker(HttpContext, Logger, Database.NotificationCenter, Database.Configuration, "Query"))
            {
                try
                {
                    using (var token = CreateHttpRequestBoundTimeLimitedOperationTokenForQuery())
                    using (var queryContext = QueryOperationContext.Allocate(Database))
                    {
                        var debug = GetStringQueryString("debug", required: false);
                        if (string.IsNullOrWhiteSpace(debug) == false)
                        {
                            throw new NotImplementedException($"DEBUG");
                            //await Debug(queryContext, debug, token, tracker, httpMethod);
                            
                            tracker.Dispose();
                            
                            return;
                        }

                        await Query(queryContext, token, tracker, httpMethod);
                        
                        tracker.Dispose();
                    }
                }
                catch (Exception e)
                {
                    if (tracker.Query == null)
                    {
                        string errorMessage;
                        if (e is EndOfStreamException || e is ArgumentException)
                        {
                            errorMessage = "Failed: " + e.Message;
                        }
                        else
                        {
                            errorMessage = "Failed: " +
                                           HttpContext.Request.Path.Value +
                                           e.ToString();
                        }

                        tracker.Query = errorMessage;
                        if (TrafficWatchManager.HasRegisteredClients)
                            AddStringToHttpContext(errorMessage, TrafficWatchChangeType.Queries);
                    }

                    throw;
                }
            }
        }
        private async Task<IndexQueryServerSide> GetIndexQuery(JsonOperationContext context, HttpMethod method, RequestTimeTracker tracker)
        {
            var addSpatialProperties = GetBoolValueQueryString("addSpatialProperties", required: false) ?? false;
            var clientQueryId = GetStringQueryString("clientQueryId", required: false);

            if (method == HttpMethod.Get)
                return await IndexQueryServerSide.CreateAsync(HttpContext, GetStart(), GetPageSize(), context, tracker, addSpatialProperties, clientQueryId);

            var json = await context.ReadForMemoryAsync(RequestBodyStream(), "index/query");

            return IndexQueryServerSide.Create(HttpContext, json, Database.QueryMetadataCache, tracker, addSpatialProperties, clientQueryId);
        }

        private async Task Query(QueryOperationContext queryContext, OperationCancelToken token, RequestTimeTracker tracker, HttpMethod method)
        {
            var indexQuery = await GetIndexQuery(queryContext.Documents, method, tracker);
            
            indexQuery.Diagnostics = GetBoolValueQueryString("diagnostics", required: false) ?? false ? new List<string>() : null;
            indexQuery.AddTimeSeriesNames = GetBoolValueQueryString("addTimeSeriesNames", false) ?? false;
            indexQuery.DisableAutoIndexCreation = GetBoolValueQueryString("disableAutoIndexCreation", false) ?? false;

            queryContext.WithQuery(indexQuery.Metadata);

            if (TrafficWatchManager.HasRegisteredClients)
                TrafficWatchQuery(indexQuery);

            var existingResultEtag = GetLongFromHeaders(Raven.Client.Constants.Headers.IfNoneMatch);

            if (indexQuery.Metadata.HasFacet)
            {
                throw new NotImplementedException($"Facet");
                return;
            }

            if (indexQuery.Metadata.HasSuggest)
            {
                throw new NotImplementedException($"suggest");
                return;
            }

            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;
            var shouldReturnServerSideQuery = GetBoolValueQueryString("includeServerSideQuery", required: false) ?? false;

            DocumentQueryResult result;
            try
            {
                result = await Database.QueryRunner.ExecuteQuery(indexQuery, queryContext, existingResultEtag, token).ConfigureAwait(false);
            }
            catch (IndexDoesNotExistException)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

            long numberOfResults;
            long totalDocumentsSizeInBytes;
            await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, ResponseBodyStream()))
            {
                result.Timings = indexQuery.Timings?.ToTimings();
                (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentQueryResultAsync(queryContext.Documents, result, metadataOnly, WriteAdditionalData(indexQuery, shouldReturnServerSideQuery), token.Token);
            }

            Database.QueryMetadataCache.MaybeAddToCache(indexQuery.Metadata, result.IndexName);
            
            if (ShouldAddPagingPerformanceHint(numberOfResults))
                AddPagingPerformanceHint(PagingOperationType.Queries, $"{nameof(Query)} ({result.IndexName})", $"{indexQuery.Metadata.QueryText}\n{indexQuery.QueryParameters}", numberOfResults, indexQuery.PageSize, result.DurationInMs, totalDocumentsSizeInBytes);

            AddQueryTimingsToTrafficWatch(indexQuery);
        }
        
        private void AddQueryTimingsToTrafficWatch(IndexQueryServerSide indexQuery)
        {
            if (TrafficWatchManager.HasRegisteredClients && indexQuery.Timings != null)
                HttpContext.Items[nameof(QueryTimings)] = indexQuery.Timings.ToTimings();
        }
        
        private Action<AbstractBlittableJsonTextWriter> WriteAdditionalData(IndexQueryServerSide indexQuery, bool shouldReturnServerSideQuery)
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
        
        [RavenAction("/databases/*/queries", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Patch()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForPatch(this)) 
                await processor.ExecuteAsync().ConfigureAwait(false);
        }

        [RavenAction("/databases/*/queries/test", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task PatchTest()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForPatchTest(this))
                await processor.ExecuteAsync().ConfigureAwait(false);
        }

        [RavenAction("/databases/*/queries", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForDelete(this))
                await processor.ExecuteAsync().ConfigureAwait(false);
        }
    }
}
