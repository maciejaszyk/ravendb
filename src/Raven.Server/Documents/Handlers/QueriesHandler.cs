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
                await processor.Execute();
        }

        [RavenAction("/databases/*/queries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task Get()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForGet(this, HttpMethod.Get))
                await processor.Execute();
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
