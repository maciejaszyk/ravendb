﻿using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Documents.Indexing;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Indexing
{
    public class StaticIndexesFromClient : RavenTestBase
    {
        public StaticIndexesFromClient(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [MemberData(nameof(SearchEngineTypeValue.Data), MemberType= typeof(SearchEngineTypeValue))]
        public async Task Can_Put(string searchEngineType)
        {
            using (var store = GetDocumentStore(new Options(){ModifyDatabaseRecord = d => d.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = searchEngineType}))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" });

                    await session.SaveChangesAsync();
                }

                var input = new IndexDefinition
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map
                };

                await store
                    .Maintenance
                    .SendAsync(new PutIndexesOperation(new[] { input }));

                var output = await store
                    .Maintenance
                    .SendAsync(new GetIndexOperation("Users_ByName"));

                Assert.True(input.Equals(output));
            }
        }

        public class UserAndAge
        {
            public string Name { set; get; }
            public int Age { set; get; }
        }

        [Theory]
        [MemberData(nameof(SearchEngineTypeValue.Data), MemberType= typeof(SearchEngineTypeValue))]
        public async Task Can_Put_And_Replace(string searchEngineType)
        {
            using (var store = GetDocumentStore(new Options(){ModifyDatabaseRecord = d => d.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = searchEngineType}))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new UserAndAge { Name = "Boki", Age = 14 });
                    await session.StoreAsync(new UserAndAge { Name = "Toli", Age = 5 });

                    await session.SaveChangesAsync();
                }

                var input = new IndexDefinition
                {
                    Maps = { "from user in docs.UserAndAges select new { user.Name }" },
                    Type = IndexType.Map,
                    Name = "Users_ByName"
                };

                var input2 = new IndexDefinition
                {
                    Maps = { "from user in docs.UserAndAges select new { user.Age }" },
                    Type = IndexType.Map,
                    Name = "Users_ByName"
                };

                await store
                    .Maintenance
                    .SendAsync(new PutIndexesOperation(new[] { input }));

                var output1 = await store
                    .Maintenance
                    .SendAsync(new GetIndexOperation("Users_ByName"));

                Assert.True(input.Equals(output1));

                await store
                    .Maintenance
                    .SendAsync(new PutIndexesOperation(new[] { input2 }));

                WaitForIndexing(store);

                var output2 = await store
                     .Maintenance
                     .SendAsync(new GetIndexOperation("Users_ByName"));

                Assert.Equal("Users_ByName", output2.Name);
                Assert.True(input2.Equals(output2));

            }
        }

        [Theory]
        [MemberData(nameof(SearchEngineTypeValue.Data), MemberType= typeof(SearchEngineTypeValue))]
        public async Task Can_Put_Replace_And_Back_To_Original(string searchEngineType)
        {
            using (var store = GetDocumentStore(new Options(){ModifyDatabaseRecord = d => d.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = searchEngineType}))
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90)))
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new UserAndAge { Name = "Boki", Age = 14 }, cts.Token);
                        await session.StoreAsync(new UserAndAge { Name = "Toli", Age = 5 }, cts.Token);

                        await session.SaveChangesAsync(cts.Token);
                    }

                    var input = new IndexDefinition
                    {
                        Maps = { "from user in docs.UserAndAges select new { user.Name }" },
                        Type = IndexType.Map,
                        Name = "Users_ByName"
                    };

                    var input2 = new IndexDefinition
                    {
                        Maps = { "from user in docs.UserAndAges select new { user.Age }" },
                        Type = IndexType.Map,
                        Name = "Users_ByName"
                    };

                    await store
                        .Maintenance
                        .SendAsync(new PutIndexesOperation(input), cts.Token);

                    var output1 = await store
                        .Maintenance
                        .SendAsync(new GetIndexOperation("Users_ByName"), cts.Token);

                    Assert.True(input.Equals(output1));

                    await store
                        .Maintenance
                        .SendAsync(new StopIndexingOperation(), cts.Token);

                    await store
                        .Maintenance
                        .SendAsync(new PutIndexesOperation(input2), cts.Token);

                    await store
                       .Maintenance
                       .SendAsync(new PutIndexesOperation(input), cts.Token);

                    await store
                        .Maintenance
                        .SendAsync(new StartIndexingOperation(), cts.Token);

                    WaitForIndexing(store);

                    var output2 = await store
                        .Maintenance
                        .SendAsync(new GetIndexOperation("Users_ByName"), cts.Token);

                    Assert.True(input.Equals(output2));
                }
            }
        }

        [Theory]
        [MemberData(nameof(SearchEngineTypeValue.Data), MemberType= typeof(SearchEngineTypeValue))]
        public async Task Can_start_and_stop_index(string searchEngineType)
        {
            using (var store = GetDocumentStore(new Options(){ModifyDatabaseRecord = d => d.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = searchEngineType}))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new UserAndAge { Name = "Boki", Age = 14 });
                    await session.StoreAsync(new UserAndAge { Name = "Toli", Age = 5 });

                    await session.SaveChangesAsync();
                }

                var input = new IndexDefinition
                {
                    Maps = { "from user in docs.UserAndAges select new { user.Name }" },
                    Type = IndexType.Map,
                    Name = "Users_ByName"
                };

                await store
                    .Maintenance
                    .SendAsync(new PutIndexesOperation(new[] { input }));

                await store
                    .Maintenance
                    .SendAsync(new StopIndexingOperation());

                await store
                    .Maintenance
                    .SendAsync(new StartIndexingOperation());

                WaitForIndexing(store);

                var errors = store.Maintenance.Send(new GetIndexErrorsOperation());

                Assert.Equal(0, errors[0].Errors.Length);
            }
        }
    }
}
