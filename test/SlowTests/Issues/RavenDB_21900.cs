﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21900 : RavenTestBase
{
    [RavenFact(RavenTestCategory.Querying)]
    public void CanReferencePreviousDocumentInStreamCollectionQuery()
    {
        using (var store = GetDocumentStore(new Options
               {
                   ModifyDatabaseRecord = x => x.DocumentsCompression = new DocumentsCompressionConfiguration(compressRevisions: true, compressAllCollections: true)
               }))
        {
            string orderId;
            using (var session = store.OpenSession())
            {
                var order = new Order { Company = "Companies/1-A" };
                session.Store(order);
                orderId = order.Id;

                session.Store(new Order { Employee = "Employees/1-A", Freight = 30, Company = "Companies/2-A" });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = session.Advanced.RawQuery<Order>("from 'Orders' as a " +
                                                             $"load \"{orderId}\" as orderDoc " +
                                                             "select { Company: orderDoc.Company }");

                var list = query.ToList();
                foreach (var order in list)
                {
                    Assert.Equal("Companies/1-A", order.Company);
                }

                var stream = session.Advanced.Stream<Order>(query);
                while (stream.MoveNext())
                {
                    Assert.Equal("Companies/1-A", stream.Current.Document.Company);
                }
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Querying)]
    public void CanReferencePreviousDocumentInStreamCollectionQuery2()
    {
        using (var store = GetDocumentStore(new Options
               {
                   ModifyDatabaseRecord = x => x.DocumentsCompression = new DocumentsCompressionConfiguration(compressRevisions: true, compressAllCollections: true)
               }))
        {
            string orderId;
            using (var session = store.OpenSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
                for (var i = 0; i < 1024; ++i)
                    session.Store(new Order(){Company = "Maciej"});
                session.SaveChanges();

                var newRand = new Random(1337);
                var randomIds = session.Advanced.DocumentQuery<Order>().RandomOrdering("1337").SelectFields<string>("id()").Take(64).ToList();
                var allDocs = session.Query<Order>().ToList();

                foreach (var doc in allDocs)
                    doc.Employee = randomIds[newRand.Next(randomIds.Count)];
                
                session.SaveChanges();
                
                var query = session.Advanced.RawQuery<Order>($"from 'Orders' as a " +
                                                             $"order by a.Company " +
                                                             $"load a.Employee as orderDoc " +
                                                             "select orderDoc.Company, a.Freight");
                var list = query.WaitForNonStaleResults().ToList();
            }

            using (var session = store.OpenSession())
            {
                var query = session.Advanced.RawQuery<Order>($"from 'Orders' as a " +
                                                             $"order by a.Company " +
                                                             $"load a.Employee as orderDoc " +
                                                             "select orderDoc.Company, a.Freight");

                var list = query.ToList();
                foreach (var order in list)
                {
                    Assert.Equal("Maciej", order.Company);
                }

                var stream = session.Advanced.Stream<Order>(query);
                while (stream.MoveNext())
                {
                    Assert.Equal("Maciej", stream.Current.Document.Company);
                }
            }
        }
    }

    public RavenDB_21900(ITestOutputHelper output) : base(output)
    {
    }
}
