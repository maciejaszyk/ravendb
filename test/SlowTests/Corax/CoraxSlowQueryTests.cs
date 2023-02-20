﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class CoraxSlowQueryTests : RavenTestBase
{
    public CoraxSlowQueryTests(ITestOutputHelper output) : base(output)
    {
    }

    private class IndexEntry
    {
        public string Id { get; set; } 
        public string[] Content { get; set; }
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AllIn(Options options)
    {
        var entry0 = new IndexEntry
        {
            Id = "entry/0",
            Content = new string[]
            {
                "quo", "consequatur?", "officia", "in", "pariatur.", "illo", "minim", "nihil", "consequuntur", "eum", "consequuntur", "error", "qui", "et", "eos",
                "minim", "numquam", "commodo", "architecto", "ut", "Cicero", "deserunt", "Finibus", "sunt", "nesciunt.", "molestiae", "Quis",
                "THIS_IS_UNIQUE_VALUE,", "eum", "in"
            },
        };
        var entry1 = new IndexEntry
        {
            Id = "entry/1",
            Content = new string[]
            {
                "incididunt", "fugiat", "quia", "consequatur?", "magnam", "officia", "elit,", "illum", "ipsa", "of", "culpa", "ea", "voluptas", "Duis",
                "voluptatem", "Lorem", "modi", "qui", "Sed", "veritatis", "written", "ea", "mollit", "sint", "porro", "ratione", "THIS_IS_UNIQUE_VALUE,",
                "consectetur", "laudantium,", "aliquam"
            },
        };
        var entry2 = new IndexEntry
        {
            Id = "entry/2",
            Content = new string[]
            {
                "laboris", "natus", "Neque", "consequatur,", "qui", "ut", "natus", "illo", "Quis", "voluptas", "eaque", "quasi", "", "aut", "esse", "sed", "qui",
                "aut", "eos", "eius", "quia", "esse", "aliquip", "", "vel", "quia", "aliqua.", "quia", "consequatur,", "Sed"
            },
        };
        var entry3 = new IndexEntry
        {
            Id = "entry/3",
            Content = new string[]
            {
                "enim", "aliquid", "voluptas", "Finibus", "eaque", "esse", "Duis", "aut", "voluptatem.", "reprehenderit", "ad", "illum", "consequatur?",
                "architecto", "velit", "esse", "veniam,", "amet,", "voluptatem", "accusantium", "THIS_IS_UNIQUE_VALUE.", "dolore", "eum", "laborum.", "ipsam",
                "of", "explicabo.", "voluptatem", "et", "quis"
            },
        };
        var entry4 = new IndexEntry
        {
            Id = "entry/4",
            Content = new string[]
            {
                "incididunt", "id", "ratione", "inventore", "pariatur.", "molestiae", "dolor", "sit", "Nemo", "de", "nulla", "et", "proident,", "quae", "ipsam",
                "iste", "in", "dolore", "culpa", "enim", "dolor", "consectetur", "veritatis", "of", "45", "fugiat", "magnam", "Bonorum", "dolor", "beatae"
            },
        };
        var entry5 = new IndexEntry
        {
            Id = "entry/5",
            Content = new string[]
            {
                "laboriosam,", "totam", "voluptate", "et", "sit", "culpa", "reprehenderit", "eius", "accusantium", "", "omnis", "beatae", "amet,", "nulla",
                "tempor", "ullamco", "dolor", "ipsam", "vel", "THIS_IS_UNIQUE_VALUE", "quia", "", "consequatur,", "labore", "aliqua.", "dicta", "nostrum", "ut",
                "dolorem", "Duis"
            },
        };
        var entry6 = new IndexEntry
        {
            Id = "entry/6",
            Content = new string[]
            {
                "enim", "sed", "ad", "deserunt", "eu", "omnis", "voluptate", "in", "qui", "rem", "sunt", "tempor", "voluptatem", "vel", "enim", "velit", "velit",
                "aliquip", "by", "in", "eum", "dolore", "incidunt", "commodi", "anim", "amet,", "quo", "est,", "ratione", "sit"
            },
        };
        var entry7 = new IndexEntry
        {
            Id = "entry/7",
            Content = new string[]
            {
                "sed", "qui", "esse", "THIS_IS_UNIQUE_VALUE", "dolore", "totam", "Nemo", "veniam,", "reprehenderit", "consequuntur", "consequuntur", "aperiam,",
                "fugiat", "sed", "corporis", "45", "culpa", "accusantium", "quae", "dolor", "voluptate", "dolor", "et", "explicabo.", "voluptate", "Nemo",
                "tempora", "accusantium", "dolore", "in"
            },
        };
        var entry8 = new IndexEntry
        {
            Id = "entry/8",
            Content = new string[]
            {
                "nihil", "velit", "quia", "amet,", "fugit,", "eiusmod", "magna", "aliqua.", "ullamco", "accusantium", "nulla", "ex", "sit", "quo", "sit", "sit",
                "enim", "qui", "sunt", "aspernatur", "laboris", "autem", "voluptas", "amet,", "ipsa", "commodo", "minima", "consectetur,", "fugiat", "voluptas"
            },
        };
        var entry9 = new IndexEntry
        {
            Id = "entry/9",
            Content = new string[]
            {
                "dolorem", "ipsa", "in", "omnis", "ullamco", "ab", "esse", "aut", "rem", "eu", "iure", "ad", "consequuntur", "est", "adipisci", "velit",
                "inventore", "nesciunt.", "ad", "vitae", "laborum.", "esse", "voluptate", "et", "fugiat", "fugiat", "voluptas", "quae", "dolor", "qui"
            },
        };
        
        var entries = new[] {entry0, entry1, entry2, entry3, entry4, entry5, entry6, entry7, entry8, entry9};
        using var store = GetDocumentStore(options);
        {
            using var bulk = store.BulkInsert();
            foreach (var entry in entries)
            {
                bulk.Store(entry);
            }
        }
        {
            using var session = store.OpenSession();

            var result = session.Advanced.RawQuery<IndexEntry>(ArrayToQuery(new []{"quo", "in"})).ToList();
            Assert.Equal(2, result.Count);
            
            result = session.Advanced.RawQuery<IndexEntry>(ArrayToQuery(new []{"dolorem", "ipsa", "in", "omnis", "ullamco", "ab", "esse", "aut", "rem", "eu", "iure", "ad", "consequuntur", "est", "adipisci", "velit", "inventore", "nesciunt.", "ad", "vitae", "laborum.", "esse", "voluptate", "et", "fugiat", "fugiat", "voluptas", "quae", "dolor", "qui"})).ToList();
            Assert.Equal(1, result.Count);
            
            result = session.Advanced.RawQuery<IndexEntry>(ArrayToQuery(new []{"dolorem", "ipsa", "in", "omnis", "ullamco", "ab", "esse", "aut", "rem", "eu", "iure", "ad", "consequuntur", "est", "adipisci", "velit", "inventore", "nesciunt.", "ad", "vitae", "laborum.", "esse", "voluptate", "et", "fugiat", "fugiat", "voluptas", "quae", "dolor", 
                "THIS_IS_SUPER_UNIQUE_VALUE"})).ToList();
            Assert.Equal(0, result.Count);
        }

        string ArrayToQuery(string[] array)
        {
            var sb = new StringBuilder();
            sb.Append("from IndexEntries where Content all in (  ");
            for (int i = 0; i < array.Length; ++i)
            {
                if (i != 0)
                    sb.Append(", ");
                sb.Append($"\"{array[i]}\"");
            }

            sb.Append(')');
            return sb.ToString();
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SortingTest(Options options)
    {
        var expected = new List<Person>();
        using var store = GetDocumentStore(options);
        {
            using var bulk = store.BulkInsert();
            foreach (Person person in Enumerable.Range(0, 10_000).Select(i => new Person() {Name = $"ItemNo{i}", Age = i % 100, Height = i % 200}))
            {
                bulk.Store(person);
                expected.Add(person);
            }
        }
        expected = expected.OrderBy(y => y.Age).ThenByDescending(y => y.Height).ToList();
        {
            using var session = store.OpenSession();
            
            var result = session.Query<Person>().Where(p => p.Age < 200).OrderBy(y => y.Age)
                                           .ThenByDescending(y => y.Height)
                                           .Customize(i => i.WaitForNonStaleResults().NoCaching()).ToList();

            Assert.Equal(10_000, result.Count);
            for (int i = 0; i < 10_000; ++i)
            {
                var e = expected[i];
                var r = result[i];
                Assert.Equal(e.Age, r.Age);
                Assert.Equal(e.Height, r.Height);
            }

            Assert.False(result.GroupBy(x => x.Name).Any(g => g.Count() > 1));
        }
    }


    [Theory]
    [RavenExplicitData(searchEngine: RavenSearchEngineMode.Corax)]
    public void CompoundOrderByWithPagination(RavenTestParameters config, int size = 10_000)
    {
        List<Result> expected = new();
        var option = new Options()
        {
            ModifyDatabaseRecord = d =>
            {
                d.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                d.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                d.Client = new ClientConfiguration() {MaxNumberOfRequestsPerSession = int.MaxValue};
            }
        };

        using (var store = GetDocumentStore(option))
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < size; ++i)
                {
                    bulkInsert.Store(new Person() {Name = $"ItemNo{i}", Age = i % 100, Height = i % 200});
                    expected.Add(new Result() {Name = $"ItemNo{i}", Age = i % 100, Height = i % 200});
                }
            }

            expected = expected.OrderBy(y => y.Age).ThenByDescending(y => y.Height).ToList();

            using (var session = store.OpenSession())
            {
                List<Result> actual = new();
                for (int i = 0; i < size; i += 70)
                {
                    var results = session.Query<Person>().Where(p => p.Age < 200).OrderBy(y => y.Age) 
                                                             .ThenByDescending(y => y.Height)
                                                             .Skip(i).Take(70)
                                                             .Select(z => new Result() { Name = z.Name, Age = z.Age, Height = z.Height })
                                                             .Customize(i => i.WaitForNonStaleResults().NoCaching());
                    actual.AddRange(results.ToList());
                }

                var duplicates = actual.GroupBy(x => x.Name).Where(g => g.Count() > 1).Select(i => i.Key).ToList();
                WaitForUserToContinueTheTest(store);
                Assert.Equal(expected.Count, actual.Count);
                for (var i = 0; i < expected.Count; ++i)
                {
                    Assert.Equal(expected[i].Age, actual[i].Age);
                    Assert.Equal(expected[i].Height, actual[i].Height);
                }
            }
        }
    }

    [Theory]
    [RavenExplicitData(searchEngine: RavenSearchEngineMode.Corax)]
    public void DistinctBigTestWithPagination(RavenTestParameters config, int size = 100_00)
    {
        List<Result> expected = new();
        var option = new Options()
        {
            ModifyDatabaseRecord = d =>
            {
                d.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                d.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                d.Client = new ClientConfiguration() {MaxNumberOfRequestsPerSession = int.MaxValue};
            }
        };

        using (var store = GetDocumentStore(option))
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < size; ++i)
                {
                    bulkInsert.Store(new Person() {Name = $"ItemNo{i}", Age = i % 27, Height = i % 13});
                    expected.Add(new Result() {Age = i % 27, Height = i % 13});
                }
            }

            expected = expected.DistinctBy(x => x.Height).ToList();

            using (var session = store.OpenSession())
            {
                List<int> actual = new();
                for (int i = 0; i < size; i += 70)
                {
                    var query = session.Query<Person>()
                                                    .Where(p => p.Age < 123)
                                                    .Select(z => z.Height)
                                                    .Customize(i => i.WaitForNonStaleResults().NoCaching())
                                                    .Distinct().Skip(i).Take(70);
                    actual.AddRange(query.ToList());
                }
                   
                WaitForUserToContinueTheTest(store);
                Assert.Equal(expected.Count, actual.Count);
                actual.Sort();
                Assert.True(actual.SequenceEqual(expected.Select(p => p.Height).ToList()));
            }
        }
    }

    [Theory]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AnalyzerAreApplied(Options options)
    {
        var item = new Person() {Name = "MaCiEJ"};
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(item);
            session.SaveChanges();
        }
        {
            using var session = store.OpenSession();
            var result = session.Query<Person>().Where(x => x.Name == "Maciej")
                                                      .Customize(i => i.WaitForNonStaleResults().NoCaching())
                                                      .SingleOrDefault();
            Assert.NotNull(result);
            Assert.Equal(item.Name, result.Name);
        }
    }

    [Theory]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void BoostingTest(Options options)
    {
        const int terms = 29;
        using var coraxStore = GetDocumentStore(options);
        using var luceneStore = GetDocumentStore();
        List<Result> results;
        {
            using var coraxSession = coraxStore.BulkInsert();
            using var luceneSession = luceneStore.BulkInsert();
            results = Enumerable.Range(0, 10_000).Select(i => new Result() {Age = i % terms, Height = i}).ToList();
            results.ForEach((x) =>
            {
                coraxSession.Store(x);
                luceneSession.Store(x);
            });
        }


        {
            //TermMatches and BinaryMatches
            var rawQuery = new StringBuilder();
            rawQuery.Append("from Results where boost(Age == 0, 0)");
            for (int i = 1; i < terms; ++i)
                rawQuery.Append($" or boost(Age == '{i}',{i})");
            rawQuery.Append(" order by score()");

            Assertion(rawQuery.ToString());
        }
        {
            //MultiTermMatches
            var rawQuery = new StringBuilder();
            rawQuery.Append("from Results where boost(startsWith(Age, \"0\"),0)");
            for (int i = 1; i < terms; ++i)
                rawQuery.Append($" or boost(startsWith(Age, \"{i}\"),{i})");
            rawQuery.Append(" order by score()");

            Assertion(rawQuery.ToString());
        }

        {
            //UnaryTest
            Assertion($"from Results where boost(Age > {terms - 2}, 100) order by score(), Age as alphanumeric desc ");
        }

        void Assertion(string rawQuery)
        {
            using var coraxSession = coraxStore.OpenSession();
            using var luceneSession = luceneStore.OpenSession();
            var luceneResult = luceneSession.Advanced.RawQuery<Result>(rawQuery.ToString()).ToList();

            var coraxResult = coraxSession.Advanced.RawQuery<Result>(rawQuery.ToString()).ToList();
            Assert.NotEmpty(luceneResult);
            Assert.NotEmpty(coraxResult);
            Assert.Equal(luceneResult.Count, coraxResult.Count);
            for (int i = 0; i < luceneResult.Count; ++i)
                Assert.Equal(luceneResult[i].Age, coraxResult[i].Age);
        }
    }

    [Theory]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NgramSuggestionTest(Options options)
    {
        using (var documentStore = GetDocumentStore(options))
        {
            using (var s = documentStore.OpenSession())
            {
                s.Store(new User {Name = "Maciej"});
                s.Store(new User {Name = "Matt"});
                s.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
                var suggestionQueryResult = session.Query<User>()
                    .Customize(i => i.WaitForNonStaleResults().NoCaching())
                    .SuggestUsing(x => x.ByField(y => y.Name, "Mett").WithOptions(new SuggestionOptions
                    {
                        PageSize = 10, Accuracy = 0.25f, Distance = StringDistanceTypes.NGram
                    }))
                    .Execute();

                Assert.True(suggestionQueryResult["Name"].Suggestions.Count >= 1);
                Assert.Contains("matt", suggestionQueryResult["Name"].Suggestions);
            }
        }
    }

    public class TestA
    {
        public string Name { get; set; }
        public TestB Inner { get; set; }
    }

    public class TestB
    {
        public string InnerName { get; set; }
        public TestC Inner { get; set; }
    }

    public class TestC
    {
        public string InnerName { get; set; }
    }

    [Theory]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void TestOnDeep(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            var s = store.OpenSession();
            s.Store(
                new TestA() {Name = "Matt", Inner = new() {InnerName = "Jan", Inner = new() {InnerName = "Tester"}}});
            s.SaveChanges();
        }

        {
            var s = store.OpenSession();

            var result = s.Query<TestA>().Where(a => a.Inner == new TestB() {InnerName = "Jan", Inner = new() {InnerName = "Tester"}}).ToList()
                ;
            Assert.Equal(1, result.Count);
        }


        WaitForUserToContinueTheTest(store);
    }

    [Theory]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void Alphanumerical(Options options)
    {
        int size = 100;
        var random = new Random(4532234);
        var exampleStrings = new[] {"a", "b", "c", "d", "e", "f", "g"};
        using var coraxStore = GetDocumentStore(options);
        using var luceneStore = GetDocumentStore();

        {
            using var coraxBulk = coraxStore.BulkInsert();
            using var luceneBulk = luceneStore.BulkInsert();
            var data = Enumerable.Range(0, size).Select(i => new SortingData($"{exampleStrings[i % exampleStrings.Length]}{i}"));
            foreach (var i in data)
            {
                coraxBulk.Store(i);
                luceneBulk.Store(i);
            }
        }
        {
            using var coraxSession = coraxStore.OpenSession();
            using var luceneSession = luceneStore.OpenSession();

            var coraxResult = coraxSession.Query<SortingData>().OrderBy(p => p.data, OrderingType.AlphaNumeric).ToList();
            var luceneResult = luceneSession.Query<SortingData>().OrderBy(p => p.data, OrderingType.AlphaNumeric).ToList();

            Assert.Equal(size, luceneResult.Count);
            Assert.Equal(luceneResult.Count, coraxResult.Count);
            for (var i = 0; i < luceneResult.Count; ++i)
                Assert.Equal(luceneResult[i], coraxResult[i]);
        }
    }

    private record SortingData(string data);

    [Theory]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void MaxSuggestionsShouldWork(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            store.Maintenance.Send(new CreateSampleDataOperation());

            using (var session = store.OpenSession())
            {
                var result = session
                    .Query<Product>()
                    .SuggestUsing(f => f.ByField("Name", new[] {"chaig", "tof"}).WithOptions(new SuggestionOptions
                    {
                        PageSize = 5, Distance = StringDistanceTypes.NGram, SortMode = SuggestionSortMode.Popularity, Accuracy = 0.5f
                    }))
                    .Execute();

                Assert.True(result["Name"].Suggestions.Count is > 0 and <= 5);
            }
        }
    }

    [Theory]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanSearchOnLists(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var s = store.OpenSession();
            s.Store(new ListOfNames(new[] {"Maciej", "Gracjan", "Marcin", "Arek", "Tomek", "Pawel"}));
            s.Store(new ListOfNames(new[] {"Lukasz", "Damian", "Grzesiu", "Bartek", "Oliwia"}));
            s.Store(new ListOfNames(new[] {"Krzysiu", "Rafal", "Mateusz"}));

            s.SaveChanges();
        }
        {
            using var s = store.OpenSession();
            var r = s.Query<ListOfNames>().Search(p => p.Names, "maciej").ToList();
            Assert.Equal(1, r.Count);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void Test(Options options)
    {
        using var store = GetDocumentStore();
        {
            using var bulkInsert = store.BulkInsert();
            for (int i = 0; i < 10_000; ++i)
                bulkInsert.Store(new Person {Height = i % 599});
        }
        WaitForUserToContinueTheTest(store);
    }


    private record ListOfNames(string[] Names);


    private class Result
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public int Height { get; set; }
    }

    private class Person
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public int Height { get; set; }
    }
}
