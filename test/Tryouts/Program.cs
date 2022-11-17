using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using CsvHelper;
using Raven.Server.Utils.Monitoring;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }


    // Mockup for LINQ to generate good RQL
    record Item(string Title, int Count);

    public const string IndexName = "Questions/Search";

    public static void Main(string[] args)
    {
        Console.WriteLine("Start collecting metrics...");
        TestQueriesAndSaveMetricsIntoFile();
    }


    class QueryGenerated
    {
        public string Id { get; set; }
        public string Query { get; set; }
        public int Count { get; set; }
        public int Depth { get; set; }
        public int AverageReqTime { get; set; }
    }

    private static readonly HttpClient HttpClient = new HttpClient();


    private static object UpdateLock = new object();
    private static long MetricId = 0;


    //Metrics.
    static void GetMetrics(CsvWriter writer)
    {
        if (Monitor.TryEnter(UpdateLock) == false)
            return;
        var json = HttpClient.GetStringAsync("http://127.0.0.1:8080/admin/monitoring/v1/server").Result;
        var metrics = JsonConvert.DeserializeObject<ServerMetrics>(json);

        writer.WriteField(metrics.Cpu.ProcessUsage);
        writer.WriteField(metrics.Cpu.MachineUsage);
        writer.WriteField(metrics.Network.RequestsPerSec);
        writer.WriteField(MetricId++);
        writer.NextRecord();
        Monitor.Exit(UpdateLock);
    }


    public record ComplexQuery(string Query, int MedianTimeToExecute, int MaxStackOfDepth, int Results);

    public static ConcurrentBag<double> _averageTime = new();


    /// <summary>
    /// To make this work you've to change metrics code in RavenDB Server. You can do this by cherry-picking commit: {913a5b15d709aa0aab38737577466dfc8d0ac91a} from `maciejaszyk/ravendb` repo
    /// </summary>
    public static void TestQueriesAndSaveMetricsIntoFile()
    {
        using FileStream csvFile = File.Create(@"MapReduceOptimized.csv");
        using var writer = new StreamWriter(csvFile);
        using var csvWriter = new CsvWriter(writer, CultureInfo.CurrentCulture);
        csvWriter.WriteField("Cpu usage");
        csvWriter.WriteField("CPU MachineUsage");

        csvWriter.WriteField("Managed memory");
        csvWriter.WriteField("AverageDuration");
        csvWriter.WriteField("RequestPerSec");
        csvWriter.WriteField("MetricId");
        csvWriter.NextRecord();

        using var session = DocumentStoreHolder.Store.OpenSession(new SessionOptions { NoCaching = true });

        var queries = session.Query<QueryGenerated>()
                                        .OrderBy(i => i.Id)
                                        .Select(i => i.Query)
                                        //.Take(128)
                                        .ToList();
        //.Select(i => i.Replace("Questions/Search", "QuestionSearchCustom")).ToList();
        //var queries = session.Query<MapReduceQuery>()
        //                                .OrderBy(i => i.Id)
        //                                .Select(i => i.Query)
        //                                .Take(256)
        //                                .ToList();//.Select(i => i.Replace("Questions/Tags", "CloneOf/Questions/Tags")).ToList(); 

        Console.WriteLine($"Running {queries.Count()} queries.");

        var timer = new System.Timers.Timer(1000);
        timer.Elapsed += (sender, args) => GetMetrics(csvWriter);
        timer.Start();
        var stopwatch = new Stopwatch();
        stopwatch.Start();

        using var innerSession = DocumentStoreHolder.Store.OpenSession(new SessionOptions { NoCaching = true });
        foreach (var query in queries)
        {
            var count = innerSession.Advanced.RawQuery<QuestionTags>(query).NoCaching().NoTracking().Statistics(out var stats).Count();
        }

        //Parallel.ForEach(queries, (term) =>
        //{
        //    using var innerSession = DocumentStoreHolder.Store.OpenSession(new SessionOptions { NoCaching = true });

        //    for (int i = 0; i < 64; ++i)
        //    {
        //        var count = innerSession.Advanced.RawQuery<QuestionTags>(term).NoCaching().NoTracking().Statistics(out var stats).Count();
        //    }


        //    //_averageTime.Add((int)durations.Average());
        //    //Console.WriteLine($"Average for {testingQuery.Id} is {testingQuery.AverageReqTime}ms");
        //});

        Console.WriteLine("Total time: " + stopwatch.ElapsedMilliseconds / 1000);
        timer.Stop();
    }



    record QuestionTags(string Tag, long Count, long Answers, long AcceptedAnswers);
    record MapReduceQuery(string Query, long Results, string Id = null);

    public static void GenerateMapReduceQueries()
    {
        using var session = DocumentStoreHolder.Store.OpenSession(new SessionOptions { NoCaching = true });

        var entries = session.Query<QuestionTags>("Questions/Tags").OrderByDescending(i => i.Answers, OrderingType.Long).ToList();
        var maxAcceptedAnswers = entries[^1].AcceptedAnswers;
        var random = new Random();

        Parallel.For(0, 10_000, (i) =>
        {
            using var innerSession = DocumentStoreHolder.Store.OpenSession(new SessionOptions { NoCaching = true });
            var count = 0;
        Repeat:
            var leftSide = random.Next(0, entries.Count);
            if (leftSide == entries.Count)
                goto Repeat;
            var rightSide = random.Next(leftSide, entries.Count);
            var leftInclusive = random.Next() % 2 == 0;
            var rightInclusive = random.Next() % 2 == 0;
            var leftAcceptedAnswer = random.Next(int.MinValue, (int)entries[leftSide].AcceptedAnswers + (leftInclusive ? 1 : 0));
            var rightAcceptedAnswer = random.Next((int)entries[rightSide].AcceptedAnswers + (rightInclusive ? 0 : 1), int.MaxValue);

            var leftSideQuery = $"AcceptedAnswers  {(leftInclusive ? " >= " : " > ")} {leftAcceptedAnswer} and AcceptedAnswers {(rightInclusive ? " <= " : " < ")} {rightAcceptedAnswer}";
            var innerData = innerSession.Advanced.RawQuery<QuestionTags>($"from index 'Questions/Tags' where {leftSideQuery}  order by Answers as long asc").NoCaching().NoTracking().ToList();
            var query = $"from index 'Questions/Tags' where ({leftSideQuery})";
            count = innerData.Count;


            if (innerData.Count == 0)
                goto Repeat;
            if (innerData.Count < 50)
                goto Store;




            RightSideRepeat:
            var innerLeftSide = random.Next(0, count);
            var innerRightSide = random.Next(innerLeftSide, count);
            if (innerData[innerLeftSide].Answers == innerData[innerRightSide].Answers)
            {
                if (innerData[0].Answers == innerData[^1].Answers)
                {
                    goto Store;
                }

                innerLeftSide = 0;
            }

            var innerLeftInclusive = random.Next() % 2 == 0;
            var innerRightInclusive = random.Next() % 2 == 0;
            var rightSideQuery = $"Answers  {(innerLeftInclusive ? " >= " : " > ")} {innerData[innerLeftSide].Answers} and Answers {(innerRightInclusive ? " <= " : " < ")} {innerData[innerRightSide].Answers}";
            query = $"from index 'Questions/Tags' where ({leftSideQuery}) and ({rightSideQuery})";
            var innerQuery = innerSession.Advanced.RawQuery<QuestionTags>(query).NoCaching().NoTracking().ToList();
            if (innerQuery.Count == 0)
                goto RightSideRepeat;
            count = innerQuery.Count;

        Store:
            Console.WriteLine($"Found Query {query}");

            innerSession.Store(new MapReduceQuery(query, count));
            innerSession.SaveChanges();
        });




    }


    #region ReqsIntoRaven

    public class QueryParameters
    {
    }

    public class Root
    {
        public string Query { get; set; }
        public int Start { get; set; }
        public int PageSize { get; set; }
        public bool DisableCaching { get; set; }
        public QueryParameters QueryParameters { get; set; }
    }

    record SimpleQuery(string Query, int Results, string Id = null);
    public static void InsertQueriesIntoRaven()
    {
        long counter = 0;
        List<SimpleQuery> record = new();
        using var session = DocumentStoreHolder.Store.OpenSession(new SessionOptions { NoCaching = true });
        using (StreamReader sr = new StreamReader(@"questions_search.reqs"))
        {
            while (sr.Peek() >= 0)
            {
                var json = sr.ReadLine();
                if (json is null)
                    continue;
                var query = JsonConvert.DeserializeObject<Root>(json);
                var count = session.Advanced.RawQuery<Item>(query.Query).NoTracking().NoCaching().Count();
                record.Add(new(query.Query, count));
                Console.WriteLine($"Query no {counter++} added with result no {count}");
                if (record.Count == 512)
                {
                    using var bulk = DocumentStoreHolder.Store.BulkInsert();
                    foreach (var r in record)
                        bulk.Store(r);
                    record.Clear();
                }
            }
        }

    }

    #endregion


    public static void SaveQueriesIntoFile()
    {
        using var session = DocumentStoreHolder.Store.OpenSession(new SessionOptions { NoCaching = true });

        var queries = session.Query<QueryGenerated>().Select(i => i.Query);

        using FileStream csvFile = File.Create(@"queries.csv");
        using var writer = new StreamWriter(csvFile);
        using var csvWriter = new CsvWriter(writer, CultureInfo.CurrentCulture);

        csvWriter.WriteField("Query");
        csvWriter.NextRecord();
        foreach (var q in queries)
        {
            csvWriter.WriteField(q);
            csvWriter.NextRecord();
        }

        writer.Flush();
    }

    #region QueryGenerators



    public static void GenerateComplexQueries()
    {
        using var session = DocumentStoreHolder.Store.OpenSession();
        const int AmountOfQueries = 10000;
        var random = new Random();
        var allPair = session.Query<AndQueries>().ToList();
        var list = new List<QueryGenerated>();
        for (int queryId = 0; queryId < AmountOfQueries; queryId++)
        {
            var isBoostingInvolved = random.Next() % 2 == 0;
            var stackDepth = random.Next(1, 10);
            var query = $"from index '{IndexName}' where {RecursivelyGenerateQueries(stackDepth, isBoostingInvolved)}";


            var count = session.Advanced.RawQuery<Item>(query).Count();
            if (count < 1000 * stackDepth)
            {
                list.Add(new QueryGenerated() { Query = query, Count = count, Depth = stackDepth, AverageReqTime = 0 });

                Console.WriteLine("bingo");
            }
            else
                Console.WriteLine("ups");

            if (list.Count == 500)
            {
                using (var bulk = DocumentStoreHolder.Store.BulkInsert())
                    foreach (var toUpload in list)
                        bulk.Store(toUpload);
                list.Clear();
            }
        }


        string RecursivelyGenerateQueries(in int stackDepthMax, bool appendBoosting, int depth = 1)
        {
            if (stackDepthMax == depth)
            {
                var idOfPackage = random.Next(0, allPair.Count);
                var pair = allPair[idOfPackage];
                var leftTerms = session.Load<OrQueries>(pair.LeftId);
                var rightTerms = session.Load<OrQueries>(pair.RightId);

                return appendBoosting switch
                {
                    true => AppendBoost($"{GetOrOrInClauseFromTerms(leftTerms.Terms)} and {GetOrOrInClauseFromTerms(rightTerms.Terms)}"),
                    false => $"({GetOrOrInClauseFromTerms(leftTerms.Terms)} and {GetOrOrInClauseFromTerms(rightTerms.Terms)})"
                };
            }


            var left = RecursivelyGenerateQueries(stackDepthMax, appendBoosting, depth + 1);
            var right = RecursivelyGenerateQueries(stackDepthMax, appendBoosting, depth + 1);

            var makeItHard = random.Next() % 5 == 0;


            var result = appendBoosting switch
            {
                true => AppendBoost($"{left} or {right}"),
                false => $"(" +
                         $"{left} or {right}" +
                         $")"
            };

            return makeItHard ? $"(true and {result})" : result;
        }

        string AppendBoost(string innerQuery)
        {
            var boostValue = random.Next(0, 100) * random.NextDouble();
            if (boostValue < 0.0001)
                boostValue = 0;
            return $"boost({innerQuery}, {boostValue})";
        }

        string GetOrOrInClauseFromTerms(List<string> terms)
        {
            bool shouldUseIn = random.Next() % 2 == 0;
            var query = shouldUseIn
                ? $"(" +
                  $"Title " +
                  $"in " +
                  $"(" +
                  $"{string.Join(", ", terms.Select(i => $"\"{i}\""))}" +
                  $")" +
                  $")"
                : $"(" +
                  $"{string.Join(" or ", terms.Select(i => $"Title = \"{i}\""))}" +
                  $")";

            return query;
        }
    }


    public const int ChunkSize = 1024;
    record AndQueries(string Id, int Count, string LeftId, string RightId);
    /// <summary>
    /// Generates AND clauses with inner ORs. We're doing this by randomly checking intersection of OR generated in GenerateOrQueries
    /// </summary>
    public static void GenerateAndPairs()
    {
        using var session = DocumentStoreHolder.Store.OpenSession();
        var candidates = Queryable.Select(session.Query<OrQueries>().Where(i => i.AverageResult > 300), i => i.Id).ToList();


        var random = new Random();
        var threadList = Enumerable.Range(0, 100).Select(i => new Thread(new ThreadStart(() =>
        {
            var sessionCounter = 0;
            var localSession = DocumentStoreHolder.Store.OpenSession();
            foreach (var bulk in Enumerable.Range(0, 100_000_000).Select(i => i))
            {
                var leftIndex = random.Next(0, candidates.Count);
                var rightIndex = random.Next(0, candidates.Count);
                if (leftIndex == rightIndex)
                    continue;


                var downloadLeftTerms = localSession.Load<OrQueries>(candidates[leftIndex]);
                var downloadRightTerms = localSession.Load<OrQueries>(candidates[rightIndex]);
                var count = localSession.Advanced.RawQuery<Item>(GetInQueries(downloadLeftTerms.Terms, downloadRightTerms.Terms)).Count();

                if (count >= 10)
                {
                    localSession.Store(new AndQueries(null, count, candidates[leftIndex], candidates[rightIndex]));
                    if (sessionCounter++ == 1)
                    {
                        Console.WriteLine($"Found new pair ({candidates[leftIndex]}, {candidates[rightIndex]}).");
                        localSession.SaveChanges();
                        localSession.Dispose();
                        sessionCounter = 0;
                        localSession = DocumentStoreHolder.Store.OpenSession();
                    }
                }
            }

            localSession.SaveChanges();
            localSession.Dispose();
        }))).ToList();

        Parallel.ForEach(threadList, i => i.Start());

        string GetInQueries(List<string> leftTerms, List<string> rightTerms) =>
            $"from index '{IndexName}' where Title in ({string.Join(", ", leftTerms.Select(i => $"'{i.Replace("'", "")}'"))})" +
            $"and Title in ({string.Join(", ", rightTerms.Select(i => $"'{i.Replace("'", "")}'"))})";
    }


    /// <summary>
    ///  Generate OR / IN clauses.
    /// </summary>
    public static void GenerateOrQueries()
    {
        var session = DocumentStoreHolder.Store.OpenSession();
        //Data here is well distrubited so lets look for pair thats give us big stack and then try to limit it
        //Lets create 10k chunks and discover chunks

        //Sometimes we've added to much terms. Lets clean this up.
        var operation = DocumentStoreHolder.Store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = "from 'OrQueries' where Terms.Length > 20" }));


        var results = session.Query<TermFrequency>().OrderByDescending(i => i.Count, OrderingType.Long).ToList();
        List<List<TermFrequency>> chunks = new List<List<TermFrequency>>();
        List<(int Max, int Min)> chunksDetails = new();
        var startChunkIndex = -1;
        var firstUniqueChunk = -1;
        for (int i = 0; i < results.Count; i += ChunkSize)
        {
            var rightBorder = i + ChunkSize >= results.Count ? results.Count : i + ChunkSize;
            var chunk = new List<TermFrequency>();

            for (int left = i; left < rightBorder; ++left)
                chunk.Add(results[left]);

            if (chunk[0].Count <= 1024 && startChunkIndex == -1)
                startChunkIndex = chunksDetails.Count + 1;

            if (chunk[0].Count == 1 && firstUniqueChunk == -1)
                firstUniqueChunk = chunksDetails.Count + 1;


            chunks.Add(chunk);
            chunksDetails.Add((chunk[0].Count, chunk[^1].Count));
        }

        results = null;
        var averageOutputSize = new int[] { 64, 128, 256, 512, 1024, 2048, 4096 };

        var counter = 0;
        var random = new Random();
        foreach (var wantedSize in Enumerable.Range(0, 100_000_000).Select(i => random.Next(100, 10_000)))
        {
            var currentAverageOutput = 0L; //Lets assume all are unique.
            var termsList = new List<string>();
            while (currentAverageOutput < wantedSize)
            {
                var randomChunk = random.Next(startChunkIndex, firstUniqueChunk);
                var chunk = chunks[randomChunk];
                if (chunk.Count == 0)
                    continue;
                var randomItemInsideChunk = chunk[random.Next(0, chunk.Count)];
                currentAverageOutput += randomItemInsideChunk.Count;
                termsList.Add(randomItemInsideChunk.Term);
            }

            if (termsList.Count is < 2 or > 20)
                continue;

            session.Store(new OrQueries(string.Empty, wantedSize, termsList));
            //Console.WriteLine(GetInQueries(termsList) + "\n\n");
            if (counter++ == 128)
            {
                session.SaveChanges();
                session.Dispose();
                session = DocumentStoreHolder.Store.OpenSession();
                counter = 0;
            }
        }

        session.SaveChanges();
        session?.Dispose();
    }

    record OrQueries(string Id, int AverageResult, List<string> Terms);
    #endregion



    #region TermStatistics
    record TermFrequency(string Term, int Count);

    public static void GetMostSimpleStatisticsAboutTermStored()
    {
        using var session = DocumentStoreHolder.Store.OpenSession();
        var results = Queryable.Where(session.Query<TermFrequency>(), i => i.Count != 1).OrderByDescending(i => i.Count, OrderingType.Long).ToList();
        var average = results.Average(i => i.Count);
        var median = results[results.Count / 2].Count;

        Console.WriteLine($"Average {average}  Median: {median}");
    }


    /// <summary>
    ///  Full-text-search will produce thousands of terms and we want gather statistics about them and put into DB.
    /// </summary>
    public static void GetTermFrequencyFromIndex()
    {
        var batch = 0L;
        var shouldStop = false;
        string fromValue = null;
        do
        {
            ConcurrentBag<TermFrequency> frequencyList = new();
            var terms = DocumentStoreHolder.Store
                .Maintenance
                .Send(new GetTermsOperation(IndexName, "Title", fromValue, 1024 * 128));

            if (terms.Length == 0)
            {
                shouldStop = true;
                break;
            }

            Console.WriteLine($"Starting batch no {batch}");
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            var standardChunk = 1024;
            var list = new List<(int start, int end)>();
            for (var page = 0; page < terms.Length; page += standardChunk)
            {
                var end = page + standardChunk > terms.Length ? terms.Length : page + standardChunk;
                list.Add((page, end));
            }


            var queries = Parallel.ForEach(list, list =>
            {
                Console.WriteLine($"\t\tStarted chunk no {list.start / standardChunk}");
                using var session = DocumentStoreHolder.Store.OpenSession();
                for (int beg = list.start; beg < list.end; ++beg)
                {
                    var term = terms[beg];
                    var count = session.Query<Item>(IndexName).Count(i => i.Title == term);
                    frequencyList.Add(new(term, count));
                }

                Console.WriteLine($"\t\tFinished chunk no {list.start / standardChunk}");
            });

            {
                using var bulkInsert = DocumentStoreHolder.Store.BulkInsert();
                foreach (var stat in frequencyList)
                {
                    bulkInsert.Store(stat);
                }
            }
            Console.WriteLine($"Finished batch no {batch++} in {stopwatch.ElapsedMilliseconds / 1000} s.");


            fromValue = terms[^1];
        } while (shouldStop == false);
    }

    #endregion
}

public class DocumentStoreHolder
{
    // Use Lazy<IDocumentStore> to initialize the document store lazily. 
    // This ensures that it is created only once - when first accessing the public `Store` property.
    private static Lazy<IDocumentStore> store = new Lazy<IDocumentStore>(CreateStore);

    public static IDocumentStore Store => store.Value;

    private static IDocumentStore CreateStore()
    {
        IDocumentStore store = new DocumentStore()
        {
            // Define the cluster node URLs (required)
            Urls = new[]
            {
                "http://127.0.0.1:8080",
                /*some additional nodes of this cluster*/
            },

            // Set conventions as necessary (optional)
            Conventions = { MaxNumberOfRequestsPerSession = int.MaxValue, UseOptimisticConcurrency = true, },

            // Define a default database (optional)
            Database = "stackoverflow",
            // Initialize the Document Store
        }.Initialize();

        return store;
    }
}
