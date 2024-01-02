using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Corax.Querying.Matches;
using FastTests.Corax;
using Tests.Infrastructure;
using Raven.Server.Utils;
using SlowTests.Corax;
using SlowTests.Sharding.Cluster;
using Xunit;
using FastTests.Voron.Util;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Database;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static void Main(string[] args)
    {
        Console.WriteLine("Test");
        Console.WriteLine(Process.GetCurrentProcess().Id);
        Console.WriteLine($"Is32: {Sparrow.Platform.PlatformDetails.Is32Bits}");
        Parallel.For(0, 1_000_000, new ParallelOptions() {MaxDegreeOfParallelism = 2}, i =>
            {
                Console.WriteLine($"Starting to run {i}");

                try
                {
                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    using (var test = new FastTests.Corax.StreamingOptimization_DataTests(testOutputHelper))
                    {
                        DebuggerAttachedTimeout.DisableLongTimespan = true;
                        test.UnboundedRangeQueries(UnaryMatchOperation.GreaterThan, OrderingType.Double, true, 2.0D);
                    }
                }
                catch (DatabaseLoadFailureException)
                {
                    Console.WriteLine(nameof(DatabaseLoadFailureException));
                } // skip
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
        );
    }

    private static void TryRemoveDatabasesFolder()
    {
        var p = System.AppDomain.CurrentDomain.BaseDirectory;
        var dbPath = Path.Combine(p, "Databases");
        if (Directory.Exists(dbPath))
        {
            try
            {
                Directory.Delete(dbPath, true);
                Assert.False(Directory.Exists(dbPath), "Directory.Exists(dbPath)");
            }
            catch
            {
                Console.WriteLine($"Could not remove Databases folder on path '{dbPath}'");
            }
        }
    }
}
