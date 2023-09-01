using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Corax.Queries;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Raven.Server.Utils;
using SlowTests.Sharding.Cluster;
using Xunit;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static async Task Main(string[] args)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);
        Console.WriteLine("Is 32: " + Sparrow.Platform.PlatformDetails.Is32Bits);

        Parallel.For(0, 10_000, new ParallelOptions() {MaxDegreeOfParallelism = 4}, i =>
        {
            Console.WriteLine($"Starting to run {i}");
            try
            {
                using (var testOutputHelper = new ConsoleTestOutputHelper())
                using (var test = new FastTests.Corax.StreamingOptimization_DataTests(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;
                    test.UnboundedRangeQueries(UnaryMatchOperation.GreaterThan, OrderingType.Double, ascending: true, value: 2D);
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
            }
        });
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
