using System;
using System.Threading.Tasks;
using Tests.Infrastructure;
using System.Linq;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static  void Main(string[] args)
    {
        var defaultOptions = FastTests.RavenTestBase.Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        var i = 0;
        while (true)
        {
            try
            {
                Console.WriteLine($"Test no {i++}");
                using var testOutputHelper = new ConsoleTestOutputHelper();
                using var testClass = new FastTests.Client.Indexing.IndexExtensionFromClient(testOutputHelper);
                testClass.CanUseMethodFromExtensionsInIndex_WithHashsetReturnType(defaultOptions);
            }
            catch (Exception e)
            {
                if (e.ToString().Contains("cluster") == false) // sometimes cluster fails due to connections overloads, shouldn't matter tbh
                    throw;
                Console.WriteLine("Connection problem");

            }
        }
    }
}
