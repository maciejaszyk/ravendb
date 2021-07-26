using Antlr4.Runtime;
using BenchmarkDotNet.Attributes;
using Newtonsoft.Json;
using RqlGrammar;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;

namespace Benchmark
{
    [MarkdownExporter, AsciiDocExporter, HtmlExporter, CsvExporter, RPlotExporter]
    public class ASTGeneratorBenchmark
    {
        private List<string> queries;
        public ASTGeneratorBenchmark()
        {
            queries = JsonConvert.DeserializeObject<List<string>>(System.IO.File.ReadAllText(@"data.json")).ToList();
            Console.WriteLine("Queries count: " + queries.Count);
        }

        [Benchmark]
        public void AntlrASTGenerator()
        {
            foreach (string query in queries)
            {
                var lexer = new RqlLexer(new AntlrInputStream(query));
                var parser = new RqlParser(new CommonTokenStream(lexer));
                parser.prog();
            }
        }

        [Benchmark]
        public void RavenDBASTGenerator()
        {
            foreach (string query in queries)
            {
                
                var queryParser = new QueryParser();
                queryParser.Init(query);

                Query q = queryParser.Parse(QueryType.Select);

                //catch(Exception e){
                //    Console.WriteLine($"Real: {query}");
                //    Console.WriteLine(e.ToString());
                //}
                // var qp = new QueryParser();
                // qp.Init(query);
                //  qp.Parse(QueryType.Select);
            }
        }
    }
}
