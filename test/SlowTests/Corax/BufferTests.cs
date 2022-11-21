using System;
using System.Text.Unicode;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using FastTests.Voron;
using Raven.Client.Documents.Linq;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class BufferTests : StorageTest
{

    public BufferTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void GrowableBufferInBoostingMatchWillCreateBufferWithProperSize()
    {
        const int IdIndex = 0, NumbersId = 1, ContentId = 2;
        const string IdName = "id()", NumbersName = "Numbers", ContentName = "Content";

        using var fieldMapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddDefaultAnalyzer(Analyzer.DefaultLowercaseAnalyzer)
            .AddBinding(IdIndex, IdName, Analyzer.DefaultLowercaseAnalyzer)
            .AddBinding(NumbersId, NumbersName, Analyzer.DefaultLowercaseAnalyzer)
            .AddBinding(ContentId, ContentName, Analyzer.DefaultLowercaseAnalyzer)
            .Build();

        using (var indexWriter = new IndexWriter(Env, fieldMapping))
        using (var indexEntryBuilder = new IndexEntryWriter(Allocator, fieldMapping))
        {
            for (var i = 0; i < 1024; ++i) //bufferSize = 4 * Size.Kilobyte;   
            {
                indexEntryBuilder.Write(IdIndex, Encodings.Utf8.GetBytes($"test{i}"));
                indexEntryBuilder.Write(NumbersId, Encodings.Utf8.GetBytes($"{i}"), i, i);
                indexEntryBuilder.Write(ContentId, Encodings.Utf8.GetBytes($"SecretByte{i}"), i, i);
                indexEntryBuilder.Finish(out var outputField);
                indexWriter.Index($"test/{i}", outputField.ToSpan());
            }

            indexWriter.Commit();
        }


        using (var indexSearcher = new IndexSearcher(Env, fieldMapping))
        {
            var match1 = indexSearcher.Boost(indexSearcher.GreaterThanQuery(NumbersName, -1L, default(NullScoreFunction)), 1f);
            var match2 = indexSearcher.StartWithQuery(ContentName, "Sec", default, ContentId);
            var and = indexSearcher.And(match1, match2);
            Span<long> ids = stackalloc long[256];
            var read = 0;

            while ((read = and.Fill(ids)) != 0)
            {
                //should increase size
            }
        }
    }
}
