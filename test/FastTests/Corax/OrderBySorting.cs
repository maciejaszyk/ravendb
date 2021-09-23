﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax.Queries;
using Corax;
using FastTests.Voron;
using Sparrow.Server;
using Voron;
using Xunit.Abstractions;
using Xunit;
using Sparrow.Threading;

namespace FastTests.Corax
{
    public class OrderBySortingTests : StorageTest
    {
        private List<IndexSingleNumericalEntry<long>> longList = new();
        private IndexSearcher _indexSearcher;
        private const int IndexId = 0, ContentId = 1;

        public OrderBySortingTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void OrderByNumber()
        {
            PrepareData();
            IndexEntries();
            longList.Sort(CompareDescending);
            using var searcher = new IndexSearcher(Env);
            {
                var match1 = searcher.StartWithQuery("Id", "l");
                var match = searcher.OrderByDescending(match1, ContentId, MatchCompareFieldType.Integer);
                
                List<string> sortedByCorax = new();
                Span<long> ids = stackalloc long[2048];
                int read = 0;
                do
                {
                    read = match.Fill(ids);
                    for (int i = 0; i < read; ++i)
                        sortedByCorax.Add(searcher.GetIdentityFor(ids[i]));
                } 
                while (read != 0);

                for (int i = 0; i < longList.Count; ++i)
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);
                
                Assert.Equal(1000, sortedByCorax.Count);
            }
        }

        private static int CompareAscending(IndexSingleNumericalEntry<long> value1, IndexSingleNumericalEntry<long> value2)
        {
            return value1.Content.CompareTo(value2.Content);
        }

        private static int CompareDescending(IndexSingleNumericalEntry<long> value1, IndexSingleNumericalEntry<long> value2)
        {
            return value2.Content.CompareTo(value1.Content);
        }
        private void PrepareData()
        {
            for (int i = 0; i < 1000; ++i)
            {
                longList.Add(new IndexSingleNumericalEntry<long>
                {
                    Id = $"list/{i}",
                    Content = i
                });
            }
        }

        private void IndexEntries()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            Dictionary<Slice, int> knownFields = CreateKnownFields(bsc);

            const int bufferSize = 4096;
            using var _ = bsc.Allocate(bufferSize, out ByteString buffer);

            {
                using var indexWriter = new IndexWriter(Env);
                foreach (var entry in longList)
                {
                    var entryWriter = new IndexEntryWriter(buffer.ToSpan(), knownFields);
                    var data = CreateIndexEntry(ref entryWriter, entry);
                    indexWriter.Index(entry.Id, data, knownFields);
                }
                indexWriter.Commit();
            }
        }

        private Span<byte> CreateIndexEntry(ref IndexEntryWriter entryWriter, IndexSingleNumericalEntry<long> entry)
        {
            entryWriter.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
            entryWriter.Write(ContentId, Encoding.UTF8.GetBytes(entry.Content.ToString()), entry.Content, Convert.ToDouble(entry.Content));
            entryWriter.Finish(out var output);
            return output;
        }

        private Dictionary<Slice, int> CreateKnownFields(ByteStringContext bsc)
        {
            Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(bsc, "Content", ByteStringType.Immutable, out Slice contentSlice);

            return new Dictionary<Slice, int>
            {
                [idSlice] = IndexId,
                [contentSlice] = ContentId,
            };
        }

        private class IndexSingleNumericalEntry<T>
        {
            public string Id { get; set; }
            public T Content { get; set; }
        }
    }
}
