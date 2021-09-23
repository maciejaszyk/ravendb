﻿using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Text;
using Corax;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public class SingleEntryWriterScope : IWriterScope
    {
        private readonly List<int> _lengthList;
        private readonly ByteStringContext _allocator;

        public SingleEntryWriterScope(List<int> lengthList, ByteStringContext allocator)
        {
            _allocator = allocator;
            _lengthList = lengthList;
        }
        
        public void Write(int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter)
        {
            entryWriter.Write(field, value);
        }

        public List<int> GetLengthList() => _lengthList;

        public void Write(int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            entryWriter.Write(field, value, longValue, doubleValue);
        }

        public void Write(int field, string value, ref IndexEntryWriter entryWriter)
        {
            using (_allocator.Allocate(Encoding.UTF8.GetByteCount(value), out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                entryWriter.Write(field, buffer.ToSpan());
            }
        }

        public void Write(int field, string value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            using (_allocator.Allocate(Encoding.UTF8.GetByteCount(value), out var buffer))
            {
                var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
                buffer.Truncate(length);
                entryWriter.Write(field, buffer.ToSpan(), longValue, doubleValue);
            }
        }
    }
}
