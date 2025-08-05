using System;
using System.Buffers;
using System.IO;
using System.Runtime.InteropServices;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Voron.Util;

namespace Corax.Indexing;

public partial class IndexWriter
{
    /// <summary>
    /// FieldBuffers are used to prepare field terms in sorted order without allocating native memory and do not changing the orders of IndexedField properties since we're linking them via positions in buffers.
    /// 
    /// </summary>
    private class FieldBuffers<TKey, TLookupKey> : IDisposable
        where TKey : unmanaged
        where TLookupKey : struct, ILookupKey
    {
        private readonly IndexWriter _parent;
        public const int BatchSize = 1024;

        private ContextBoundNativeList<TKey> _sortedTerms;
        private ContextBoundNativeList<int> _termIndexes;

        public TLookupKey[] Keys;
        public ContextBoundNativeList<int> PageOffsets;
        public ContextBoundNativeList<long> PostListIds;
        private ContextBoundNativeList<int> _entriesOffsets;

        public void PrepareTerms(IndexedField field, out Span<TKey> terms, out Span<int> indexes)
        {
            int termsCount;
            if (typeof(TKey) == typeof(Slice))
                termsCount = field.Textual.Count;
            else if (typeof(TKey) == typeof(long))
                termsCount = field.Longs.Count;
            else if (typeof(TKey) == typeof(double))
                termsCount = field.Doubles.Count;
            else
                throw new InvalidDataException($"Type {typeof(TKey).FullName} is not supported");

            if (_sortedTerms.Capacity < termsCount)
            {
                _sortedTerms.EnsureCapacityFor(termsCount);
                _sortedTerms.Count = _sortedTerms.Capacity;

                _termIndexes.EnsureCapacityFor(termsCount);
                _termIndexes.Count = _termIndexes.Capacity;
            }

            int idx = 0;
            var sortedTermsSpan = _sortedTerms.ToSpan();
            var termIndexesSpan = _termIndexes.ToSpan();
            if (typeof(TKey) == typeof(Slice))
            {
                foreach (var (k, v) in field.Textual)
                {
                    sortedTermsSpan[idx] = (TKey)(object)k;
                    termIndexesSpan[idx] = v;
                    idx++;
                }
            }

            if (typeof(TKey) == typeof(long))
            {
                foreach (var (k, v) in field.Longs)
                {
                    sortedTermsSpan[idx] = (TKey)(object)k;
                    termIndexesSpan[idx] = v;
                    idx++;
                }
            }

            if (typeof(TKey) == typeof(double))
            {
                foreach (var (k, v) in field.Doubles)
                {
                    sortedTermsSpan[idx] = (TKey)(object)k;
                    termIndexesSpan[idx] = v;
                    idx++;
                }
            }

            terms = _sortedTerms.ToSpan().Slice(0, termsCount);
            indexes = _termIndexes.ToSpan().Slice(0, termsCount);

            if (typeof(TKey) == typeof(Slice))
                (MemoryMarshal.Cast<TKey, Slice>(terms)).Sort(indexes, SliceComparer.Instance);
            else if (typeof(TKey) == typeof(long))
                (MemoryMarshal.Cast<TKey, long>(terms)).Sort(indexes);
            else if (typeof(TKey) == typeof(double))
                (MemoryMarshal.Cast<TKey, double>(terms)).Sort(indexes);
            else
                throw new InvalidDataException($"Type {typeof(TKey).FullName} is not supported");
        }

        public FieldBuffers(IndexWriter parent)
        {
            _parent = parent;
            Keys = ArrayPool<TLookupKey>.Shared.Rent(BatchSize);
            PageOffsets = new(parent._transaction.Allocator, BatchSize);
            PageOffsets.Count = BatchSize;

            PostListIds = new(parent._transaction.Allocator, BatchSize);
            PostListIds.Count = BatchSize;

            _entriesOffsets = new(parent._transaction.Allocator, BatchSize);
            _entriesOffsets.Count = BatchSize;

            _sortedTerms = new(parent._transaction.Allocator);
            _termIndexes = new(_parent._transaction.Allocator);
        }

        public void Dispose()
        {
            PostListIds.Dispose();
            PageOffsets.Dispose();
            _entriesOffsets.Dispose();
            _sortedTerms.Dispose();
            _termIndexes.Dispose();

            if (Keys != null && typeof(TLookupKey) == typeof(CompactTree.CompactKeyLookup))
            {
                var llt = _parent._transaction.LowLevelTransaction;
                var ctk = (CompactTree.CompactKeyLookup[])(object)Keys;
                for (int i = 0; i < Keys.Length; i++)
                {
                    ref var k = ref ctk[i].Key;
                    if (k != null)
                    {
                        llt.ReleaseCompactKey(ref k);
                    }
                }
            }
            
            if (Keys != null)
                ArrayPool<TLookupKey>.Shared.Return(Keys);

            if (Keys != null)
                ArrayPool<TLookupKey>.Shared.Return(Keys);

            PostListIds = default;
            PageOffsets = default;
            _entriesOffsets = default;
            _sortedTerms = default;
            _termIndexes = default;
            Keys = null;
        }
    }
}
