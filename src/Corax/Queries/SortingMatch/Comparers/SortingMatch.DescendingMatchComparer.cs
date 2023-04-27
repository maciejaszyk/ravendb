using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Utils;

namespace Corax.Queries;

unsafe partial struct SortingMatch
{
    public unsafe struct DescendingMatchComparer : IMatchComparer
    {
        private readonly FieldMetadata _field;
        private readonly MatchCompareFieldType _fieldType;

        public FieldMetadata Field => _field;
        public MatchCompareFieldType FieldType => _fieldType;

        public DescendingMatchComparer(OrderMetadata orderMetadata)
        {
            _field = orderMetadata.Field;
            _fieldType = orderMetadata.FieldType;

            if (orderMetadata.Ascending == true)
                throw new ArgumentException($"The metadata for field '{orderMetadata.Field.FieldName}' is not marked as 'Ascending == false' ");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareNumerical<T>(T sx, T sy) where T : unmanaged, INumber<T>
        {
            return -BasicComparers.CompareAscending(sx, sy);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return -BasicComparers.CompareAscending(sx, sy);
        }
    }
}
