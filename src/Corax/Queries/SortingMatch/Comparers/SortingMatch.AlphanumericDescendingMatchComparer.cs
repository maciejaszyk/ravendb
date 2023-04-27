﻿using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Utils;

namespace Corax.Queries;

unsafe partial struct SortingMatch
{
    public unsafe struct AlphanumericDescendingMatchComparer : IMatchComparer
    {
        private readonly FieldMetadata _field;
        private readonly MatchCompareFieldType _fieldType;

        public FieldMetadata Field => _field;
        public MatchCompareFieldType FieldType => _fieldType;

        public AlphanumericDescendingMatchComparer(OrderMetadata orderMetadata)
        {
            _field = orderMetadata.Field;
            _fieldType = orderMetadata.FieldType;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareNumerical<T>(T sx, T sy) where T : unmanaged, INumber<T>
        {
            return -BasicComparers.CompareAscending(sx, sy);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return -BasicComparers.CompareAlphanumericAscending(sx, sy);
        }
    }
}
