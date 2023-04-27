using System;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Utils;
using Corax.Utils.Spatial;
using Spatial4n.Shapes;

namespace Corax.Queries;

unsafe partial struct SortingMatch
{
    public struct SpatialDescendingMatchComparer : ISpatialComparer
    {
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly delegate*<ref SpatialDescendingMatchComparer, long, long, int> _compareFunc;
        private readonly MatchCompareFieldType _fieldType;
        private readonly IPoint _point;
        private readonly double _round;
        private readonly SpatialUnits _units;
        
        public IPoint Point => _point;

        public double Round => _round;

        public SpatialUnits Units => _units;

        public FieldMetadata Field => _field;
        
        public MatchCompareFieldType FieldType => _fieldType;

        public SpatialDescendingMatchComparer(IndexSearcher searcher, in OrderMetadata metadata)
        {
            _searcher = searcher;
            _field = metadata.Field;
            _fieldType = metadata.FieldType;
            _point = metadata.Point;
            _round = metadata.Round;
            _units = metadata.Units;
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
