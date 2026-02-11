using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Platform.Win32;

namespace Corax.Utils;

public unsafe struct GrowableBitArray : IDisposable
{
    internal static readonly int MaxCapacityPerBitmap = (int.MaxValue - sizeof(ByteStringStorage)) / sizeof(ulong);
    internal static readonly long MaxCapacityPerBitmapInBits = MaxCapacityPerBitmap * 64L;
    private BitArray[] _bitArrays;
    private readonly long _capacity;

    /// <summary>
    /// The owner must update this count manually.
    /// </summary>
    public long Count;

    /// <summary>
    /// Creates a new bit array. It accepts when bits id is between [0, capacity]
    /// </summary>
    public GrowableBitArray(ByteStringContext allocator, long capacity)
    {
        _capacity = capacity + 1; // ensure it's not zero and handles the last bit inclusively.
        var numberOfUlongsToAllocate = _capacity / 64 + (_capacity % 64 == 0 ? 0 : 1);
        var numberOfBitArrays = (int)Math.Ceiling(numberOfUlongsToAllocate / (double)MaxCapacityPerBitmap);
        _bitArrays = new BitArray[numberOfBitArrays];
        var lastChunkSize = (int)(numberOfUlongsToAllocate - (long)(numberOfBitArrays - 1) * MaxCapacityPerBitmap);
        for (int i = 0; i < numberOfBitArrays; ++i)
        {
            _bitArrays[i] = new BitArray(allocator, i == numberOfBitArrays - 1
                ? lastChunkSize
                : MaxCapacityPerBitmap);
        }
    }

    public void Invert()
    {
        for (var bitArrayIdx = 0; bitArrayIdx < _bitArrays.Length; ++bitArrayIdx)
        {
            _bitArrays[bitArrayIdx].Invert();
        }
    }

    //Perform AND operation on current buffer.
    public void And(GrowableBitArray other)
    {
        Debug.Assert(_capacity == other._capacity);
        for (var bitArrayIdx = 0; bitArrayIdx < _bitArrays.Length; ++bitArrayIdx)
        {
            _bitArrays[bitArrayIdx].And(other._bitArrays[bitArrayIdx]);
        }
    }

    //Perform AND operation on current buffer.
    public void Or(GrowableBitArray other)
    {
        Debug.Assert(_capacity == other._capacity);

        for (var bitArrayIdx = 0; bitArrayIdx < _bitArrays.Length; ++bitArrayIdx)
        {
            _bitArrays[bitArrayIdx].Or(other._bitArrays[bitArrayIdx]);
        }
    }

    public Iterator GetIterator(long from) => new Iterator(this, from);

    //todo
    public static IEnumerable<long> Probe(GrowableBitArray source, Random random)
    {
        HashSet<long> seen = new();
        while (seen.Count < source.Count)
        {
            long next = random.Next((int)source._capacity);
            if (seen.Add(next) && source.Contains(next))
            {
                yield return next;
            }
        }
    }

    public ref struct Iterator : IEnumerator<long>
    {
        private readonly GrowableBitArray _bitArray;
        private readonly long _from;
        private int _currentBitArrayIdx;
        private long _currentShift;
        private BitArray.Iterator _iterator;

        public Iterator(GrowableBitArray bitArray, long from)
        {
            _bitArray = bitArray;
            _from = from;
            Reset();
        }

        public bool MoveNext()
        {
            while (_currentBitArrayIdx < _bitArray._bitArrays.Length)
            {
                if (_iterator.MoveNext())
                {
                    return true;
                }

                _currentShift += MaxCapacityPerBitmapInBits;
                _currentBitArrayIdx++;

                if (_currentBitArrayIdx < _bitArray._bitArrays.Length)
                    _iterator = new(_bitArray._bitArrays[_currentBitArrayIdx], 0);
            }

            return false;
        }

        public int Fill(Span<long> dest)
        {
            var totalRead = 0;
            while (dest.IsEmpty == false && _currentBitArrayIdx < _bitArray._bitArrays.Length)
            {
                if (_iterator.Fill(dest) is var read and > 0)
                {
                    totalRead += read;
                    dest = dest[read..];
                    continue;
                }

                _currentShift += MaxCapacityPerBitmapInBits;
                _currentBitArrayIdx++;

                if (_currentBitArrayIdx < _bitArray._bitArrays.Length)
                    _iterator = new(_bitArray._bitArrays[_currentBitArrayIdx], 0);
            }

            return totalRead;
        }

        public void Reset()
        {
            _currentBitArrayIdx = (int)(_from / MaxCapacityPerBitmapInBits);
            _currentShift = _currentBitArrayIdx * MaxCapacityPerBitmapInBits;

            if (_currentBitArrayIdx <= _bitArray._bitArrays.Length)
                _iterator = new(_bitArray._bitArrays[_currentBitArrayIdx], (int)(_from % MaxCapacityPerBitmapInBits));
        }

        public long Current => _currentShift + _iterator.Current;

        object IEnumerator.Current
        {
            get => Current;
        }

        public void Dispose()
        {
            //nothing to dispose
        }
    }

#if DEBUG
    public bool IsValid
    {
        get
        {
            for (int i = 0; i < _bitArrays.Length; ++i)
                if (_bitArrays[i].IsValid == false)
                    return false;
            return true;
        }
    }
#endif

    public void Add(Span<long> positions)
    {
        if (_bitArrays.Length == 1)
        {
            _bitArrays[0].Add(positions);
        }
        else
        {
            foreach (var pos in positions)
                Add(pos);
        }
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Add(long pos)
    {
        if (pos >= _capacity)
            throw new ArgumentOutOfRangeException($"Tried to modify the bit at position '{pos}', however the capacity is only {_capacity}");

        if (_bitArrays.Length == 1)
            return _bitArrays[0].Add(pos);


        var bitmapIdx = (int)(pos / MaxCapacityPerBitmapInBits);
        return _bitArrays[(int)bitmapIdx].Add(pos - bitmapIdx * MaxCapacityPerBitmapInBits);
    }

    public bool Contains(long pos)
    {
        var bitmapIdx = (int)(pos / MaxCapacityPerBitmapInBits);
        return _bitArrays[(int)bitmapIdx].Contains(pos - bitmapIdx * MaxCapacityPerBitmapInBits);
    }

    public void Dispose()
    {
        if (_bitArrays == null)
            return;

        for (int i = 0; i < _bitArrays.Length; ++i)
            _bitArrays[i].Dispose();
        _bitArrays = null;
    }

    private struct BitArray : IDisposable
    {
        private ulong* _bits;
        private IDisposable _memoryScope;
        private int _length;
#if DEBUG
        public bool IsValid = true;
#endif
        public BitArray(ByteStringContext allocator, int numberOfUlongsToAllocate)
        {
            _length = numberOfUlongsToAllocate;
            _memoryScope = allocator.Allocate(numberOfUlongsToAllocate * sizeof(ulong), out ByteString memory);
            memory.ToSpan<ulong>().Clear();
            _bits = (ulong*)memory.Ptr;
#if DEBUG
            IsValid = true;
#endif
        }

        public void Or(BitArray other)
        {
            var currentPosition = 0;
            if (AdvInstructionSet.IsAcceleratedVector128)
            {
                var N = Vector512<ulong>.Count;
                for (currentPosition = 0; currentPosition + N <= _length; currentPosition += N)
                {
                    var current = Vector512.Load(_bits + currentPosition);
                    var otherCurrent = Vector512.Load(other._bits + currentPosition);
                    (current | otherCurrent).Store(_bits + currentPosition);
                }
            }

            for (; currentPosition < _length; currentPosition++)
            {
                _bits[currentPosition] &= other._bits[currentPosition];
            }
        }

        public void And(BitArray other)
        {
            var currentPosition = 0;
            if (AdvInstructionSet.IsAcceleratedVector128)
            {
                var N = Vector512<ulong>.Count;
                for (currentPosition = 0; currentPosition + N <= _length; currentPosition += N)
                {
                    var current = Vector512.Load(_bits + currentPosition);
                    var otherCurrent = Vector512.Load(other._bits + currentPosition);
                    (current & otherCurrent).Store(_bits + currentPosition);
                }
            }

            for (; currentPosition < _length; currentPosition++)
            {
                _bits[currentPosition] &= other._bits[currentPosition];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Add(long id)
        {
            var mask = 1UL << (int)(id & 63);
            var bucket = _bits + (int)(id >> 6);
            var result = *bucket & mask;
            *bucket |= mask;
            return result == 0;
        }

        // PERF: See scalar add for details. This is the unrolled version for efficiency.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SkipLocalsInit]
        public unsafe void Add(scoped Span<long> ids)
        {
            var currentIdx = 0;
            if (AdvInstructionSet.IsAcceleratedVector128)
            {
                var N = Vector512<ulong>.Count;
                ref var idsStart = ref MemoryMarshal.GetReference(ids);
                var mask = Vector512.Create(63L);
                for (; currentIdx + N <= ids.Length; currentIdx += N)
                {
                    var currentIds = Vector512.LoadUnsafe(ref Unsafe.Add(ref idsStart, currentIdx));
                    var positions = currentIds & mask;
                    var destinations = Vector512.ShiftRightLogical(currentIds.AsUInt64(), 6);
                    
                    _bits[destinations[0]] |= (1UL << (int)positions[0]);
                    _bits[destinations[1]] |= (1UL << (int)positions[1]);
                    _bits[destinations[2]] |= (1UL << (int)positions[2]);
                    _bits[destinations[3]] |= (1UL << (int)positions[3]);
                    _bits[destinations[4]] |= (1UL << (int)positions[4]);
                    _bits[destinations[5]] |= (1UL << (int)positions[5]);
                    _bits[destinations[6]] |= (1UL << (int)positions[6]);
                    _bits[destinations[7]] |= (1UL << (int)positions[7]);
                }
            }

            for (; currentIdx < ids.Length; currentIdx++)
                Add(ids[currentIdx]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(long id)
        {
            var mask = 1UL << (int)(id & 63);
            var bucket = _bits + (int)(id >> 6);
            return (*bucket & mask) != 0;
        }


        //adjusted from src/Raven.Server/Documents/Queries/LuceneIntegration/FastBitArray.cs:7
        public ref struct Iterator : IEnumerator<long>
        {
            private int _it;
            private ulong _bitmap = 0;
            private int _count = 0;
            private readonly BitArray _array;
            private readonly int _from;

            public Iterator(BitArray array, int from)
            {
                _from = from;
                _array = array;
                Reset();
            }

            public int Fill(Span<long> dest)
            {
                if (_it >= _array._length)
                    return 0;

                var currentIdx = 0;

                while (currentIdx < dest.Length)
                {
                    var offset = _it * 64;
                    while (currentIdx < dest.Length && _bitmap != 0)
                    {
                        ulong t = _bitmap & (ulong)-(long)_bitmap;
                        _count = BitOperations.TrailingZeroCount(_bitmap);
                        _bitmap ^= t;
                        dest[currentIdx++] = offset + _count;
                    }

                    _it++;
                    if (_it >= _array._length)
                        break;

                    _bitmap = *(_array._bits + _it);
                }

                return currentIdx;
            }

            public bool MoveNext()
            {
                if (_it >= _array._length)
                    return false;

                while (true)
                {
                    if (_bitmap != 0)
                    {
                        ulong t = _bitmap & (ulong)-(long)_bitmap;
                        _count = BitOperations.TrailingZeroCount(_bitmap);
                        _bitmap ^= t;
                        _current = _it * 64 + _count;
                        return true;
                    }

                    _it++;
                    if (_it >= _array._length)
                        break;

                    _bitmap = *(_array._bits + _it);
                }

                return false;
            }

            public void Reset()
            {
                _it = _from / 64;
                _bitmap = 0;
                _count = 0;
                _current = -1L;
                _bitmap = *(_array._bits + _it);
                _bitmap &= ulong.MaxValue << (_from % 64);
            }

            private long _current = -1L;
            public long Current => _current;

            object IEnumerator.Current
            {
                get => Current;
            }

            public void Dispose()
            {
                // nothing to dispose
            }
        }

        public unsafe IEnumerable<int> Iterate(int from)
        {
            // https://lemire.me/blog/2018/02/21/iterating-over-set-bits-quickly/
            int i = from / 64;
            if (i >= _length)
                yield break;

            ulong bitmap;
            unsafe
            {
                bitmap = *(_bits + i);
                bitmap &= ulong.MaxValue << (from % 64);
            }

            while (true)
            {
                while (bitmap != 0)
                {
                    ulong t = bitmap & (ulong)-(long)bitmap;
                    int count = BitOperations.TrailingZeroCount(bitmap);
                    int setBitPos = i * 64 + count;
                    yield return setBitPos;
                    bitmap ^= t;
                }

                i++;
                if (i >= _length)
                    break;
                unsafe
                {
                    bitmap = *(_bits + i);
                }
            }
        }

        public void Dispose()
        {
#if DEBUG
            IsValid = false;
#endif
            _memoryScope?.Dispose();
            _bits = null;
            _memoryScope = null;
        }

        public int CalculateCount()
        {
            int count = 0;
            foreach (var ul in new Span<ulong>(_bits, _length))
                count += BitOperations.PopCount(ul);

            return count;
        }

        public void Invert()
        {
            var idX = 0;
            if (AdvInstructionSet.IsAcceleratedVector128)
            {
                var N = Vector512<ulong>.Count;
                for (; idX + N <= _length; idX += N)
                {
                    // load and invert
                    var src = ~Vector512.Load(_bits + idX);
                    src.Store(_bits + idX);
                }
            }
            
            for (; idX < _length; idX++)
            {
                _bits[idX] = ~_bits[idX];
            }
        }
    }

    public int CalculateCount()
    {
        long count = 0;
        if (_bitArrays.Length == 1)
            return _bitArrays[0].CalculateCount();

        foreach (var bitArray in _bitArrays)
            count += bitArray.CalculateCount();
        return (int)count;
    }
}
