using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow.Platform;

namespace Sparrow.Server.Utils;

public interface IBufferGrowth
{
    public int GetInitialSize(in long initialSize);
    public int GetNewSize(in int currentSizeInBytes);
    public bool GrowingThresholdExceed(in int count, in int sizeInBytes);
}

public readonly struct Constant<TNumber> : IBufferGrowth
    where TNumber : unmanaged, INumber<TNumber>
{
    public int GetInitialSize(in long initialSize)
    {
        return (int)initialSize * Unsafe.SizeOf<TNumber>();
    }

    public int GetNewSize(in int currentSizeInBytes) => currentSizeInBytes * 2;
    public bool GrowingThresholdExceed(in int count, in int sizeInBytes)
    {
        var amountOfLongs = (sizeInBytes / Unsafe.SizeOf<TNumber>());
        return (amountOfLongs - count) < amountOfLongs / 16;
    }
}

public readonly struct Progressive<TNumber> : IBufferGrowth
where TNumber : unmanaged, INumber<TNumber>
{
    public int GetNewSize(in int currentSizeInBytes)
    {
        // Slower growth on 32-bit platforms
        float platformScalar = PlatformDetails.Is32Bits ? 1.1f : 1.5f;
        
        var size = currentSizeInBytes > 16 * Sparrow.Global.Constants.Size.Megabyte
            ? (int)(currentSizeInBytes * platformScalar)
            : currentSizeInBytes * 2;

        // Represent array as N*sizeof(long)
        return size - (size % Unsafe.SizeOf<TNumber>());
    }

    public bool GrowingThresholdExceed(in int count, in int sizeInBytes)
    {
        // 1/16 left
        var amountOfLongs = (sizeInBytes / Unsafe.SizeOf<TNumber>());
        return (amountOfLongs - count) < amountOfLongs / 16;
    }

    public int GetInitialSize(in long initialSize)
    {
        Debug.Assert(initialSize < int.MaxValue, "initialSize < int.MaxValue");
        var size = 4 * Math.Min(Math.Max(Sparrow.Global.Constants.Size.Kilobyte, (int)initialSize), 16 * Sparrow.Global.Constants.Size.Kilobyte);
        if (size < initialSize)
        {
            Debug.Assert(size % sizeof(long) == 0, "size % sizeof(long) == 0");
            return (int)initialSize;
        }
        
        // Represent array as N*sizeof(long)
        return size - (size % Unsafe.SizeOf<TNumber>());
    }
}

public unsafe struct GrowableBuffer<TNumber, TGrowth> : IDisposable
    where TGrowth : IBufferGrowth
    where TNumber : unmanaged, INumber<TNumber>
{
    private readonly TGrowth _growthCalculator = default;
    private ByteStringContext _context;
    private ByteString _buffer;
    private int _count;
    public int Count => _count;
    public bool IsInitialized;
    
    public int Capacity => IsInitialized ? _buffer.Length / sizeof(TNumber) : 0;

    public Span<TNumber> GetSpace()
    {
        if (_growthCalculator.GrowingThresholdExceed(_count, _buffer.Length))
            Grow();

        return _buffer.ToSpan<TNumber>().Slice(_count);
    }

    public Span<TNumber> Results => _buffer.ToSpan<TNumber>().Slice(0, _count);
    
    public bool HasEmptySpace => _buffer.Length == (_count * sizeof(TNumber));

    public GrowableBuffer()
    {
    }

    public void AddUsage(in int count) => _count += count;

    public void Truncate(in int newCount) => _count = newCount;
    
    public void Init(ByteStringContext context, in long initialSize)
    {
        _context = context;
        _context.Allocate(_growthCalculator.GetInitialSize(initialSize), out _buffer);
        IsInitialized = true;
    }

    private void Grow()
    {
        var oldBuffer = _buffer;
        var oldCount = _count;

        var newSize = _growthCalculator.GetNewSize(oldBuffer.Length);
        _context.Allocate(newSize, out ByteString newBuffer);

        if (RangesOverlap(oldBuffer.Ptr, oldBuffer.Length, newBuffer.Ptr, newBuffer.Length))
            throw new InvalidOperationException(
                $"GrowableBuffer allocator returned overlapping ranges. " +
                $"old={(long)oldBuffer.Ptr:X} oldLen={oldBuffer.Length}, " +
                $"new={(long)newBuffer.Ptr:X} newLen={newBuffer.Length}, count={oldCount}");

        var oldSpan = new Span<TNumber>(oldBuffer.Ptr, oldCount);
        var newSpan = new Span<TNumber>(newBuffer.Ptr, oldCount);
        var hashBytes = (long)oldCount * Unsafe.SizeOf<TNumber>();

        var newHashAtAllocate     = Fnv1aHash(newBuffer.Ptr, hashBytes);
        var oldHashBefore         = Fnv1aHash(oldBuffer.Ptr, hashBytes);
        var newHashJustBeforeCopy = Fnv1aHash(newBuffer.Ptr, hashBytes);
        oldSpan.CopyTo(newSpan);
        var oldHashAfter = Fnv1aHash(oldBuffer.Ptr, hashBytes);
        var newHashAfter = Fnv1aHash(newBuffer.Ptr, hashBytes);

        if (newHashAtAllocate != newHashJustBeforeCopy)
            throw new InvalidOperationException(
                $"GrowableBuffer DESTINATION mutated BEFORE copy.{Environment.NewLine}" +
                $"  old = 0x{(long)oldBuffer.Ptr:X}{Environment.NewLine}" +
                $"  new = 0x{(long)newBuffer.Ptr:X}{Environment.NewLine}" +
                $"  count = {oldCount} {typeof(TNumber).Name} ({hashBytes} bytes){Environment.NewLine}" +
                $"  newHashAtAllocate     = 0x{newHashAtAllocate:X16}{Environment.NewLine}" +
                $"  newHashJustBeforeCopy = 0x{newHashJustBeforeCopy:X16}");

        if (oldHashBefore != oldHashAfter)
            throw new InvalidOperationException(
                $"GrowableBuffer SOURCE mutated during copy.{Environment.NewLine}" +
                $"  old = 0x{(long)oldBuffer.Ptr:X}{Environment.NewLine}" +
                $"  new = 0x{(long)newBuffer.Ptr:X}{Environment.NewLine}" +
                $"  count = {oldCount} {typeof(TNumber).Name} ({hashBytes} bytes){Environment.NewLine}" +
                $"  oldHashBefore = 0x{oldHashBefore:X16}{Environment.NewLine}" +
                $"  oldHashAfter  = 0x{oldHashAfter:X16}");

        if (oldHashAfter != newHashAfter)
            throw new InvalidOperationException(
                $"GrowableBuffer DESTINATION mutated during copy.{Environment.NewLine}" +
                $"  old = 0x{(long)oldBuffer.Ptr:X}{Environment.NewLine}" +
                $"  new = 0x{(long)newBuffer.Ptr:X}{Environment.NewLine}" +
                $"  count = {oldCount} {typeof(TNumber).Name} ({hashBytes} bytes){Environment.NewLine}" +
                $"  oldHashBefore = 0x{oldHashBefore:X16}{Environment.NewLine}" +
                $"  oldHashAfter  = 0x{oldHashAfter:X16}   (source unchanged){Environment.NewLine}" +
                $"  newHashAfter  = 0x{newHashAfter:X16}   (destination differs from source)");

        _context.Release(ref _buffer);
        _buffer = newBuffer;
    }

    private static bool RangesOverlap(byte* a, int aBytes, byte* b, int bBytes)
    {
        nuint aa = (nuint)a, bb = (nuint)b;
        return aa < bb + (nuint)bBytes && bb < aa + (nuint)aBytes;
    }

    private static ulong Fnv1aHash(byte* ptr, long bytes)
    {
        const ulong off = 14695981039346656037UL, prime = 1099511628211UL;
        ulong h = off;
        long i = 0, stop = bytes - 7;
        for (; i < stop; i += 8) { h ^= *(ulong*)(ptr + i); h *= prime; }
        for (; i < bytes;  i++) { h ^= ptr[i];              h *= prime; }
        return h;
    }
    
    public void Dispose()
    {
        _context.Release(ref _buffer);
        _buffer = default;
    }
}
