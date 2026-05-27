using System;
using System.Numerics;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class GrowableBufferTests(ITestOutputHelper output) : NoDisposalNoOutputNeeded(output)
{
    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineDataWithRandomSeed(4 * Sparrow.Global.Constants.Size.Megabyte)]
    [InlineDataWithRandomSeed(8 * Sparrow.Global.Constants.Size.Megabyte)]
    public void CanExtendAndNotLooseAnythingSingle(int size, int seed) => CanExtendAndNotLooseAnythingBase<float>(size, seed);

    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineDataWithRandomSeed(4 * Sparrow.Global.Constants.Size.Megabyte)]
    [InlineDataWithRandomSeed(8 * Sparrow.Global.Constants.Size.Megabyte)]
    public void CanExtendAndNotLooseAnything(int size, int seed) => CanExtendAndNotLooseAnythingBase<long>(size, seed);

    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineDataWithRandomSeed(16 * Sparrow.Global.Constants.Size.Megabyte)]
    [InlineDataWithRandomSeed(32 * Sparrow.Global.Constants.Size.Megabyte)]
    public void CanExtendAndNotLooseAnythingExtended(int size, int seed) => CanExtendAndNotLooseAnythingBase<long>(size, seed);

    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineDataWithRandomSeed(16 * Sparrow.Global.Constants.Size.Megabyte)]
    [InlineDataWithRandomSeed(32 * Sparrow.Global.Constants.Size.Megabyte)]
    public void CanExtendAndNotLooseAnythingExtendedSingle(int size, int seed) => CanExtendAndNotLooseAnythingBase<float>(size, seed);

    private static void CanExtendAndNotLooseAnythingBase<T>(int size, int seed) where T : unmanaged, INumber<T>
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var growableBuffer = new GrowableBuffer<T, Progressive<T>>();
        growableBuffer.Init(bsc, 16);
        var count = 0;
        var random = new Random(seed);
        var random2 = new Random(seed);

        while (Fill(growableBuffer.GetSpace()) is var read and > 0)
        {
            growableBuffer.AddUsage(read);
        }
        
        Assert.Equal(size, growableBuffer.Results.Length);
        var results = growableBuffer.Results;
        for (var i = 0; i < size; ++i)
        {
            Assert.Equal(typeof(T) == typeof(long) ? (T)(object)random2.NextInt64() : (T)(object)(random2.NextSingle() * random2.Next()), results[i]);
        }
        
        int Fill(Span<T> buffer)
        {
            var i = 0;
            for (i = 0; i < buffer.Length && count < size; count++, i++)
                buffer[i] = typeof(T) == typeof(long) ? (T)(object)random.NextInt64() : (T)(object)(random.NextSingle() * random.Next());

            return i;
        }
    }
}
