using System;
using System.Runtime.Intrinsics.X86;
using Sparrow.Binary;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public unsafe class PtrBitVectorTests : NoDisposalNeeded
    {
        public PtrBitVectorTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SetIndex()
        {
            var original = new byte[8] {0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00};
            fixed (byte* storage = original)
            {
                var ptr = new PtrBitVector(storage, 64);
                int idx = ptr.FindLeadingOne();
                Assert.Equal(false, ptr[idx - 1]);
                Assert.Equal(true, ptr[idx]);
                Assert.Equal(false, ptr[idx + 1]);

                ptr.Set(idx, false);
                ptr.Set(idx + 1, true);

                Assert.Equal(false, ptr[idx - 1]);
                Assert.Equal(false, ptr[idx]);
                Assert.Equal(true, ptr[idx + 1]);
            }

            ;
        }


        [Theory]
        [InlineData(3)]
        [InlineData(256)]
        [InlineData(1024)]
        [InlineData(1025)]
        [InlineData(3333)]
        public void RandomlySetBitsInBitVector(bool forceNonAccelerated, int size)
        {
            if (forceNonAccelerated && (Avx2.IsSupported || Sse2.IsSupported) == false)
                return;

            var buffer = new byte[size];
            buffer.AsSpan().Clear();

            var bitVector = new BitVector(buffer);

            Assert.Equal(-1, bitVector.IndexOfFirstSetBit());

            var random = new Random(12532423);
            var boolBuffer = new bool[size];


            for (int i = 0; i < size; ++i)
            {
                var id = random.Next(0, size);
                boolBuffer[id] = boolBuffer[id] == false;
                bitVector[id] = bitVector[id] == false;
                AssertBitVectorAndBufferCopy(boolBuffer, in bitVector);
            }
        }

        [Theory]
        [InlineData(3)]
        [InlineData(256)]
        [InlineData(1024)]
        [InlineData(1025)]
        [InlineData(3333)]
        public void SetWholeOneByOne(int size)
        {
            var bitBuffer = new byte[size];
            var boolBuffer = new bool[size];
            bitBuffer.AsSpan().Clear();
            boolBuffer.AsSpan().Fill(false);

            var bitVector = new BitVector(bitBuffer);
            Assert.Equal(-1, bitVector.IndexOfFirstSetBit());
            for (int i = size - 1; i >=0; ++i) 
            {
                bitVector[i] = true;
                boolBuffer[i] = true;
                var booleanArrayFirstSetBit = IndexOfFirstSetBit();
                if (Avx2.IsSupported)
                    Assert.Equal(booleanArrayFirstSetBit, bitVector.IndexOfFirstSetBitAvx2());
            }

            
            AssertBitVectorAndBufferCopy(boolBuffer, in bitVector);
            
            int IndexOfFirstSetBit()
            {
                for (int i = 0; i < size; ++i)
                    if (boolBuffer[i])
                        return 1;
                return -1;
            }
        }

        private static void AssertBitVectorAndBufferCopy(Span<bool> buffer, in BitVector bitVector)
        {
            for (int i = 0; i < buffer.Length; ++i)
                Assert.Equal(buffer[i], bitVector[i]);
        }
    }
}
