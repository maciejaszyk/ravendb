using Sparrow.Json;

namespace Sparrow.Debugging
{
    internal static class DebugStuff
    {
        public static IElectricFencedMemory ElectricFencedMemory;

        internal interface IElectricFencedMemory
        {
            void IncrementContext();

            void DecrementContext();

            void RegisterContextAllocation(JsonOperationContext context, string stackTrace);

            void UnregisterContextAllocation(JsonOperationContext context);

            unsafe byte* Allocate(int size);

            unsafe void Free(byte* p);
        }
    }
}
