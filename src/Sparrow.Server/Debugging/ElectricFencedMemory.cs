using System;
using Sparrow.Json;
using Sparrow.Platform;
#if MEM_GUARD_STACK
using System.Threading;
using Sparrow.Collections;
#endif

namespace Sparrow.Server.Debugging
{
    internal sealed unsafe class ElectricFencedMemory : Sparrow.Debugging.DebugStuff.IElectricFencedMemory
    {
        public static ElectricFencedMemory Instance = new ElectricFencedMemory();

        private ElectricFencedMemory()
        {
        }

#if MEM_GUARD_STACK

        public System.Collections.Concurrent.ConcurrentDictionary<IntPtr, Tuple<int, string>> Allocs =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, Tuple<int, string>>();

        public System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>> DoubleMemoryReleases =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>>();

        public System.Collections.Concurrent.ConcurrentDictionary<JsonOperationContext, string> ContextAllocations =
            new System.Collections.Concurrent.ConcurrentDictionary<JsonOperationContext, string>();

        public int ContextCount;

#endif

        public void IncrementContext()
        {
#if MEM_GUARD_STACK
            Interlocked.Increment(ref ContextCount);
#endif
        }

        public void DecrementContext()
        {
#if MEM_GUARD_STACK
            Interlocked.Decrement(ref ContextCount);
#endif
        }

        public void RegisterContextAllocation(JsonOperationContext context, string stackTrace)
        {
#if MEM_GUARD_STACK
            ContextAllocations.TryAdd(context, stackTrace);
#endif
        }

        public void UnregisterContextAllocation(JsonOperationContext context)
        {
#if MEM_GUARD_STACK
            string _;
            ContextAllocations.TryRemove(context, out _);
#endif
        }

        public byte* Allocate(int size)
        {
            var memory =
            PlatformDetails.RunningOnPosix
                ? PosixElectricFencedMemory.Allocate(size)
                : Win32ElectricFencedMemory.Allocate(size);
#if MEM_GUARD_STACK
            Allocs.TryAdd((IntPtr)memory, Tuple.Create(size, Environment.StackTrace));
#endif
            return memory;
        }

        public void Free(byte* p)
        {
#if MEM_GUARD_STACK
            Tuple<int, string> _;
            if (Allocs.TryRemove((IntPtr)p, out _) == false)
            {
                var allocationsList = DoubleMemoryReleases.GetOrAdd((IntPtr)p, x => new ConcurrentSet<string>());
                allocationsList.Add(Environment.StackTrace);
            }
#endif

            if (PlatformDetails.RunningOnPosix)
                PosixElectricFencedMemory.Free(p);
            else
                Win32ElectricFencedMemory.Free(p);
        }
    }
}
