// using System;
// using System.Runtime.InteropServices;
// using BenchmarkDotNet.Attributes;
//
// namespace Voron.Benchmark.Corax;
//
// public class ImplicitCast
// {
//     [Params(256, 1024, 1 << 13, 1 << 14, 1 << 15, 1 << 16)]
//     public int StructsToCast;
//
//     private B[] _explicitStructs;
//     private BB[] _implicitStructs;
//     
//     
//     [GlobalSetup]
//     public void GlobalSetup()
//     {
//         var random = new Random(124123);
//         _explicitStructs = new [StructsToCast]{0d, 0d, 0d};
//         default(B);
//
//     }
//
//     [StructLayout(LayoutKind.Explicit)]
//     private struct A
//     {
//         [FieldOffset(0)]
//         public B Str;
//         
//         [FieldOffset(0)]
//         public double First;
//         [FieldOffset(8)]
//         public double Second;
//         [FieldOffset(16)]
//         public double Third;
//     }
//
//     [StructLayout(LayoutKind.Explicit)]
//     private struct B
//     {
//         [FieldOffset(0)]
//         public double First;
//         [FieldOffset(8)]
//         public double Second;
//         [FieldOffset(16)]
//         public double Third;
//     }
//     
//     private struct AA
//     {
//         public double First;
//         public double Second;
//         public double Third;
//     }
//
//     private struct BB
//     {
//         public double First;
//         public double Second;
//         public double Third;
//     }
//
//     [Benchmark]
//     public double CastAsUnion()
//     {
//         
//     }
//
//     [Benchmark]
//     public double CreateSecondStruct()
//     {
//         
//     }
// }
