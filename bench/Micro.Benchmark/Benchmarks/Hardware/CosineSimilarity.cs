using System;
using System.Linq;
using System.Numerics.Tensors;
using BenchmarkDotNet.Attributes;

namespace Micro.Benchmark.Benchmarks.Hardware;

[MemoryDiagnoser]
public class CosineSimilarity
{
    private float[] _v1;
    private float[] _v2;
    private float _v1Norm;
    private float _v2Norm;
    
    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(124123);
        _v1 = Enumerable.Range(0, 1536).Select(x => random.NextSingle()).ToArray();
        _v2 = Enumerable.Range(0, 1536).Select(x => random.NextSingle()).ToArray();
        _v1Norm = TensorPrimitives.Norm(_v1);
        _v2Norm = TensorPrimitives.Norm(_v2);
    }
    
    [Benchmark(Baseline = true)]
    public float CosineSimilarityNormal()
    {
        float res = -1;
        for (int i = 0; i < 10_000_000; i++)
         res = TensorPrimitives.CosineSimilarity(_v1, _v2);
        return res;
    }
    
    [Benchmark]
    public float CosineSimilarityWithStoredNorm()
    {
        float res = -1;
        for (int i = 0; i < 10_000_000; i++)
            res = TensorPrimitives.Dot(_v1, _v2) / (_v1Norm * _v2Norm);
        return res;
    }
}
