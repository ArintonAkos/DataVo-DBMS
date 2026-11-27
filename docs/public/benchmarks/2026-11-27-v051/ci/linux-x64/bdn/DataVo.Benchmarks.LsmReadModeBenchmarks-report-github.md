```

BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04.1 LTS (Resolute Raccoon)
AMD EPYC 9V45 4.17GHz, 1 CPU, 4 logical and 2 physical cores
.NET SDK 11.0.100
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4


```
| Method        | Path             | Mean           | Error        | StdDev       | Gen0   | Allocated |
|-------------- |----------------- |---------------:|-------------:|-------------:|-------:|----------:|
| **Lookup**        | **AutoMmap**         |       **157.3 ns** |      **1.36 ns** |      **1.27 ns** |      **-** |         **-** |
| Scan          | AutoMmap         | 1,257,072.4 ns | 24,541.29 ns | 28,261.79 ns |      - |     216 B |
| EngineReadRow | AutoMmap         |       198.9 ns |      3.26 ns |      3.05 ns | 0.0038 |      64 B |
| **Lookup**        | **Positional**       |     **1,782.0 ns** |     **27.60 ns** |     **25.82 ns** |      **-** |         **-** |
| Scan          | Positional       | 4,301,314.4 ns | 59,565.66 ns | 52,803.41 ns |      - |     216 B |
| EngineReadRow | Positional       |     1,840.3 ns |     21.28 ns |     16.61 ns | 0.0038 |      64 B |
| **Lookup**        | **PositionalCached** |       **203.2 ns** |      **4.04 ns** |      **4.33 ns** |      **-** |         **-** |
| Scan          | PositionalCached | 1,412,008.9 ns | 27,427.02 ns | 28,165.53 ns |      - |     216 B |
| EngineReadRow | PositionalCached |       251.8 ns |      2.46 ns |      2.05 ns | 0.0038 |      64 B |
