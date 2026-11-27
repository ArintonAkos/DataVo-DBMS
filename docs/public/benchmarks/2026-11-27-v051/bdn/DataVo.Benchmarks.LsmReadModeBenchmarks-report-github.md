```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.3 (25D125) [Darwin 25.3.0]
Apple M4 Pro, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.103
  [Host]     : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a
  DefaultJob : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a


```
| Method        | Path             | Mean           | Error        | StdDev       | Gen0   | Allocated |
|-------------- |----------------- |---------------:|-------------:|-------------:|-------:|----------:|
| **Lookup**        | **AutoMmap**         |       **180.3 ns** |      **0.65 ns** |      **0.58 ns** |      **-** |         **-** |
| Scan          | AutoMmap         | 1,852,414.0 ns | 25,398.45 ns | 23,757.73 ns |      - |     216 B |
| EngineReadRow | AutoMmap         |       185.6 ns |      0.89 ns |      0.79 ns | 0.0076 |      64 B |
| **Lookup**        | **Positional**       |     **6,402.1 ns** |      **7.28 ns** |      **6.08 ns** |      **-** |         **-** |
| Scan          | Positional       | 4,661,094.9 ns |  8,775.99 ns |  7,779.69 ns |      - |     216 B |
| EngineReadRow | Positional       |     6,474.3 ns |     19.35 ns |     16.16 ns | 0.0076 |      64 B |
| **Lookup**        | **PositionalCached** |       **175.5 ns** |      **1.05 ns** |      **0.82 ns** |      **-** |         **-** |
| Scan          | PositionalCached | 1,827,385.8 ns | 18,095.17 ns | 16,926.23 ns |      - |     216 B |
| EngineReadRow | PositionalCached |       207.3 ns |      1.03 ns |      0.81 ns | 0.0076 |      64 B |
