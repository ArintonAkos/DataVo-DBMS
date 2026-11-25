```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.3 (25D125) [Darwin 25.3.0]
Apple M4 Pro, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.103
  [Host]     : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a
  DefaultJob : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a


```
| Method        | Path             | Mean           | Error        | StdDev       | Gen0   | Allocated |
|-------------- |----------------- |---------------:|-------------:|-------------:|-------:|----------:|
| **Lookup**        | **AutoMmap**         |       **178.1 ns** |      **0.57 ns** |      **0.51 ns** |      **-** |         **-** |
| Scan          | AutoMmap         | 1,799,937.3 ns | 32,424.87 ns | 30,330.25 ns |      - |     216 B |
| EngineReadRow | AutoMmap         |       207.1 ns |      0.54 ns |      0.45 ns | 0.0076 |      64 B |
| **Lookup**        | **Positional**       |     **6,496.6 ns** |     **14.58 ns** |     **12.92 ns** |      **-** |         **-** |
| Scan          | Positional       | 4,756,090.1 ns |  9,929.44 ns |  8,802.19 ns |      - |     216 B |
| EngineReadRow | Positional       |     6,535.4 ns |     19.93 ns |     18.64 ns | 0.0076 |      64 B |
| **Lookup**        | **PositionalCached** |       **204.2 ns** |      **1.20 ns** |      **1.06 ns** |      **-** |         **-** |
| Scan          | PositionalCached | 1,842,624.7 ns | 28,128.56 ns | 26,311.47 ns |      - |     216 B |
| EngineReadRow | PositionalCached |       220.3 ns |      0.74 ns |      0.66 ns | 0.0076 |      64 B |
