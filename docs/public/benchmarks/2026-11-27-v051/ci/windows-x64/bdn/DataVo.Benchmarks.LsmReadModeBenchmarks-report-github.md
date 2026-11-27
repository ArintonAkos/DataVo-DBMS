```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26100.33828/24H2/2024Update/HudsonValley) (Hyper-V)
Intel Xeon Platinum 8370C CPU 2.80GHz (Max: 2.79GHz), 1 CPU, 4 logical and 2 physical cores
.NET SDK 11.0.100
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4


```
| Method        | Path             | Mean            | Error         | StdDev        | Gen0   | Allocated |
|-------------- |----------------- |----------------:|--------------:|--------------:|-------:|----------:|
| **Lookup**        | **AutoMmap**         |        **289.9 ns** |       **5.27 ns** |       **4.40 ns** |      **-** |         **-** |
| Scan          | AutoMmap         |  2,490,888.5 ns |  19,571.35 ns |  18,307.05 ns |      - |     216 B |
| EngineReadRow | AutoMmap         |        377.5 ns |       7.24 ns |       8.89 ns | 0.0024 |      64 B |
| **Lookup**        | **Positional**       |      **5,714.4 ns** |      **57.98 ns** |      **54.23 ns** |      **-** |         **-** |
| Scan          | Positional       | 10,160,543.0 ns | 162,809.86 ns | 135,953.61 ns |      - |     216 B |
| EngineReadRow | Positional       |      5,901.4 ns |      71.36 ns |      66.75 ns |      - |      64 B |
| **Lookup**        | **PositionalCached** |        **377.9 ns** |       **7.39 ns** |       **9.61 ns** |      **-** |         **-** |
| Scan          | PositionalCached |  2,752,439.0 ns |  36,408.77 ns |  32,275.43 ns |      - |     216 B |
| EngineReadRow | PositionalCached |        480.9 ns |       9.33 ns |      11.81 ns | 0.0019 |      64 B |
