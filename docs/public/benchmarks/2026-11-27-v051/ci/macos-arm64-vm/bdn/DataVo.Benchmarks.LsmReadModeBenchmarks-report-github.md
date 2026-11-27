```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.7.2 (25G332) [Darwin 25.6.0]
Apple M1 (Virtual), 1 CPU, 3 logical and 3 physical cores
.NET SDK 11.0.100
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), Arm64 RyuJIT armv8.0-a
  DefaultJob : .NET 10.0.15 (10.0.15, 10.0.1526.53008), Arm64 RyuJIT armv8.0-a


```
| Method        | Path             | Mean           | Error         | StdDev        | Median         | Gen0   | Allocated |
|-------------- |----------------- |---------------:|--------------:|--------------:|---------------:|-------:|----------:|
| **Lookup**        | **AutoMmap**         |       **387.9 ns** |      **19.84 ns** |      **58.51 ns** |       **386.9 ns** |      **-** |         **-** |
| Scan          | AutoMmap         | 2,286,899.3 ns |  53,670.53 ns | 156,559.48 ns | 2,242,267.0 ns |      - |     216 B |
| EngineReadRow | AutoMmap         |       430.7 ns |      14.88 ns |      43.65 ns |       416.9 ns | 0.0100 |      64 B |
| **Lookup**        | **Positional**       |     **7,216.9 ns** |     **237.14 ns** |     **699.20 ns** |     **7,203.6 ns** |      **-** |         **-** |
| Scan          | Positional       | 6,131,352.7 ns | 131,828.80 ns | 386,631.09 ns | 6,137,252.6 ns |      - |     216 B |
| EngineReadRow | Positional       |     8,295.0 ns |     163.72 ns |     395.40 ns |     8,324.1 ns |      - |      64 B |
| **Lookup**        | **PositionalCached** |       **423.2 ns** |      **20.22 ns** |      **59.29 ns** |       **415.4 ns** |      **-** |         **-** |
| Scan          | PositionalCached | 2,117,643.0 ns |  30,440.39 ns |  23,765.86 ns | 2,118,129.0 ns |      - |     216 B |
| EngineReadRow | PositionalCached |       388.1 ns |       8.69 ns |      24.94 ns |       387.1 ns | 0.0100 |      64 B |
