```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26100.33828/24H2/2024Update/HudsonValley) (Hyper-V)
Intel Xeon Platinum 8370C CPU 2.80GHz (Max: 2.79GHz), 1 CPU, 4 logical and 2 physical cores
.NET SDK 11.0.100
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4
  Job-CVTXAR : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4

InvocationCount=1  IterationCount=10  RunStrategy=Monitoring  
UnrollFactor=1  WarmupCount=2  

```
| Method        | Rows   | Storage | Mean      | Error      | StdDev    | Ratio | RatioSD | Gen0      | Gen1      | Gen2      | Allocated | Alloc Ratio |
|-------------- |------- |-------- |----------:|-----------:|----------:|------:|--------:|----------:|----------:|----------:|----------:|------------:|
| **DataVo**        | **100000** | **Memory**  |  **94.22 ms** |  **51.537 ms** | **34.089 ms** |  **1.10** |    **0.49** | **2000.0000** | **1000.0000** | **1000.0000** |   **51.5 MB** |        **1.00** |
| Sqlite        | 100000 | Memory  | 174.65 ms |  11.642 ms |  7.701 ms |  2.03 |    0.54 | 3000.0000 |         - |         - |  93.84 MB |        1.82 |
| DuckDb        | 100000 | Memory  |  93.65 ms |  13.644 ms |  9.025 ms |  1.09 |    0.31 |         - |         - |         - |  19.08 MB |        0.37 |
| DuckDb1Thread | 100000 | Memory  |  89.56 ms |  15.445 ms | 10.216 ms |  1.04 |    0.30 |         - |         - |         - |  19.08 MB |        0.37 |
|               |        |         |           |            |           |       |         |           |           |           |           |             |
| **DataVo**        | **100000** | **File**    | **203.05 ms** | **114.171 ms** | **75.517 ms** |  **1.09** |    **0.47** | **2000.0000** | **1000.0000** | **1000.0000** |  **75.26 MB** |        **1.00** |
| Sqlite        | 100000 | File    | 181.48 ms |   3.014 ms |  1.993 ms |  0.97 |    0.23 | 3000.0000 |         - |         - |  93.84 MB |        1.25 |
| DuckDb        | 100000 | File    | 129.46 ms |  18.642 ms | 12.331 ms |  0.69 |    0.18 |         - |         - |         - |  19.08 MB |        0.25 |
| DuckDb1Thread | 100000 | File    | 132.09 ms |  23.903 ms | 15.810 ms |  0.71 |    0.19 |         - |         - |         - |  19.08 MB |        0.25 |
