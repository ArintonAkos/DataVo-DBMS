```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26100.33674/24H2/2024Update/HudsonValley) (Hyper-V)
AMD EPYC 9V74 2.60GHz, 1 CPU, 4 logical and 2 physical cores
.NET SDK 10.0.404
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4
  Job-CVTXAR : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4

InvocationCount=1  IterationCount=10  RunStrategy=Monitoring  
UnrollFactor=1  WarmupCount=2  

```
| Method        | Rows   | Storage | Mean      | Error     | StdDev    | Ratio | RatioSD | Gen0      | Gen1      | Allocated | Alloc Ratio |
|-------------- |------- |-------- |----------:|----------:|----------:|------:|--------:|----------:|----------:|----------:|------------:|
| **DataVo**        | **100000** | **Memory**  |  **57.87 ms** | **20.747 ms** | **13.723 ms** |  **1.05** |    **0.33** | **2000.0000** | **1000.0000** |  **55.07 MB** |        **1.00** |
| Sqlite        | 100000 | Memory  | 130.34 ms |  2.310 ms |  1.528 ms |  2.36 |    0.50 | 5000.0000 |         - |  93.84 MB |        1.70 |
| DuckDb        | 100000 | Memory  |  73.36 ms |  9.999 ms |  6.614 ms |  1.33 |    0.31 | 1000.0000 |         - |  19.08 MB |        0.35 |
| DuckDb1Thread | 100000 | Memory  |  71.75 ms | 10.640 ms |  7.038 ms |  1.30 |    0.30 | 1000.0000 |         - |  19.08 MB |        0.35 |
|               |        |         |           |           |           |       |         |           |           |           |             |
| **DataVo**        | **100000** | **File**    | **132.89 ms** | **43.098 ms** | **28.507 ms** |  **1.04** |    **0.28** | **2000.0000** | **1000.0000** |  **61.61 MB** |        **1.00** |
| Sqlite        | 100000 | File    | 137.64 ms |  3.426 ms |  2.266 ms |  1.07 |    0.19 | 5000.0000 |         - |  93.84 MB |        1.52 |
| DuckDb        | 100000 | File    | 119.75 ms | 26.567 ms | 17.572 ms |  0.93 |    0.21 | 1000.0000 |         - |  19.08 MB |        0.31 |
| DuckDb1Thread | 100000 | File    | 113.28 ms |  9.983 ms |  6.603 ms |  0.88 |    0.17 | 1000.0000 |         - |  19.08 MB |        0.31 |
