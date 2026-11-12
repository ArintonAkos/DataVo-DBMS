```

BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04.1 LTS (Resolute Raccoon)
AMD EPYC 7763 3.24GHz, 1 CPU, 4 logical and 2 physical cores
.NET SDK 10.0.404
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  Job-CVTXAR : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3

InvocationCount=1  IterationCount=10  RunStrategy=Monitoring  
UnrollFactor=1  WarmupCount=2  

```
| Method        | Rows   | Storage | Mean      | Error     | StdDev    | Ratio | RatioSD | Gen0      | Gen1      | Allocated | Alloc Ratio |
|-------------- |------- |-------- |----------:|----------:|----------:|------:|--------:|----------:|----------:|----------:|------------:|
| **DataVo**        | **100000** | **Memory**  |  **62.26 ms** | **21.174 ms** | **14.005 ms** |  **1.04** |    **0.29** | **2000.0000** | **1000.0000** |  **55.07 MB** |        **1.00** |
| Sqlite        | 100000 | Memory  | 182.94 ms |  3.284 ms |  2.172 ms |  3.05 |    0.55 | 5000.0000 |         - |  93.84 MB |        1.70 |
| DuckDb        | 100000 | Memory  |  65.64 ms | 12.236 ms |  8.093 ms |  1.10 |    0.24 | 1000.0000 |         - |  19.08 MB |        0.35 |
| DuckDb1Thread | 100000 | Memory  |  64.46 ms | 13.717 ms |  9.073 ms |  1.08 |    0.24 | 1000.0000 |         - |  19.08 MB |        0.35 |
|               |        |         |           |           |           |       |         |           |           |           |             |
| **DataVo**        | **100000** | **File**    | **161.64 ms** | **47.356 ms** | **31.323 ms** |  **1.03** |    **0.27** | **2000.0000** | **1000.0000** |  **61.61 MB** |        **1.00** |
| Sqlite        | 100000 | File    | 192.12 ms |  4.546 ms |  3.007 ms |  1.23 |    0.23 | 5000.0000 |         - |  93.84 MB |        1.52 |
| DuckDb        | 100000 | File    |  73.13 ms | 14.092 ms |  9.321 ms |  0.47 |    0.10 | 1000.0000 |         - |  19.08 MB |        0.31 |
| DuckDb1Thread | 100000 | File    |  74.13 ms | 13.283 ms |  8.786 ms |  0.47 |    0.10 | 1000.0000 |         - |  19.08 MB |        0.31 |
