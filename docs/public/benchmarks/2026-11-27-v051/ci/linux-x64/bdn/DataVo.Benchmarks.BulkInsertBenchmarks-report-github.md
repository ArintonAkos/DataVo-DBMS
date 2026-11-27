```

BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04.1 LTS (Resolute Raccoon)
AMD EPYC 9V45 4.17GHz, 1 CPU, 4 logical and 2 physical cores
.NET SDK 11.0.100
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4
  Job-CVTXAR : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4

InvocationCount=1  IterationCount=10  RunStrategy=Monitoring  
UnrollFactor=1  WarmupCount=2  

```
| Method        | Rows   | Storage | Mean      | Error     | StdDev    | Ratio | RatioSD | Gen0      | Gen1      | Gen2      | Allocated | Alloc Ratio |
|-------------- |------- |-------- |----------:|----------:|----------:|------:|--------:|----------:|----------:|----------:|----------:|------------:|
| **DataVo**        | **100000** | **Memory**  |  **49.60 ms** | **19.866 ms** | **13.140 ms** |  **1.05** |    **0.35** | **3000.0000** | **2000.0000** | **1000.0000** |   **51.5 MB** |        **1.00** |
| Sqlite        | 100000 | Memory  | 126.13 ms |  3.710 ms |  2.454 ms |  2.68 |    0.56 | 5000.0000 |         - |         - |  93.84 MB |        1.82 |
| DuckDb        | 100000 | Memory  |  46.38 ms | 21.865 ms | 14.462 ms |  0.99 |    0.36 | 1000.0000 |         - |         - |  19.08 MB |        0.37 |
| DuckDb1Thread | 100000 | Memory  |  42.89 ms |  8.910 ms |  5.894 ms |  0.91 |    0.23 | 1000.0000 |         - |         - |  19.08 MB |        0.37 |
|               |        |         |           |           |           |       |         |           |           |           |           |             |
| **DataVo**        | **100000** | **File**    | **169.69 ms** | **85.423 ms** | **56.502 ms** |  **1.15** |    **0.62** | **3000.0000** | **2000.0000** | **1000.0000** |  **75.26 MB** |        **1.00** |
| Sqlite        | 100000 | File    | 130.40 ms |  8.181 ms |  5.411 ms |  0.88 |    0.37 | 5000.0000 |         - |         - |  93.84 MB |        1.25 |
| DuckDb        | 100000 | File    |  57.86 ms |  9.724 ms |  6.432 ms |  0.39 |    0.17 | 1000.0000 |         - |         - |  19.08 MB |        0.25 |
| DuckDb1Thread | 100000 | File    |  52.47 ms | 12.182 ms |  8.058 ms |  0.35 |    0.16 | 1000.0000 |         - |         - |  19.08 MB |        0.25 |
