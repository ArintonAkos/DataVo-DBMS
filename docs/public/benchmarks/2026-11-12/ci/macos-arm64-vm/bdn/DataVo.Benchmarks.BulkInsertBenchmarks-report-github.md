```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.7.2 (25G332) [Darwin 25.6.0]
Apple M1 (Virtual), 1 CPU, 3 logical and 3 physical cores
.NET SDK 10.0.404
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), Arm64 RyuJIT armv8.0-a
  Job-CVTXAR : .NET 10.0.15 (10.0.15, 10.0.1526.53008), Arm64 RyuJIT armv8.0-a

InvocationCount=1  IterationCount=10  RunStrategy=Monitoring  
UnrollFactor=1  WarmupCount=2  

```
| Method        | Rows   | Storage | Mean      | Error     | StdDev    | Median    | Ratio | RatioSD | Gen0       | Gen1      | Gen2      | Allocated | Alloc Ratio |
|-------------- |------- |-------- |----------:|----------:|----------:|----------:|------:|--------:|-----------:|----------:|----------:|----------:|------------:|
| **DataVo**        | **100000** | **Memory**  |  **75.20 ms** |  **26.18 ms** | **17.319 ms** |  **70.25 ms** |  **1.05** |    **0.32** |  **8000.0000** | **3000.0000** | **1000.0000** |  **55.08 MB** |        **1.00** |
| Sqlite        | 100000 | Memory  | 125.18 ms |  17.15 ms | 11.346 ms | 120.49 ms |  1.74 |    0.39 | 15000.0000 |         - |         - |  93.84 MB |        1.70 |
| DuckDb        | 100000 | Memory  |  42.01 ms |  14.68 ms |  9.707 ms |  37.86 ms |  0.58 |    0.18 |  3000.0000 |         - |         - |  19.08 MB |        0.35 |
| DuckDb1Thread | 100000 | Memory  |  44.14 ms |  17.20 ms | 11.376 ms |  36.44 ms |  0.61 |    0.20 |  3000.0000 |         - |         - |  19.08 MB |        0.35 |
|               |        |         |           |           |           |           |       |         |            |           |           |           |             |
| **DataVo**        | **100000** | **File**    | **156.35 ms** | **115.41 ms** | **76.337 ms** | **119.87 ms** |  **1.19** |    **0.72** |  **8000.0000** | **3000.0000** | **1000.0000** |  **61.61 MB** |        **1.00** |
| Sqlite        | 100000 | File    | 149.41 ms |  12.28 ms |  8.121 ms | 149.00 ms |  1.13 |    0.41 | 15000.0000 |         - |         - |  93.84 MB |        1.52 |
| DuckDb        | 100000 | File    |  60.43 ms |  19.23 ms | 12.722 ms |  59.78 ms |  0.46 |    0.19 |  3000.0000 |         - |         - |  19.08 MB |        0.31 |
| DuckDb1Thread | 100000 | File    |  59.22 ms |  14.40 ms |  9.525 ms |  59.89 ms |  0.45 |    0.18 |  3000.0000 |         - |         - |  19.08 MB |        0.31 |
