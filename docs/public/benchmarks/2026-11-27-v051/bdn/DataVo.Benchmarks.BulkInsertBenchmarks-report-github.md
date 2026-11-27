```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.3 (25D125) [Darwin 25.3.0]
Apple M4 Pro, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.103
  [Host]     : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a
  Job-CVTXAR : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a

InvocationCount=1  IterationCount=10  RunStrategy=Monitoring  
UnrollFactor=1  WarmupCount=2  

```
| Method        | Rows   | Storage | Mean      | Error     | StdDev    | Median    | Ratio | RatioSD | Gen0       | Gen1      | Gen2      | Allocated | Alloc Ratio |
|-------------- |------- |-------- |----------:|----------:|----------:|----------:|------:|--------:|-----------:|----------:|----------:|----------:|------------:|
| **DataVo**        | **100000** | **Memory**  |  **38.56 ms** | **21.899 ms** | **14.485 ms** |  **30.54 ms** |  **1.10** |    **0.49** |  **5000.0000** | **3000.0000** | **1000.0000** |   **51.5 MB** |        **1.00** |
| Sqlite        | 100000 | Memory  |  62.71 ms |  0.435 ms |  0.288 ms |  62.81 ms |  1.78 |    0.45 | 11000.0000 |         - |         - |  93.84 MB |        1.82 |
| DuckDb        | 100000 | Memory  |  29.68 ms | 10.604 ms |  7.014 ms |  34.19 ms |  0.84 |    0.29 |  2000.0000 |         - |         - |  19.08 MB |        0.37 |
| DuckDb1Thread | 100000 | Memory  |  31.10 ms |  7.549 ms |  4.993 ms |  33.34 ms |  0.88 |    0.26 |  2000.0000 |         - |         - |  19.08 MB |        0.37 |
|               |        |         |           |           |           |           |       |         |            |           |           |           |             |
| **DataVo**        | **100000** | **File**    | **119.05 ms** | **67.232 ms** | **44.470 ms** | **126.10 ms** |  **1.15** |    **0.63** |  **6000.0000** | **3000.0000** | **1000.0000** |  **75.26 MB** |        **1.00** |
| Sqlite        | 100000 | File    |  77.01 ms | 43.034 ms | 28.464 ms |  68.13 ms |  0.75 |    0.40 | 11000.0000 |         - |         - |  93.84 MB |        1.25 |
| DuckDb        | 100000 | File    |  28.36 ms |  9.157 ms |  6.057 ms |  24.65 ms |  0.27 |    0.12 |  2000.0000 |         - |         - |  19.08 MB |        0.25 |
| DuckDb1Thread | 100000 | File    |  28.12 ms |  9.372 ms |  6.199 ms |  24.44 ms |  0.27 |    0.12 |  2000.0000 |         - |         - |  19.08 MB |        0.25 |
