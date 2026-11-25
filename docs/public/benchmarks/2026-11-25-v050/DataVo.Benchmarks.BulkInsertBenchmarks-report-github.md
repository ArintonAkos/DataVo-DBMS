```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.3 (25D125) [Darwin 25.3.0]
Apple M4 Pro, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.103
  [Host]     : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a
  Job-CVTXAR : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a

InvocationCount=1  IterationCount=10  RunStrategy=Monitoring  
UnrollFactor=1  WarmupCount=2  

```
| Method        | Rows   | Storage | Mean      | Error     | StdDev    | Median   | Ratio | RatioSD | Gen0       | Gen1      | Gen2      | Allocated | Alloc Ratio |
|-------------- |------- |-------- |----------:|----------:|----------:|---------:|------:|--------:|-----------:|----------:|----------:|----------:|------------:|
| **DataVo**        | **100000** | **Memory**  |  **38.82 ms** | **23.442 ms** | **15.505 ms** | **28.79 ms** |  **1.12** |    **0.56** |  **5000.0000** | **3000.0000** | **1000.0000** |   **51.5 MB** |        **1.00** |
| Sqlite        | 100000 | Memory  |  66.73 ms | 15.291 ms | 10.114 ms | 60.82 ms |  1.93 |    0.64 | 11000.0000 |         - |         - |  93.84 MB |        1.82 |
| DuckDb        | 100000 | Memory  |  38.15 ms |  9.152 ms |  6.053 ms | 33.93 ms |  1.11 |    0.37 |  2000.0000 |         - |         - |  19.08 MB |        0.37 |
| DuckDb1Thread | 100000 | Memory  |  28.56 ms |  6.909 ms |  4.570 ms | 31.67 ms |  0.83 |    0.28 |  2000.0000 |         - |         - |  19.08 MB |        0.37 |
|               |        |         |           |           |           |          |       |         |            |           |           |           |             |
| **DataVo**        | **100000** | **File**    | **109.92 ms** | **77.289 ms** | **51.122 ms** | **92.53 ms** |  **1.21** |    **0.75** |  **6000.0000** | **3000.0000** | **1000.0000** |  **75.26 MB** |        **1.00** |
| Sqlite        | 100000 | File    |  65.91 ms |  0.900 ms |  0.596 ms | 66.09 ms |  0.72 |    0.29 | 11000.0000 |         - |         - |  93.84 MB |        1.25 |
| DuckDb        | 100000 | File    |  27.98 ms |  7.931 ms |  5.246 ms | 24.23 ms |  0.31 |    0.14 |  2000.0000 |         - |         - |  19.08 MB |        0.25 |
| DuckDb1Thread | 100000 | File    |  28.14 ms |  8.133 ms |  5.379 ms | 24.18 ms |  0.31 |    0.14 |  2000.0000 |         - |         - |  19.08 MB |        0.25 |
