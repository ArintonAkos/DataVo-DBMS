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
| **DataVo**        | **100000** | **Memory**  |  **35.00 ms** | **10.297 ms** |  **6.811 ms** |  **31.05 ms** |  **1.03** |    **0.25** |  **6000.0000** | **3000.0000** | **1000.0000** |  **55.08 MB** |        **1.00** |
| Sqlite        | 100000 | Memory  |  82.18 ms | 56.210 ms | 37.179 ms |  62.33 ms |  2.42 |    1.12 | 11000.0000 |         - |         - |  93.84 MB |        1.70 |
| DuckDb        | 100000 | Memory  |  38.54 ms |  7.399 ms |  4.894 ms |  34.93 ms |  1.13 |    0.23 |  2000.0000 |         - |         - |  19.08 MB |        0.35 |
| DuckDb1Thread | 100000 | Memory  |  28.70 ms |  8.404 ms |  5.559 ms |  31.84 ms |  0.84 |    0.21 |  2000.0000 |         - |         - |  19.08 MB |        0.35 |
|               |        |         |           |           |           |           |       |         |            |           |           |           |             |
| **DataVo**        | **100000** | **File**    | **133.39 ms** | **53.578 ms** | **35.439 ms** | **133.76 ms** |  **1.09** |    **0.47** |  **6000.0000** | **3000.0000** | **1000.0000** |  **61.61 MB** |        **1.00** |
| Sqlite        | 100000 | File    |  76.81 ms | 40.000 ms | 26.457 ms |  67.74 ms |  0.63 |    0.30 | 11000.0000 |         - |         - |  93.84 MB |        1.52 |
| DuckDb        | 100000 | File    |  28.99 ms |  8.774 ms |  5.804 ms |  24.77 ms |  0.24 |    0.09 |  2000.0000 |         - |         - |  19.08 MB |        0.31 |
| DuckDb1Thread | 100000 | File    |  31.07 ms |  8.968 ms |  5.932 ms |  34.21 ms |  0.25 |    0.10 |  2000.0000 |         - |         - |  19.08 MB |        0.31 |
