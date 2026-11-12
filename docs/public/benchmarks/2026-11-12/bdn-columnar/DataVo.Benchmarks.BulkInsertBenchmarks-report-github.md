```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.3 (25D125) [Darwin 25.3.0]
Apple M4 Pro, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.103
  [Host]     : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a
  Job-CVTXAR : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a

InvocationCount=1  IterationCount=10  RunStrategy=Monitoring  
UnrollFactor=1  WarmupCount=2  

```
| Method        | Rows   | Storage | Mean     | Error     | StdDev    | Ratio | RatioSD | Gen0       | Gen1      | Gen2      | Allocated | Alloc Ratio |
|-------------- |------- |-------- |---------:|----------:|----------:|------:|--------:|-----------:|----------:|----------:|----------:|------------:|
| **DataVo**        | **100000** | **Memory**  | **42.05 ms** | **17.726 ms** | **11.725 ms** |  **1.06** |    **0.37** |  **6000.0000** | **3000.0000** | **1000.0000** |  **54.42 MB** |        **1.00** |
| Sqlite        | 100000 | Memory  | 64.74 ms |  1.213 ms |  0.802 ms |  1.63 |    0.35 | 11000.0000 |         - |         - |  93.84 MB |        1.72 |
| DuckDb        | 100000 | Memory  | 39.54 ms |  7.522 ms |  4.975 ms |  1.00 |    0.25 |  2000.0000 |         - |         - |  19.08 MB |        0.35 |
| DuckDb1Thread | 100000 | Memory  | 28.56 ms |  9.794 ms |  6.478 ms |  0.72 |    0.22 |  2000.0000 |         - |         - |  19.08 MB |        0.35 |
|               |        |         |          |           |           |       |         |            |           |           |           |             |
| **DataVo**        | **100000** | **File**    | **86.66 ms** | **26.875 ms** | **17.776 ms** |  **1.03** |    **0.27** |  **6000.0000** | **3000.0000** | **1000.0000** |  **61.61 MB** |        **1.00** |
| Sqlite        | 100000 | File    | 69.88 ms |  1.154 ms |  0.764 ms |  0.83 |    0.14 | 11000.0000 |         - |         - |  93.84 MB |        1.52 |
| DuckDb        | 100000 | File    | 28.63 ms |  9.554 ms |  6.319 ms |  0.34 |    0.09 |  2000.0000 |         - |         - |  19.08 MB |        0.31 |
| DuckDb1Thread | 100000 | File    | 27.84 ms |  8.243 ms |  5.452 ms |  0.33 |    0.08 |  2000.0000 |         - |         - |  19.08 MB |        0.31 |
