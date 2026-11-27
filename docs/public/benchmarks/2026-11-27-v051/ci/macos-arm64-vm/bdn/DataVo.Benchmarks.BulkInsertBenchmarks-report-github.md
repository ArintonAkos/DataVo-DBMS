```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.7.2 (25G332) [Darwin 25.6.0]
Apple M1 (Virtual), 1 CPU, 3 logical and 3 physical cores
.NET SDK 11.0.100
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), Arm64 RyuJIT armv8.0-a
  Job-CVTXAR : .NET 10.0.15 (10.0.15, 10.0.1526.53008), Arm64 RyuJIT armv8.0-a

InvocationCount=1  IterationCount=10  RunStrategy=Monitoring  
UnrollFactor=1  WarmupCount=2  

```
| Method        | Rows   | Storage | Mean      | Error      | StdDev    | Ratio | RatioSD | Gen0       | Gen1      | Gen2      | Allocated | Alloc Ratio |
|-------------- |------- |-------- |----------:|-----------:|----------:|------:|--------:|-----------:|----------:|----------:|----------:|------------:|
| **DataVo**        | **100000** | **Memory**  |  **90.27 ms** |  **35.516 ms** | **23.492 ms** |  **1.05** |    **0.35** |  **7000.0000** | **3000.0000** | **1000.0000** |   **51.5 MB** |        **1.00** |
| Sqlite        | 100000 | Memory  | 155.40 ms |  23.278 ms | 15.397 ms |  1.81 |    0.42 | 15000.0000 |         - |         - |  93.84 MB |        1.82 |
| DuckDb        | 100000 | Memory  |  56.86 ms |  20.289 ms | 13.420 ms |  0.66 |    0.21 |  3000.0000 |         - |         - |  19.08 MB |        0.37 |
| DuckDb1Thread | 100000 | Memory  |  58.25 ms |  27.033 ms | 17.881 ms |  0.68 |    0.25 |  3000.0000 |         - |         - |  19.08 MB |        0.37 |
|               |        |         |           |            |           |       |         |            |           |           |           |             |
| **DataVo**        | **100000** | **File**    | **211.96 ms** | **106.304 ms** | **70.314 ms** |  **1.15** |    **0.63** |  **8000.0000** | **3000.0000** | **1000.0000** |  **75.26 MB** |        **1.00** |
| Sqlite        | 100000 | File    | 130.75 ms |   9.341 ms |  6.179 ms |  0.71 |    0.30 | 15000.0000 |         - |         - |  93.84 MB |        1.25 |
| DuckDb        | 100000 | File    |  56.97 ms |  21.835 ms | 14.443 ms |  0.31 |    0.15 |  3000.0000 |         - |         - |  19.08 MB |        0.25 |
| DuckDb1Thread | 100000 | File    |  52.40 ms |  22.202 ms | 14.685 ms |  0.28 |    0.15 |  3000.0000 |         - |         - |  19.08 MB |        0.25 |
