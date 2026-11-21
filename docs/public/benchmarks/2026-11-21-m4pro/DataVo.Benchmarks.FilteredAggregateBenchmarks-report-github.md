```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.3 (25D125) [Darwin 25.3.0]
Apple M4 Pro, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.103
  [Host]   : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a
  ShortRun : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method              | Categories | Rows   | Storage | Mean        | Error      | StdDev    | Ratio  | RatioSD | Allocated | Alloc Ratio |
|-------------------- |----------- |------- |-------- |------------:|-----------:|----------:|-------:|--------:|----------:|------------:|
| **DataVo_Count**        | **Count**      | **200000** | **Memory**  |    **27.11 μs** |   **0.716 μs** |  **0.039 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Count        | Count      | 200000 | Memory  | 3,655.84 μs | 212.844 μs | 11.667 μs | 134.85 |    0.41 |     504 B |          NA |
| DuckDb_Count        | Count      | 200000 | Memory  |   147.50 μs |  17.093 μs |  0.937 μs |   5.44 |    0.03 |    1144 B |          NA |
| DuckDb1Thread_Count | Count      | 200000 | Memory  |   123.05 μs |  11.238 μs |  0.616 μs |   4.54 |    0.02 |    1144 B |          NA |
|                     |            |        |         |             |            |           |        |         |           |             |
| **DataVo_Count**        | **Count**      | **200000** | **File**    |    **27.44 μs** |   **1.579 μs** |  **0.087 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Count        | Count      | 200000 | File    | 4,449.51 μs | 330.520 μs | 18.117 μs | 162.18 |    0.72 |     504 B |          NA |
| DuckDb_Count        | Count      | 200000 | File    |   211.18 μs |  27.814 μs |  1.525 μs |   7.70 |    0.05 |    1144 B |          NA |
| DuckDb1Thread_Count | Count      | 200000 | File    |   229.80 μs |  12.551 μs |  0.688 μs |   8.38 |    0.03 |    1144 B |          NA |
|                     |            |        |         |             |            |           |        |         |           |             |
| **DataVo_Max**          | **Max**        | **200000** | **Memory**  |    **89.21 μs** |   **0.730 μs** |  **0.040 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Max          | Max        | 200000 | Memory  | 4,916.34 μs | 563.630 μs | 30.894 μs |  55.11 |    0.30 |     504 B |          NA |
| DuckDb_Max          | Max        | 200000 | Memory  |   333.21 μs |  36.027 μs |  1.975 μs |   3.73 |    0.02 |    1145 B |          NA |
| DuckDb1Thread_Max   | Max        | 200000 | Memory  |   400.48 μs |  70.802 μs |  3.881 μs |   4.49 |    0.04 |    1145 B |          NA |
|                     |            |        |         |             |            |           |        |         |           |             |
| **DataVo_Max**          | **Max**        | **200000** | **File**    |    **89.62 μs** |   **2.238 μs** |  **0.123 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Max          | Max        | 200000 | File    | 5,640.20 μs | 421.153 μs | 23.085 μs |  62.93 |    0.24 |     504 B |          NA |
| DuckDb_Max          | Max        | 200000 | File    |   497.03 μs |  28.704 μs |  1.573 μs |   5.55 |    0.02 |    1145 B |          NA |
| DuckDb1Thread_Max   | Max        | 200000 | File    |   700.66 μs |  17.335 μs |  0.950 μs |   7.82 |    0.01 |    1145 B |          NA |
|                     |            |        |         |             |            |           |        |         |           |             |
| **DataVo_Min**          | **Min**        | **200000** | **Memory**  |    **72.87 μs** |   **3.915 μs** |  **0.215 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Min          | Min        | 200000 | Memory  | 4,567.14 μs | 464.539 μs | 25.463 μs |  62.67 |    0.34 |     504 B |          NA |
| DuckDb_Min          | Min        | 200000 | Memory  |   211.17 μs | 232.216 μs | 12.729 μs |   2.90 |    0.15 |    1136 B |          NA |
| DuckDb1Thread_Min   | Min        | 200000 | Memory  |   188.18 μs |  11.300 μs |  0.619 μs |   2.58 |    0.01 |    1136 B |          NA |
|                     |            |        |         |             |            |           |        |         |           |             |
| **DataVo_Min**          | **Min**        | **200000** | **File**    |    **72.53 μs** |   **1.684 μs** |  **0.092 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Min          | Min        | 200000 | File    | 5,347.59 μs | 138.056 μs |  7.567 μs |  73.73 |    0.12 |     504 B |          NA |
| DuckDb_Min          | Min        | 200000 | File    |   332.80 μs | 480.414 μs | 26.333 μs |   4.59 |    0.31 |    1137 B |          NA |
| DuckDb1Thread_Min   | Min        | 200000 | File    |   353.97 μs |  30.824 μs |  1.690 μs |   4.88 |    0.02 |    1137 B |          NA |
|                     |            |        |         |             |            |           |        |         |           |             |
| **DataVo_Sum**          | **Sum**        | **200000** | **Memory**  |    **78.15 μs** |   **1.675 μs** |  **0.092 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Sum          | Sum        | 200000 | Memory  | 4,731.30 μs | 657.559 μs | 36.043 μs |  60.54 |    0.40 |     504 B |          NA |
| DuckDb_Sum          | Sum        | 200000 | Memory  |   270.88 μs | 172.108 μs |  9.434 μs |   3.47 |    0.10 |    1169 B |          NA |
| DuckDb1Thread_Sum   | Sum        | 200000 | Memory  |   199.45 μs |  14.062 μs |  0.771 μs |   2.55 |    0.01 |    1168 B |          NA |
|                     |            |        |         |             |            |           |        |         |           |             |
| **DataVo_Sum**          | **Sum**        | **200000** | **File**    |    **77.90 μs** |   **0.521 μs** |  **0.029 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Sum          | Sum        | 200000 | File    | 5,570.33 μs | 157.338 μs |  8.624 μs |  71.50 |    0.10 |     504 B |          NA |
| DuckDb_Sum          | Sum        | 200000 | File    |   311.21 μs | 113.154 μs |  6.202 μs |   3.99 |    0.07 |    1169 B |          NA |
| DuckDb1Thread_Sum   | Sum        | 200000 | File    |   355.36 μs |  18.039 μs |  0.989 μs |   4.56 |    0.01 |    1169 B |          NA |
