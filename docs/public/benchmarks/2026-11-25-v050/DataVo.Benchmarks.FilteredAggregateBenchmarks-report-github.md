```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.3 (25D125) [Darwin 25.3.0]
Apple M4 Pro, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.103
  [Host]     : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a
  DefaultJob : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a


```
| Method              | Categories | Rows   | Storage | Mean        | Error     | StdDev    | Ratio  | RatioSD | Allocated | Alloc Ratio |
|-------------------- |----------- |------- |-------- |------------:|----------:|----------:|-------:|--------:|----------:|------------:|
| **DataVo_Count**        | **Count**      | **200000** | **Memory**  |    **27.34 μs** |  **0.034 μs** |  **0.031 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Count        | Count      | 200000 | Memory  | 3,718.63 μs | 25.309 μs | 23.674 μs | 136.00 |    0.85 |     504 B |          NA |
| DuckDb_Count        | Count      | 200000 | Memory  |   186.84 μs |  3.725 μs |  7.439 μs |   6.83 |    0.27 |    1144 B |          NA |
| DuckDb1Thread_Count | Count      | 200000 | Memory  |   128.54 μs |  0.539 μs |  0.505 μs |   4.70 |    0.02 |    1144 B |          NA |
|                     |            |        |         |             |           |           |        |         |           |             |
| **DataVo_Count**        | **Count**      | **200000** | **File**    |    **27.80 μs** |  **0.019 μs** |  **0.016 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Count        | Count      | 200000 | File    | 4,560.71 μs | 18.168 μs | 16.105 μs | 164.07 |    0.57 |     504 B |          NA |
| DuckDb_Count        | Count      | 200000 | File    |   245.99 μs |  4.223 μs |  5.920 μs |   8.85 |    0.21 |    1145 B |          NA |
| DuckDb1Thread_Count | Count      | 200000 | File    |   236.55 μs |  0.594 μs |  0.526 μs |   8.51 |    0.02 |    1144 B |          NA |
|                     |            |        |         |             |           |           |        |         |           |             |
| **DataVo_Max**          | **Max**        | **200000** | **Memory**  |    **91.13 μs** |  **0.099 μs** |  **0.087 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Max          | Max        | 200000 | Memory  | 5,018.21 μs | 24.393 μs | 19.044 μs |  55.07 |    0.21 |     504 B |          NA |
| DuckDb_Max          | Max        | 200000 | Memory  |   324.33 μs |  3.343 μs |  2.964 μs |   3.56 |    0.03 |    1145 B |          NA |
| DuckDb1Thread_Max   | Max        | 200000 | Memory  |   409.73 μs |  1.394 μs |  1.304 μs |   4.50 |    0.01 |    1145 B |          NA |
|                     |            |        |         |             |           |           |        |         |           |             |
| **DataVo_Max**          | **Max**        | **200000** | **File**    |    **91.13 μs** |  **0.073 μs** |  **0.068 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Max          | Max        | 200000 | File    | 5,691.91 μs | 30.964 μs | 28.964 μs |  62.46 |    0.31 |     504 B |          NA |
| DuckDb_Max          | Max        | 200000 | File    |   505.91 μs |  1.672 μs |  1.482 μs |   5.55 |    0.02 |    1145 B |          NA |
| DuckDb1Thread_Max   | Max        | 200000 | File    |   707.82 μs |  1.329 μs |  1.243 μs |   7.77 |    0.01 |    1145 B |          NA |
|                     |            |        |         |             |           |           |        |         |           |             |
| **DataVo_Min**          | **Min**        | **200000** | **Memory**  |    **72.72 μs** |  **0.055 μs** |  **0.052 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Min          | Min        | 200000 | Memory  | 4,539.44 μs | 30.882 μs | 28.887 μs |  62.43 |    0.39 |     504 B |          NA |
| DuckDb_Min          | Min        | 200000 | Memory  |   204.15 μs |  3.535 μs |  2.952 μs |   2.81 |    0.04 |    1136 B |          NA |
| DuckDb1Thread_Min   | Min        | 200000 | Memory  |   194.37 μs |  0.621 μs |  0.581 μs |   2.67 |    0.01 |    1136 B |          NA |
|                     |            |        |         |             |           |           |        |         |           |             |
| **DataVo_Min**          | **Min**        | **200000** | **File**    |    **72.93 μs** |  **0.108 μs** |  **0.091 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Min          | Min        | 200000 | File    | 5,394.66 μs | 16.052 μs | 14.230 μs |  73.97 |    0.21 |     504 B |          NA |
| DuckDb_Min          | Min        | 200000 | File    |   301.20 μs |  6.023 μs | 10.549 μs |   4.13 |    0.14 |    1137 B |          NA |
| DuckDb1Thread_Min   | Min        | 200000 | File    |   355.01 μs |  0.439 μs |  0.366 μs |   4.87 |    0.01 |    1137 B |          NA |
|                     |            |        |         |             |           |           |        |         |           |             |
| **DataVo_Sum**          | **Sum**        | **200000** | **Memory**  |    **78.05 μs** |  **0.080 μs** |  **0.071 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Sum          | Sum        | 200000 | Memory  | 4,779.78 μs | 76.209 μs | 71.286 μs |  61.24 |    0.89 |     504 B |          NA |
| DuckDb_Sum          | Sum        | 200000 | Memory  |   254.29 μs |  4.965 μs |  5.312 μs |   3.26 |    0.07 |    1168 B |          NA |
| DuckDb1Thread_Sum   | Sum        | 200000 | Memory  |   199.13 μs |  0.521 μs |  0.435 μs |   2.55 |    0.01 |    1168 B |          NA |
|                     |            |        |         |             |           |           |        |         |           |             |
| **DataVo_Sum**          | **Sum**        | **200000** | **File**    |    **78.22 μs** |  **0.110 μs** |  **0.098 μs** |   **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Sum          | Sum        | 200000 | File    | 5,621.52 μs | 19.486 μs | 18.227 μs |  71.87 |    0.24 |     504 B |          NA |
| DuckDb_Sum          | Sum        | 200000 | File    |   341.02 μs |  5.441 μs |  6.881 μs |   4.36 |    0.09 |    1169 B |          NA |
| DuckDb1Thread_Sum   | Sum        | 200000 | File    |   356.92 μs |  1.175 μs |  1.042 μs |   4.56 |    0.01 |    1169 B |          NA |
