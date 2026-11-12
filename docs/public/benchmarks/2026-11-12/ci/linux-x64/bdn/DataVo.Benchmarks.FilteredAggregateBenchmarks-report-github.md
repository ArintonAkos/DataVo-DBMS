```

BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04.1 LTS (Resolute Raccoon)
AMD EPYC 7763 3.24GHz, 1 CPU, 4 logical and 2 physical cores
.NET SDK 10.0.404
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  DefaultJob : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3


```
| Method              | Categories | Rows   | Storage | Mean        | Error     | StdDev   | Ratio | RatioSD | Allocated | Alloc Ratio |
|-------------------- |----------- |------- |-------- |------------:|----------:|---------:|------:|--------:|----------:|------------:|
| **DataVo_Count**        | **Count**      | **200000** | **Memory**  |  **2,121.7 μs** |  **20.07 μs** | **18.77 μs** |  **1.00** |    **0.01** |         **-** |          **NA** |
| Sqlite_Count        | Count      | 200000 | Memory  |  7,721.0 μs |  31.80 μs | 26.55 μs |  3.64 |    0.03 |     504 B |          NA |
| DuckDb_Count        | Count      | 200000 | Memory  |    606.9 μs |   3.75 μs |  3.32 μs |  0.29 |    0.00 |    1145 B |          NA |
| DuckDb1Thread_Count | Count      | 200000 | Memory  |    566.8 μs |   1.19 μs |  0.99 μs |  0.27 |    0.00 |    1145 B |          NA |
|                     |            |        |         |             |           |          |       |         |           |             |
| **DataVo_Count**        | **Count**      | **200000** | **File**    |  **3,687.8 μs** |   **7.03 μs** |  **5.87 μs** |  **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Count        | Count      | 200000 | File    |  9,364.4 μs |  70.24 μs | 65.70 μs |  2.54 |    0.02 |     504 B |          NA |
| DuckDb_Count        | Count      | 200000 | File    |    719.6 μs |   7.40 μs |  6.56 μs |  0.20 |    0.00 |    1145 B |          NA |
| DuckDb1Thread_Count | Count      | 200000 | File    |    767.9 μs |   4.83 μs |  4.52 μs |  0.21 |    0.00 |    1145 B |          NA |
|                     |            |        |         |             |           |          |       |         |           |             |
| **DataVo_Max**          | **Max**        | **200000** | **Memory**  |  **2,863.5 μs** |  **36.13 μs** | **33.80 μs** |  **1.00** |    **0.02** |         **-** |          **NA** |
| Sqlite_Max          | Max        | 200000 | Memory  | 10,257.6 μs |  69.93 μs | 61.99 μs |  3.58 |    0.05 |     504 B |          NA |
| DuckDb_Max          | Max        | 200000 | Memory  |  1,041.3 μs |  15.84 μs | 13.23 μs |  0.36 |    0.01 |    1146 B |          NA |
| DuckDb1Thread_Max   | Max        | 200000 | Memory  |  1,256.8 μs |   5.92 μs |  5.54 μs |  0.44 |    0.01 |    1146 B |          NA |
|                     |            |        |         |             |           |          |       |         |           |             |
| **DataVo_Max**          | **Max**        | **200000** | **File**    |  **4,573.0 μs** |  **53.37 μs** | **49.92 μs** |  **1.00** |    **0.01** |         **-** |          **NA** |
| Sqlite_Max          | Max        | 200000 | File    | 12,076.6 μs |  54.79 μs | 45.75 μs |  2.64 |    0.03 |     504 B |          NA |
| DuckDb_Max          | Max        | 200000 | File    |  1,484.5 μs |  23.36 μs | 21.85 μs |  0.32 |    0.01 |    1146 B |          NA |
| DuckDb1Thread_Max   | Max        | 200000 | File    |  1,922.4 μs |  11.82 μs | 10.48 μs |  0.42 |    0.00 |    1148 B |          NA |
|                     |            |        |         |             |           |          |       |         |           |             |
| **DataVo_Min**          | **Min**        | **200000** | **Memory**  |  **2,807.4 μs** |  **35.10 μs** | **31.11 μs** |  **1.00** |    **0.02** |         **-** |          **NA** |
| Sqlite_Min          | Min        | 200000 | Memory  |  9,891.7 μs | 103.62 μs | 91.86 μs |  3.52 |    0.05 |     504 B |          NA |
| DuckDb_Min          | Min        | 200000 | Memory  |    730.4 μs |   5.56 μs |  4.64 μs |  0.26 |    0.00 |    1137 B |          NA |
| DuckDb1Thread_Min   | Min        | 200000 | Memory  |    744.4 μs |   1.92 μs |  1.70 μs |  0.27 |    0.00 |    1137 B |          NA |
|                     |            |        |         |             |           |          |       |         |           |             |
| **DataVo_Min**          | **Min**        | **200000** | **File**    |  **4,230.9 μs** |  **11.58 μs** |  **9.04 μs** |  **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Min          | Min        | 200000 | File    | 11,524.2 μs |  98.81 μs | 92.42 μs |  2.72 |    0.02 |     504 B |          NA |
| DuckDb_Min          | Min        | 200000 | File    |    912.6 μs |   5.43 μs |  4.54 μs |  0.22 |    0.00 |    1137 B |          NA |
| DuckDb1Thread_Min   | Min        | 200000 | File    |  1,057.0 μs |   5.09 μs |  4.25 μs |  0.25 |    0.00 |    1138 B |          NA |
|                     |            |        |         |             |           |          |       |         |           |             |
| **DataVo_Sum**          | **Sum**        | **200000** | **Memory**  |  **2,684.9 μs** |   **3.35 μs** |  **2.80 μs** |  **1.00** |    **0.00** |         **-** |          **NA** |
| Sqlite_Sum          | Sum        | 200000 | Memory  |  9,427.4 μs | 105.03 μs | 98.24 μs |  3.51 |    0.04 |     504 B |          NA |
| DuckDb_Sum          | Sum        | 200000 | Memory  |    786.9 μs |   6.54 μs |  6.11 μs |  0.29 |    0.00 |    1169 B |          NA |
| DuckDb1Thread_Sum   | Sum        | 200000 | Memory  |    766.1 μs |   2.78 μs |  2.60 μs |  0.29 |    0.00 |    1169 B |          NA |
|                     |            |        |         |             |           |          |       |         |           |             |
| **DataVo_Sum**          | **Sum**        | **200000** | **File**    |  **4,216.5 μs** |  **61.62 μs** | **57.64 μs** |  **1.00** |    **0.02** |         **-** |          **NA** |
| Sqlite_Sum          | Sum        | 200000 | File    | 11,126.6 μs |  73.70 μs | 65.33 μs |  2.64 |    0.04 |     504 B |          NA |
| DuckDb_Sum          | Sum        | 200000 | File    |    999.8 μs |   8.12 μs |  7.20 μs |  0.24 |    0.00 |    1170 B |          NA |
| DuckDb1Thread_Sum   | Sum        | 200000 | File    |  1,079.2 μs |   2.08 μs |  1.84 μs |  0.26 |    0.00 |    1170 B |          NA |
