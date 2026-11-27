```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.3 (25D125) [Darwin 25.3.0]
Apple M4 Pro, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.103
  [Host]     : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a
  DefaultJob : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a


```
| Method                   | Storage | Mean      | Error     | StdDev    | Gen0   | Gen1   | Gen2   | Allocated |
|------------------------- |-------- |----------:|----------:|----------:|-------:|-------:|-------:|----------:|
| **ScoreUpdatesWithTopTen**   | **Memory**  |  **3.894 μs** | **0.0247 μs** | **0.0193 μs** | **0.6406** | **0.3125** |      **-** |    **5.4 KB** |
| EventUpdatesWithJoinFeed | Memory  |  6.364 μs | 0.1252 μs | 0.1045 μs | 0.8047 | 0.2656 | 0.0078 |   6.96 KB |
| **ScoreUpdatesWithTopTen**   | **File**    |  **8.989 μs** | **0.1045 μs** | **0.0978 μs** | **0.7656** | **0.3750** |      **-** |   **6.39 KB** |
| EventUpdatesWithJoinFeed | File    | 10.978 μs | 0.1957 μs | 0.2679 μs | 1.0000 | 0.3906 | 0.0313 |   8.13 KB |
