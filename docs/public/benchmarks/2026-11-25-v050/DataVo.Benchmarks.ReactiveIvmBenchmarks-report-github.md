```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.3 (25D125) [Darwin 25.3.0]
Apple M4 Pro, 1 CPU, 12 logical and 12 physical cores
.NET SDK 10.0.103
  [Host]     : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a
  DefaultJob : .NET 10.0.3 (10.0.3, 10.0.326.7603), Arm64 RyuJIT armv8.0-a


```
| Method                   | Storage | Mean      | Error     | StdDev    | Gen0   | Gen1   | Gen2   | Allocated |
|------------------------- |-------- |----------:|----------:|----------:|-------:|-------:|-------:|----------:|
| **ScoreUpdatesWithTopTen**   | **Memory**  |  **4.039 μs** | **0.0515 μs** | **0.0481 μs** | **0.7500** | **0.3750** |      **-** |   **6.28 KB** |
| EventUpdatesWithJoinFeed | Memory  |  6.872 μs | 0.1358 μs | 0.2307 μs | 0.9609 | 0.3906 | 0.0078 |   8.26 KB |
| **ScoreUpdatesWithTopTen**   | **File**    |  **9.288 μs** | **0.1549 μs** | **0.1449 μs** | **0.8438** | **0.3906** |      **-** |   **7.01 KB** |
| EventUpdatesWithJoinFeed | File    | 11.879 μs | 0.2353 μs | 0.3522 μs | 1.1563 | 0.4531 | 0.0625 |   9.17 KB |
