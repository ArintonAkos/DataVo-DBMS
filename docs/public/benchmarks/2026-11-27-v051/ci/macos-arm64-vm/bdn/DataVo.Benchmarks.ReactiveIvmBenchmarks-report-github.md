```

BenchmarkDotNet v0.15.8, macOS Tahoe 26.7.2 (25G332) [Darwin 25.6.0]
Apple M1 (Virtual), 1 CPU, 3 logical and 3 physical cores
.NET SDK 11.0.100
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), Arm64 RyuJIT armv8.0-a
  DefaultJob : .NET 10.0.15 (10.0.15, 10.0.1526.53008), Arm64 RyuJIT armv8.0-a


```
| Method                   | Storage | Mean      | Error     | StdDev   | Median    | Gen0   | Gen1   | Gen2   | Allocated |
|------------------------- |-------- |----------:|----------:|---------:|----------:|-------:|-------:|-------:|----------:|
| **ScoreUpdatesWithTopTen**   | **Memory**  |  **8.163 μs** | **0.5339 μs** | **1.549 μs** |  **7.849 μs** | **0.8594** | **0.4219** |      **-** |   **5.41 KB** |
| EventUpdatesWithJoinFeed | Memory  | 14.743 μs | 1.0255 μs | 3.008 μs | 13.918 μs | 1.0000 | 0.5000 |      - |   6.97 KB |
| **ScoreUpdatesWithTopTen**   | **File**    | **10.148 μs** | **0.4355 μs** | **1.249 μs** |  **9.973 μs** | **1.0156** | **0.5000** |      **-** |   **6.38 KB** |
| EventUpdatesWithJoinFeed | File    | 15.872 μs | 0.8063 μs | 2.352 μs | 15.283 μs | 1.3281 | 0.3906 | 0.0469 |   8.13 KB |
