```

BenchmarkDotNet v0.15.8, Linux Ubuntu 26.04.1 LTS (Resolute Raccoon)
AMD EPYC 9V45 4.17GHz, 1 CPU, 4 logical and 2 physical cores
.NET SDK 11.0.100
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4


```
| Method                   | Storage | Mean     | Error     | StdDev    | Median   | Gen0   | Gen1   | Gen2   | Allocated |
|------------------------- |-------- |---------:|----------:|----------:|---------:|-------:|-------:|-------:|----------:|
| **ScoreUpdatesWithTopTen**   | **Memory**  | **4.731 μs** | **0.0927 μs** | **0.1172 μs** | **4.698 μs** | **0.3203** | **0.2813** |      **-** |    **5.4 KB** |
| EventUpdatesWithJoinFeed | Memory  | 7.927 μs | 0.1581 μs | 0.3788 μs | 7.898 μs | 0.3906 | 0.1563 |      - |   6.96 KB |
| **ScoreUpdatesWithTopTen**   | **File**    | **6.140 μs** | **0.1988 μs** | **0.5767 μs** | **5.983 μs** | **0.3828** | **0.2969** |      **-** |   **6.38 KB** |
| EventUpdatesWithJoinFeed | File    | 9.491 μs | 0.2177 μs | 0.6246 μs | 9.433 μs | 0.5156 | 0.2188 | 0.0313 |   8.13 KB |
