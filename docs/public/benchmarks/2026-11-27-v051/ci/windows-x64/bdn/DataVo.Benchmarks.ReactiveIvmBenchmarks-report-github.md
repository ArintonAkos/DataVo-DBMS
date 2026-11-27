```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26100.33828/24H2/2024Update/HudsonValley) (Hyper-V)
Intel Xeon Platinum 8370C CPU 2.80GHz (Max: 2.79GHz), 1 CPU, 4 logical and 2 physical cores
.NET SDK 11.0.100
  [Host]     : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4
  DefaultJob : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v4


```
| Method                   | Storage | Mean      | Error     | StdDev    | Gen0   | Gen1   | Gen2   | Allocated |
|------------------------- |-------- |----------:|----------:|----------:|-------:|-------:|-------:|----------:|
| **ScoreUpdatesWithTopTen**   | **Memory**  |  **8.882 μs** | **0.1523 μs** | **0.1425 μs** | **0.2031** | **0.1250** |      **-** |    **5.4 KB** |
| EventUpdatesWithJoinFeed | Memory  | 14.484 μs | 0.2793 μs | 0.2988 μs | 0.2813 | 0.1250 | 0.0156 |   6.96 KB |
| **ScoreUpdatesWithTopTen**   | **File**    | **13.973 μs** | **0.2766 μs** | **0.5712 μs** | **0.2500** | **0.1719** |      **-** |   **6.39 KB** |
| EventUpdatesWithJoinFeed | File    | 19.085 μs | 0.3773 μs | 0.4772 μs | 0.3125 | 0.1250 |      - |   8.13 KB |
