using BenchmarkDotNet.Running;

// dotnet run -c Release --project benchmarks/DataVo.Benchmarks -- --filter '*'
// dotnet run -c Release --project benchmarks/DataVo.Benchmarks -- --filter '*Aggregate*' --job short
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
