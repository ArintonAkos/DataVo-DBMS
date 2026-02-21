using BenchmarkDotNet.Running;

namespace Server.Benchmarks;

public class Program
{
    public static void Main(string[] args)
    {
        var summary = BenchmarkRunner.Run<QueryBenchmarker>();
    }
}
