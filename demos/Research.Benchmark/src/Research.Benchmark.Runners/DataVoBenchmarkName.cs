namespace Research.Benchmark.Runners;

internal static class DataVoBenchmarkName
{
    public static string Format(string name)
    {
#if DATAVO_CORE_NETSTANDARD21
        return $"{name} [DataVo.Core netstandard2.1]";
#else
        return $"{name} [DataVo.Core net10.0]";
#endif
    }
}
