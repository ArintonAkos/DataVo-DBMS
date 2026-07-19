#if NETSTANDARD2_1
using System.Diagnostics;

namespace DataVo.Core.Compat;

internal static class MonotonicClock
{
    public static long GetMilliseconds()
    {
        long timestamp = Stopwatch.GetTimestamp();
        long wholeSeconds = timestamp / Stopwatch.Frequency;
        long remainder = timestamp % Stopwatch.Frequency;
        return checked((wholeSeconds * 1000L) + ((remainder * 1000L) / Stopwatch.Frequency));
    }
}
#endif
