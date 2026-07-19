#if NET10_0_OR_GREATER
namespace DataVo.Core.Compat;

internal static class MonotonicClock
{
    public static long GetMilliseconds() => Environment.TickCount64;
}
#endif
