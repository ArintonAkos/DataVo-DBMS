namespace DataVo.Core.Compat;

internal static class EnvironmentCompat
{
    public static long TickCount64
    {
        get
        {
#if NET6_0_OR_GREATER
            return Environment.TickCount64;
#else
            return Environment.TickCount;
#endif
        }
    }
}
