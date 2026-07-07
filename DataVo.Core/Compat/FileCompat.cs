namespace DataVo.Core.Compat;

internal static class FileCompat
{
    public static void Move(string sourceFileName, string destFileName, bool overwrite)
    {
#if NET6_0_OR_GREATER
        File.Move(sourceFileName, destFileName, overwrite);
#else
        if (overwrite && File.Exists(destFileName))
        {
            File.Delete(destFileName);
        }

        File.Move(sourceFileName, destFileName);
#endif
    }

    public static bool IsBrowser()
    {
#if NET6_0_OR_GREATER
        return OperatingSystem.IsBrowser();
#else
        return false;
#endif
    }
}
