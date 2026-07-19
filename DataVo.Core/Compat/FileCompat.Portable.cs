#if NETSTANDARD2_1
namespace DataVo.Core.Compat;

internal static class FileCompat
{
    public static void Move(string sourceFileName, string destFileName, bool overwrite)
    {
        if (overwrite && File.Exists(destFileName))
        {
            File.Delete(destFileName);
        }

        File.Move(sourceFileName, destFileName);
    }

    public static bool IsBrowser() => false;
}
#endif
