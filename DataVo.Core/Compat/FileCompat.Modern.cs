#if NET10_0_OR_GREATER
namespace DataVo.Core.Compat;

internal static class FileCompat
{
    public static void Move(string sourceFileName, string destFileName, bool overwrite) =>
        File.Move(sourceFileName, destFileName, overwrite);

    public static bool IsBrowser() => OperatingSystem.IsBrowser();
}
#endif
