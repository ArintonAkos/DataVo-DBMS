namespace DataVo.Tests;

/// <summary>
/// Hands out private on-disk directories for persistent test engines so parallel test classes never
/// share a catalog or data files. Everything lives under one per-process root that is removed on exit.
/// </summary>
internal static class TestStorageDirectories
{
    private static readonly string Root = Path.Combine(
        Path.GetTempPath(),
        "datavo-tests",
        $"{Environment.ProcessId}-{Guid.NewGuid():N}");

    static TestStorageDirectories()
    {
        AppDomain.CurrentDomain.ProcessExit += static (_, _) => TryDeleteRoot();
    }

    /// <summary>Returns a new, empty directory path unique to this call.</summary>
    public static string Create(string prefix)
    {
        string path = Path.Combine(Root, $"{prefix}_{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteRoot()
    {
        try
        {
            if (Directory.Exists(Root))
            {
                Directory.Delete(Root, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best effort: a file still held open at exit only leaves a temp directory behind.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
