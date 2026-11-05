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

/// <summary>File helpers that behave the same on Windows, where share modes are enforced.</summary>
internal static class TestFiles
{
    /// <summary>Reads a file that the engine may still hold open for writing.</summary>
    public static byte[] ReadAllBytesShared(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        using var buffer = new MemoryStream();
        stream.CopyTo(buffer);
        return buffer.ToArray();
    }

    /// <summary>Overwrites a file that the engine may still hold open (for example to corrupt a WAL).</summary>
    public static void WriteAllBytesShared(string path, byte[] bytes)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete);
        stream.Write(bytes, 0, bytes.Length);
        stream.SetLength(bytes.Length);
    }
}
