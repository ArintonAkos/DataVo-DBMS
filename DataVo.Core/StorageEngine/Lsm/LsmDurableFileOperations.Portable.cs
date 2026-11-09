#if NETSTANDARD2_1
using System.Runtime.InteropServices;
using System.Text;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Publishes files atomically on runtimes without the modern file APIs.</summary>
internal static class LsmDurableFileOperations
{
    private const int OpenReadOnly = 0;

    /// <summary>Writes a file so a crash leaves either nothing or the complete file under its final name.</summary>
    public static void WriteFileAtomically(
        string filePath,
        bool overwrite,
        Action<Stream> write,
        Action<LsmCrashPoint>? crashHook,
        LsmCrashPoint afterTempFileFsync,
        LsmCrashPoint afterRenameBeforeDirectoryFsync,
        LsmCrashPoint afterDirectoryFsync)
    {
        string? directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        string tempPath = filePath + ".tmp-" + Guid.NewGuid().ToString("N");
        bool preserveTemp = false;
        try
        {
            using (var stream = new FileStream(tempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                write(stream);
                stream.Flush(flushToDisk: true);
            }

            try
            {
                crashHook?.Invoke(afterTempFileFsync);
            }
            catch (LsmCrashSimulationException)
            {
                preserveTemp = true;
                throw;
            }

            Publish(tempPath, filePath, overwrite);
            crashHook?.Invoke(afterRenameBeforeDirectoryFsync);
            if (!string.IsNullOrEmpty(directory))
            {
                FsyncDirectory(directory);
            }

            crashHook?.Invoke(afterDirectoryFsync);
        }
        finally
        {
            if (!preserveTemp && File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }
        }
    }

    /// <summary>
    /// Forces a directory entry to disk so a rename survives power loss. Where the platform has no such call,
    /// such as iOS under IL2CPP, renames stay atomic and only power-loss ordering is weaker.
    /// </summary>
    internal static void FsyncDirectory(string directoryPath)
    {
        byte[] path = Encoding.UTF8.GetBytes(directoryPath + "\0");
        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                Sync(OpenMac(path, OpenReadOnly), FsyncMac, CloseMac, directoryPath);
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                Sync(OpenLibc(path, OpenReadOnly), FsyncLibc, CloseLibc, directoryPath);
            }
        }
        catch (Exception exception) when (exception is DllNotFoundException or EntryPointNotFoundException)
        {
        }
    }

    private static void Publish(string tempPath, string filePath, bool overwrite)
    {
        // Replace is the atomic path. Deleting first would leave a crash window with no file at all.
        if (overwrite && File.Exists(filePath))
        {
            File.Replace(tempPath, filePath, null);
            return;
        }

        File.Move(tempPath, filePath);
    }

    private static void Sync(int descriptor, Func<int, int> fsync, Func<int, int> close, string directoryPath)
    {
        if (descriptor < 0)
        {
            return;
        }

        try
        {
            if (fsync(descriptor) != 0)
            {
                throw new IOException($"Unable to fsync directory '{directoryPath}'.");
            }
        }
        finally
        {
            close(descriptor);
        }
    }

    [DllImport("libSystem.dylib", EntryPoint = "open")]
    private static extern int OpenMac(byte[] path, int flags);

    [DllImport("libSystem.dylib", EntryPoint = "fsync")]
    private static extern int FsyncMac(int descriptor);

    [DllImport("libSystem.dylib", EntryPoint = "close")]
    private static extern int CloseMac(int descriptor);

    [DllImport("libc", EntryPoint = "open")]
    private static extern int OpenLibc(byte[] path, int flags);

    [DllImport("libc", EntryPoint = "fsync")]
    private static extern int FsyncLibc(int descriptor);

    [DllImport("libc", EntryPoint = "close")]
    private static extern int CloseLibc(int descriptor);
}
#endif
