using System.ComponentModel;
using System.Runtime.InteropServices;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Same-directory temp-file publication with file fsync, atomic rename, and directory fsync.</summary>
internal static partial class LsmDurableFileOperations
{
    private const int OpenReadOnly = 0;

    public static void WriteFileAtomically(
        string filePath,
        bool overwrite,
        Action<Stream> write,
        Action<LsmCrashPoint>? crashHook,
        LsmCrashPoint afterTempFileFsync,
        LsmCrashPoint afterRenameBeforeDirectoryFsync,
        LsmCrashPoint afterDirectoryFsync)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(write);

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

            File.Move(tempPath, filePath, overwrite);
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

    internal static void FsyncDirectory(string directoryPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directoryPath);
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        int fd = OpenDirectory(directoryPath);
        try
        {
            int result = OperatingSystem.IsMacOS()
                ? FsyncMac(fd)
                : FsyncLibc(fd);
            if (result != 0)
            {
                ThrowLastIoError($"Unable to fsync directory '{directoryPath}'.");
            }
        }
        finally
        {
            _ = OperatingSystem.IsMacOS() ? CloseMac(fd) : CloseLibc(fd);
        }
    }

    private static int OpenDirectory(string directoryPath)
    {
        int fd = OperatingSystem.IsMacOS()
            ? OpenMac(directoryPath, OpenReadOnly)
            : OpenLibc(directoryPath, OpenReadOnly);
        if (fd < 0)
        {
            ThrowLastIoError($"Unable to open directory '{directoryPath}' for fsync.");
        }

        return fd;
    }

    private static void ThrowLastIoError(string message)
    {
        int error = Marshal.GetLastPInvokeError();
        throw new IOException(message, new Win32Exception(error));
    }

    [LibraryImport("libSystem.dylib", EntryPoint = "open", StringMarshalling = StringMarshalling.Utf8, SetLastError = true)]
    private static partial int OpenMac(string path, int flags);

    [LibraryImport("libSystem.dylib", EntryPoint = "fsync", SetLastError = true)]
    private static partial int FsyncMac(int fd);

    [LibraryImport("libSystem.dylib", EntryPoint = "close", SetLastError = true)]
    private static partial int CloseMac(int fd);

    [LibraryImport("libc", EntryPoint = "open", StringMarshalling = StringMarshalling.Utf8, SetLastError = true)]
    private static partial int OpenLibc(string path, int flags);

    [LibraryImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static partial int FsyncLibc(int fd);

    [LibraryImport("libc", EntryPoint = "close", SetLastError = true)]
    private static partial int CloseLibc(int fd);
}
