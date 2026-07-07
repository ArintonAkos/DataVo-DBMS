using System.ComponentModel;
using DataVo.Core.Compat;
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
#if NET6_0_OR_GREATER
        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(filePath);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(write);
#else
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(filePath));
        }

        if (write is null)
        {
            throw new ArgumentNullException(nameof(write));
        }
#endif

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

            FileCompat.Move(tempPath, filePath, overwrite);
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
#if NET6_0_OR_GREATER
        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(directoryPath);
#else
        if (string.IsNullOrWhiteSpace(directoryPath))
        {
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(directoryPath));
        }
#endif
        if (IsWindows())
        {
            return;
        }

        int fd = OpenDirectory(directoryPath);
        try
        {
            int result = IsMacOS()
                ? FsyncMac(fd)
                : FsyncLibc(fd);
            if (result != 0)
            {
                ThrowLastIoError($"Unable to fsync directory '{directoryPath}'.");
            }
        }
        finally
        {
            _ = IsMacOS() ? CloseMac(fd) : CloseLibc(fd);
        }
    }

    private static int OpenDirectory(string directoryPath)
    {
        int fd = IsMacOS()
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
        int error =
#if NET6_0_OR_GREATER
            Marshal.GetLastPInvokeError();
#else
            Marshal.GetLastWin32Error();
#endif
        throw new IOException(message, new Win32Exception(error));
    }

#if NET6_0_OR_GREATER
    private static bool IsWindows() => OperatingSystem.IsWindows();

    private static bool IsMacOS() => OperatingSystem.IsMacOS();

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
#else
    private static bool IsWindows() => RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    private static bool IsMacOS() => RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

    [DllImport("libSystem.dylib", EntryPoint = "open", CharSet = CharSet.Ansi, SetLastError = true)]
    private static extern int OpenMac(string path, int flags);

    [DllImport("libSystem.dylib", EntryPoint = "fsync", SetLastError = true)]
    private static extern int FsyncMac(int fd);

    [DllImport("libSystem.dylib", EntryPoint = "close", SetLastError = true)]
    private static extern int CloseMac(int fd);

    [DllImport("libc", EntryPoint = "open", CharSet = CharSet.Ansi, SetLastError = true)]
    private static extern int OpenLibc(string path, int flags);

    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static extern int FsyncLibc(int fd);

    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int CloseLibc(int fd);
#endif
}
