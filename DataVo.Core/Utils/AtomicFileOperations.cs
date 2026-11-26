using DataVo.Core.Compat;

namespace DataVo.Core.Utils;

/// <summary>
/// Provides cross-runtime file replacement helpers for atomic-ish persistence flows.
/// </summary>
internal static class AtomicFileOperations
{
    /// <summary>
    /// Replaces <paramref name="destinationPath"/> with <paramref name="tempPath"/>.
    /// Uses <see cref="File.Replace(string,string,string?)"/> when available and falls back
    /// to delete+move in browser runtimes where Replace is not implemented.
    /// </summary>
    /// <param name="tempPath">Path of the temporary file containing fully written content.</param>
    /// <param name="destinationPath">Path to replace with the temporary file content.</param>
    /// <exception cref="FileNotFoundException">Thrown when <paramref name="tempPath"/> does not exist.</exception>
    public static void ReplaceFromTemp(string tempPath, string destinationPath)
    {
        // On Windows a just-written file is often opened briefly by antivirus or the search indexer,
        // which makes replace/move fail with a sharing violation for a few milliseconds. Retry those.
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                ReplaceFromTempOnce(tempPath, destinationPath);
                return;
            }
            catch (Exception ex) when (attempt < SharingViolationRetries && IsTransientSharingViolation(ex))
            {
                Thread.Sleep(SharingViolationRetryDelayMs * attempt);
            }
        }
    }

    private const int SharingViolationRetries = 10;
    private const int SharingViolationRetryDelayMs = 10;

    private static bool IsTransientSharingViolation(Exception exception)
    {
        if (exception is not IOException io || !System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows))
        {
            return false;
        }

        int win32 = io.HResult & 0xFFFF;
        return win32 is 32 or 33; // ERROR_SHARING_VIOLATION, ERROR_LOCK_VIOLATION
    }

    private static void ReplaceFromTempOnce(string tempPath, string destinationPath)
    {
        if (!File.Exists(tempPath))
        {
            throw new FileNotFoundException($"Temp file not found: {tempPath}", tempPath);
        }

        if (!File.Exists(destinationPath))
        {
            FileCompat.Move(tempPath, destinationPath, overwrite: true);
            return;
        }

        if (FileCompat.IsBrowser())
        {
            ReplaceViaDeleteAndMove(tempPath, destinationPath);
            return;
        }

        try
        {
            File.Replace(tempPath, destinationPath, null);
        }
        catch (PlatformNotSupportedException)
        {
            ReplaceViaDeleteAndMove(tempPath, destinationPath);
        }
    }

    /// <summary>
    /// Browser-safe replacement strategy used when true atomic replace is unavailable.
    /// Deletes the destination and then moves the temporary file into place.
    /// </summary>
    /// <param name="tempPath">Path of the fully written temporary file.</param>
    /// <param name="destinationPath">Path that should receive the replacement file.</param>
    private static void ReplaceViaDeleteAndMove(string tempPath, string destinationPath)
    {
        string backupPath = destinationPath + ".bak";
        bool movedDestinationToBackup = false;

        try
        {
            if (File.Exists(backupPath))
            {
                File.Delete(backupPath);
            }

            if (File.Exists(destinationPath))
            {
                FileCompat.Move(destinationPath, backupPath, overwrite: true);
                movedDestinationToBackup = true;
            }

            FileCompat.Move(tempPath, destinationPath, overwrite: true);

            if (movedDestinationToBackup && File.Exists(backupPath))
            {
                try
                {
                    File.Delete(backupPath);
                }
                catch
                {
                    // Cleanup best effort; replacement has already succeeded.
                }
            }
        }
        catch
        {
            if (movedDestinationToBackup && !File.Exists(destinationPath) && File.Exists(backupPath))
            {
                FileCompat.Move(backupPath, destinationPath, overwrite: true);
            }

            throw;
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                try
                {
                    File.Delete(tempPath);
                }
                catch
                {
                    // Cleanup best effort.
                }
            }
        }
    }
}
