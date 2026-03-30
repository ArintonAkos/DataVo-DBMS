using System.Reflection;

namespace DataVo.Core.Utils;

/// <summary>
/// Provides cross-runtime file replacement helpers for atomic-ish persistence flows.
/// </summary>
internal static class AtomicFileOperations
{
    /// <summary>
    /// Optional runtime binding to <see cref="File.Replace(string,string,string?)"/>.
    /// Null when the API is unavailable on the current platform profile.
    /// </summary>
    private static readonly MethodInfo? FileReplaceMethod = typeof(File).GetMethod(
        nameof(File.Replace),
        [typeof(string), typeof(string), typeof(string)]);

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
        if (!File.Exists(tempPath))
        {
            throw new FileNotFoundException($"Temp file not found: {tempPath}", tempPath);
        }

        if (!File.Exists(destinationPath))
        {
            File.Move(tempPath, destinationPath, overwrite: true);
            return;
        }

        if (OperatingSystem.IsBrowser())
        {
            ReplaceViaDeleteAndMove(tempPath, destinationPath);
            return;
        }

        if (FileReplaceMethod is null)
        {
            ReplaceViaDeleteAndMove(tempPath, destinationPath);
            return;
        }

        try
        {
            FileReplaceMethod.Invoke(null, [tempPath, destinationPath, null]);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is PlatformNotSupportedException || ex.InnerException is MissingMethodException || ex.InnerException is EntryPointNotFoundException)
        {
            ReplaceViaDeleteAndMove(tempPath, destinationPath);
        }
        catch (ArgumentException)
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
        if (File.Exists(destinationPath))
        {
            File.Delete(destinationPath);
        }

        File.Move(tempPath, destinationPath, overwrite: true);
    }
}