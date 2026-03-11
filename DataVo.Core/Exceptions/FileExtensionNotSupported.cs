namespace DataVo.Core.Exceptions;

/// <summary>
/// Represents an attempt to use an unsupported file extension for a storage-related operation.
/// </summary>
/// <param name="extension">The extension that was expected or validated.</param>
internal class FileExtensionNotSupported(string extension) : Exception($"File extension not supported! Only *.{extension} files can be compiled!")
{
}