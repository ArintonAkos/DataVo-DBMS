namespace DataVo.Core.Exceptions;

internal class FileExtensionNotSupported(string extension) : Exception($"File extension not supported! Only *.{extension} files can be compiled!")
{
}