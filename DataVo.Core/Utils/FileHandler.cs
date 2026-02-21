using DataVo.Core.Exceptions;
using DataVo.Core.Parser.Utils;

namespace DataVo.Core.Utils;

internal class FileHandler
{
    internal static void ValidateFile(string path)
    {
        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"File not found: {path}!");
        }

        if (path.Split(".").Last() != ParserConfig.FILE_EXTENSION)
        {
            throw new FileExtensionNotSupported(ParserConfig.FILE_EXTENSION);
        }
    }

    public static string GetFileText(string path)
    {
        ValidateFile(path);

        return File.ReadAllText(path);
    }
}