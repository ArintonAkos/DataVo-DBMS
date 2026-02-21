using DataVo.Core.Exceptions;

namespace DataVo.Core.Utils;

internal class ConsoleInputHandler
{
    public static string GetSourceFileName()
    {
        if (Environment.GetCommandLineArgs().Length > 1)
        {
            string sourceFile = Environment.GetCommandLineArgs()[1];

            FileHandler.ValidateFile(sourceFile);

            return sourceFile;
        }

        throw new NoSourceFileProvided();
    }
}