using Server.Exceptions;
using Server.Parser;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Server.Utils
{
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

        public static String GetFileText(string path)
        {
            ValidateFile(path);

            return File.ReadAllText(path);
        }
    }
}
