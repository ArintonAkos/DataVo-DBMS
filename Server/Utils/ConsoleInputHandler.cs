using Server.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Utils
{
    internal class ConsoleInputHandler
    {
        public static String GetSourceFileName()
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
}
