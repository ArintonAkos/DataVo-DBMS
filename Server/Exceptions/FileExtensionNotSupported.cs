using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Exceptions
{
    internal class FileExtensionNotSupported : Exception
    {
        public FileExtensionNotSupported(string extension) 
            : base($"File extension not supported! Only *.{extension} files can be compiled!")
        {
        }
    }
}
