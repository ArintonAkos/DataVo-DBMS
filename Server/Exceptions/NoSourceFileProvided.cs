using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Exceptions
{
    internal class NoSourceFileProvided : Exception
    {
        public NoSourceFileProvided()
            : base("No source file was found when calling the compiler!")
        { }
    }
}
