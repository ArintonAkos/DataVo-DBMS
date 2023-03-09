using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Commands
{
    internal class Patterns
    {
        public static String CreateTable
        {
            get
            {
                return @"^\s?create\stable\s([A-Z_]+)\s(\(\s*.*\s*\))";
            }
        }

        public static String DropTable
        {
            get
            {
                return @"^\s?drop\stable\s([A-Z_]+)";
            }
        }

        public static String CreateDatabase
        {
            get
            {
                return @"^\s?create\sdatabase\s([A-Z_]+)";
            }
        }

        public static String DropDatabase
        {
            get
            {
                return @"^\s?drop\sdatabase\s([A-Z_]+)";
            }
        }

        public static String Go
        {
            get
            {
                return @"^\s?go(\s+|$)";
            }
        }
    }
}
