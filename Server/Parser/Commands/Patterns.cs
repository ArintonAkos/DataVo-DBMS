using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Commands
{
    internal static class Patterns
    {
        public static String CreateTable
        {
            get
            {
                var columns = @$"\s*(?<Columns>({Column},?)+)";
                return @$"^\s*create\s+table\s+(?<TableName>[A-Z_]+)\s+\({columns}\)";
            }
        }

        public static String DropTable
        {
            get
            {
                return @"^\s*drop\s+table\s+([A-Z_]+)";
            }
        }

        public static String CreateDatabase
        {
            get
            {
                return @"^\s*create\s+database\s+([A-Z_]+)";
            }
        }

        public static String DropDatabase
        {
            get
            {
                return @"^\s*drop\s+database\s+([A-Z_]+)";
            }
        }

        public static String Go
        {
            get
            {
                return @"^\s*go(\s+|$)";
            }
        }

        public static String Column
        {
            get
            {
                string integer = @"\s*int";
                string floating = @"\s*float";
                string varchar = @"\s*varchar\(\s*(?<Length>[0-9]+)\s*\)";
                string primary = @"\s*(?<PrimaryKey>primary\s+key)";
                string foreign = @"\s*(?<ForeignKey>references\s+(?<ForeignTable>[A-Z_]+)\s*\(\s*(?<ForeignColumn>[A-Z_]+)\s*\))";

                return @$"\s*(?<FieldName>[A-Z_]+)\s+(?<Type>{varchar}|{integer}|{floating})(\s+{primary})?(\s+{foreign})?\s*";
            }
        }

        public static String Value
        {
            get
            {
                string integerValue = @"(?<Integer>\s*\b[0-9]+\b\s*)";
                string floatingValue = @"(?<Floating>\s*[+-]?([0-9]*[.])?[0-9]+\s*)";
                string varcharValue = @"(?<VarChar>\s*'.*')";

                return @$"\s*(?<Column>{varcharValue}|{integerValue}|{floatingValue})\s*";
            }
        }

        public static String AddStartLine(this String s)
        {
            return @"^\s*" + s;
        }

        public static String AddEndLine(this String s)
        {
            return s + @"\s*$";
        }
    }
}
