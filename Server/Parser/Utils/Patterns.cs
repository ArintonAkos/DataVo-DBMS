namespace Server.Parser.Utils
{
    internal static class Patterns
    {
        public static string CreateTable
        {
            get
            {
                var columns = @$"\s*(?<Columns>({Column},?)+)";
                return @$"^\s*create\s+table\s+(?<TableName>[A-Z_]+)\s+\({columns}\)";
            }
        }

        public static string DropTable
        {
            get
            {
                return @"^\s*drop\s+table\s+([A-Z_]+)";
            }
        }

        public static string CreateDatabase
        {
            get
            {
                return @"^\s*create\s+database\s+([A-Z_]+)";
            }
        }

        public static string DropDatabase
        {
            get
            {
                return @"^\s*drop\s+database\s+([A-Z_]+)";
            }
        }

        public static string CreateIndex
        {
            get
            {
                return @"^\s*create\s+index\s+(?<IndexName>[A-Z_]+)\s+on\s+(?<TableName>[A-Z_]+)\(((?<Column>[A-Z_]+),?\s*)+\)";
            }
        }

        public static string DropIndex
        {
            get
            {
                return @"^\s*drop\s+index\s+([A-Z_]+)\s+from\s+([A-Z_]+)";
            }
        }

        public static string ShowDatabases
        {
            get
            {
                return @"^\s*show\s+databases\s*";
            }
        }

        public static string ShowTables
        {
            get
            {
                return @"^\s*show\s+tables\s*";
            }
        }

        public static string Describe
        {
            get
            {
                return @"^\s*describe\s+([A-Z_]+)\s*";
            }
        }

        public static string Go
        {
            get
            {
                return @"^\s*go(\s+|$)";
            }
        }

        public static string Use
        {
            get
            {
                return @"^\s*use\s+(?<DatabaseName>[A-Z_]+)\s*";
            }
        }

        public static string InsertInto
        {
            get
            {
                return @"^\s*insert\s+into\s+(?<TableName>[A-Z_]+)\s*\((?<Columns>(\s*\w+\s*,?\s*)+)\)\s*VALUES\s*(?<AllValues>(\((?<Values>(\s*('[^']*'|[^,()]+)\s*,?\s*)+)\)\s*,?\s*)+\s*)";
            }
        }

        public static string Column
        {
            get
            {
                string integer = @"\s*int";
                string floating = @"\s*float";
                string bit = @"\s*bit";
                string date = @"\s*date";
                string varchar = @"\s*varchar\(\s*(?<Length>[0-9]+)\s*\)";
                string primary = @"\s*(?<PrimaryKey>primary\s+key)";
                string unique = @"\s*(?<Unique>unique)";
                string foreign = @"\s*(?<ForeignKey>references\s+((?<ForeignTable>[A-Z_]+)\s*\(\s*(?<ForeignColumn>[A-Z_]+)\s*\)\s*)+)";

                return @$"\s*(?<FieldName>[A-Z_]+)\s+(?<Type>{varchar}|{integer}|{floating}|{bit}|{date})(\s+{primary})?(\s+{unique})?(\s+{foreign})?\s*";
            }
        }

        public static string Value
        {
            get
            {
                string integerValue = @"(?<Integer>\s*\b[0-9]+\b\s*)";
                string floatingValue = @"(?<Floating>\s*[+-]?([0-9]*[.])?[0-9]+\s*)";
                string varcharValue = @"(?<VarChar>\s*'.*')";

                return @$"\s*(?<Column>{varcharValue}|{integerValue}|{floatingValue})\s*";
            }
        }

        public static string DeleteFrom
        {
            get
            {
                return $@"^\s*delete\s+from\s+(?<TableName>[A-Z_]+)\s+(?<WhereStatement>\s*where\s+(?<Conditions>(.*)+\s*)+;\s*)\s*"; 
            }
        }

        public static string Where
        {
            get
            {
                return @"\s*where\s+(?<Conditions>(.*)+\s*)+;\s*";
            }
        }

        public static string AddStartLine(this string s)
        {
            return @"^\s*" + s;
        }

        public static string AddEndLine(this string s)
        {
            return s + @"\s*$";
        }
    }
}
