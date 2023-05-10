namespace Server.Parser.Utils;

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

    public static string DropTable => @"^\s*drop\s+table\s+([A-Z_]+)";

    public static string CreateDatabase => @"^\s*create\s+database\s+([A-Z_]+)";

    public static string DropDatabase => @"^\s*drop\s+database\s+([A-Z_]+)";

    public static string CreateIndex =>
        @"^\s*create\s+index\s+(?<IndexName>[A-Z_]+)\s+on\s+(?<TableName>[A-Z_]+)\((?<Column>[A-Z_]+)\s*\)";

    public static string DropIndex => @"^\s*drop\s+index\s+([A-Z_]+)\s+from\s+([A-Z_]+)";

    public static string ShowDatabases => @"^\s*show\s+databases\s*";

    public static string ShowTables => @"^\s*show\s+tables\s*";

    public static string Describe => @"^\s*describe\s+([A-Z_]+)\s*";

    public static string Go => @"^\s*go(\s+|$)";

    public static string Use => @"^\s*use\s+(?<DatabaseName>[A-Z_]+)\s*";

    public static string InsertInto =>
        @"^\s*insert\s+into\s+(?<TableName>[A-Z_]+)\s*\((?<Columns>(\s*\w+\s*,?\s*)+)\)\s*VALUES\s*(?<AllValues>(\((?<Values>(\s*('[^']*'|[^,()]+)\s*,?\s*)+)\)\s*,?\s*)+\s*)";

    public static string Column
    {
        get
        {
            var integer = @"\s*int";
            var floating = @"\s*float";
            var bit = @"\s*bit";
            var date = @"\s*date";
            var varchar = @"\s*varchar\(\s*(?<Length>[0-9]+)\s*\)";
            var primary = @"\s*(?<PrimaryKey>primary\s+key)";
            var unique = @"\s*(?<Unique>unique)";
            var foreign =
                @"\s*(?<ForeignKey>references\s+((?<ForeignTable>[A-Z_]+)\s*\(\s*(?<ForeignColumn>[A-Z_]+)\s*\)\s*)+)";

            return
                @$"\s*(?<FieldName>[A-Z_]+)\s+(?<Type>{varchar}|{integer}|{floating}|{bit}|{date})(\s+{primary})?(\s+{unique})?(\s+{foreign})?\s*";
        }
    }

    public static string Value
    {
        get
        {
            var integerValue = @"(?<Integer>\s*\b[0-9]+\b\s*)";
            var floatingValue = @"(?<Floating>\s*[+-]?([0-9]*[.])?[0-9]+\s*)";
            var varcharValue = @"(?<VarChar>\s*'.*')";

            return @$"\s*(?<Column>{varcharValue}|{integerValue}|{floatingValue})\s*";
        }
    }

    public static string DeleteFrom => $@"^\s*delete\s+from\s+(?<TableName>[A-Z_]+)\s+(?<WhereStatement>{Where})\s*";

    public static string Where => @"where(\s+\(?\s*(\w+)\s*(=|<|>|like)+\s*[''A-Z0-9./]+\)?(\s+and|\s+or)?)+";

    public static string AddStartLine(this string s)
    {
        return @"^\s*" + s;
    }

    public static string AddEndLine(this string s)
    {
        return s + @"\s*$";
    }
}