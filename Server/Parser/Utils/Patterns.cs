namespace Server.Parser.Utils;

internal static class Patterns
{
    public static string CreateTable
    {
        get
        {
            string columns = @$"\s*(?<Columns>({Column},?)+)";
            return @$"^\s*create\s+table\s+(?<TableName>[A-Z_]+)\s+\({columns}\)";
        }
    }

    public static string DropTable
    {
        get => @"^\s*drop\s+table\s+([A-Z_]+)";
    }

    public static string CreateDatabase
    {
        get => @"^\s*create\s+database\s+([A-Z_]+)";
    }

    public static string DropDatabase
    {
        get => @"^\s*drop\s+database\s+([A-Z_]+)";
    }

    public static string CreateIndex
    {
        get => @"^\s*create\s+index\s+(?<IndexName>[A-Z_]+)\s+on\s+(?<TableName>[A-Z_]+)\((?<Column>[A-Z_]+)\s*\)";
    }

    public static string DropIndex
    {
        get => @"^\s*drop\s+index\s+([A-Z_]+)\s+from\s+([A-Z_]+)";
    }

    public static string ShowDatabases
    {
        get => @"^\s*show\s+databases\s*";
    }

    public static string ShowTables
    {
        get => @"^\s*show\s+tables\s*";
    }

    public static string Describe
    {
        get => @"^\s*describe\s+([A-Z_]+)\s*";
    }

    public static string Go
    {
        get => @"^\s*go(\s+|$)";
    }

    public static string Use
    {
        get => @"^\s*use\s+(?<DatabaseName>[A-Z_]+)\s*";
    }

    public static string InsertInto
    {
        get =>
            @"^\s*insert\s+into\s+(?<TableName>[A-Z_]+)\s*\((?<Columns>(\s*\w+\s*,?\s*)+)\)\s*VALUES\s*(?<AllValues>(\((?<Values>(\s*('[^']*'|[^,()]+)\s*,?\s*)+)\)\s*,?\s*)+\s*)";
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
            string foreign =
                @"\s*(?<ForeignKey>references\s+((?<ForeignTable>[A-Z_]+)\s*\(\s*(?<ForeignColumn>[A-Z_]+)\s*\)\s*)+)";

            return
                @$"\s*(?<FieldName>[A-Z_]+)\s+(?<Type>{varchar}|{integer}|{floating}|{bit}|{date})(\s+{primary})?(\s+{unique})?(\s+{foreign})?\s*";
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
        get => $@"^\s*delete\s+from\s+(?<TableName>[A-Z_]+)\s+(?<WhereStatement>{Where})\s*";
    }

    public static string Where
    {
        get =>
            @"\s*where\s+(?<WhereStatement>([''A-Z0-9./]+\s*(=|!=|<>|<|>|<=|>=)\s*[''A-Z0-9./]+(\s+(and|or)\s*[''A-Z0-9./]+\s*(=|!=|<>|<|>|<=|>=)\s*[''A-Z0-9./]+)*))";
    }

    public static string SelectColumns
    {
        get => @"(\*|(\w+(\.\w+)?(\s+as\s+\w+)?|(\bSUM\b|\bCOUNT\b|\bAVG\b|\bMIN\b|\bMAX\b)\([A-Z0-9.*]+\)(\s+as\s+\w+)?)(,\s*(\w+(\.\w+)?|(\bSUM\b|\bCOUNT\b|\bAVG\b|\bMIN\b|\bMAX\b)\([A-Z0-9.*]+\))(\s+as\s+\w+)?)*)";
    }

    public static string Select
    {
        get =>
            $@"^\s*select\s+(?<Columns>{SelectColumns})\s+from\s+(?<TableName>\w+(\s+as\s+\w+)?)({Join})?(?<WhereStatement>\s+{Where})?(?<GroupByStatement>\s+{GroupBy})?\s*";
    }

    public static string Join
    {
        get =>
            @"(?<Joins>(?:\s*join\s+(?<JoinTable>\w+(\s+as\s+\w+)?)(?:\s+on\s+(?<JoinCondition>\w+(\.\w+)?\s*=\s*\w+(\.\w+)?))*)*)";
    }

    public static string GroupBy
    {
        get => @"\s*GROUP BY\s+(?<ColumnNames>(\w+\s*((,\s*\w+\s*)*)?))";
    }

    public static string AddStartLine(this string s) => @"^\s*" + s;

    public static string AddEndLine(this string s) => s + @"\s*$";
}