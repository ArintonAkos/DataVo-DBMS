namespace DataVo.Core.Constants;

public static class SqlKeywords
{
    public const string SELECT = "SELECT";
    public const string FROM = "FROM";
    public const string WHERE = "WHERE";
    public const string INSERT = "INSERT";
    public const string INTO = "INTO";
    public const string VALUES = "VALUES";
    public const string CREATE = "CREATE";
    public const string TABLE = "TABLE";
    public const string DROP = "DROP";
    public const string INDEX = "INDEX";
    public const string ON = "ON";
    public const string SHOW = "SHOW";
    public const string DATABASES = "DATABASES";
    public const string TABLES = "TABLES";
    public const string DESCRIBE = "DESCRIBE";
    public const string DELETE = "DELETE";
    public const string UPDATE = "UPDATE";
    public const string SET = "SET";
    public const string USE = "USE";
    public const string GO = "GO";
    public const string DATABASE = "DATABASE";
    public const string PRIMARY = "PRIMARY";
    public const string KEY = "KEY";
    public const string UNIQUE = "UNIQUE";
    public const string REFERENCES = "REFERENCES";
    public const string INT = "INT";
    public const string FLOAT = "FLOAT";
    public const string BIT = "BIT";
    public const string DATE = "DATE";
    public const string VARCHAR = "VARCHAR";
    public const string AS = "AS";
    public const string BY = "BY";
    public const string GROUP = "GROUP";
    public const string ORDER = "ORDER";
    public const string HAVING = "HAVING";
    public const string ASC = "ASC";
    public const string DESC = "DESC";
    public const string ALTER = "ALTER";
    public const string ADD = "ADD";
    public const string MODIFY = "MODIFY";
    public const string JOIN = "JOIN";
    public const string INNER = "INNER";
    public const string LEFT = "LEFT";
    public const string RIGHT = "RIGHT";
    public const string FULL = "FULL";
    public const string OUTER = "OUTER";
    public const string CROSS = "CROSS";
    public const string COLUMN = "COLUMN";

    public static readonly string[] All =
    [
        SELECT, FROM, WHERE, INSERT, INTO, VALUES,
        CREATE, TABLE, DROP, INDEX, ON, SHOW, DATABASES,
        TABLES, DESCRIBE, DELETE, UPDATE, SET, USE, GO,
        DATABASE, PRIMARY, KEY, UNIQUE, REFERENCES,
        INT, FLOAT, BIT, DATE, VARCHAR, AS, BY, GROUP, ORDER,
        HAVING, ASC, DESC, ALTER, ADD, MODIFY,
        JOIN, INNER, LEFT, RIGHT, FULL, OUTER, CROSS
    ];
}

public static class SqlPunctuation
{
    public const char OpenParen = '(';
    public const char CloseParen = ')';
    public const char Comma = ',';
    public const char Star = '*';
    public const char Dot = '.';

    public const string OpenParenToken = "(";
    public const string CloseParenToken = ")";
    public const string CommaToken = ",";
    public const string StarToken = "*";
    public const string DotToken = ".";
}

public static class SqlLiterals
{
    public const string TrueExpression = "1=1";
}
