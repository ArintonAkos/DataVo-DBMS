namespace DataVo.Core.Constants;

/// <summary>
/// Defines the SQL keywords recognized by the lexer and parser.
/// </summary>
/// <remarks>
/// The constants in this type centralize grammar tokens so parser components can remain consistent
/// and avoid scattering hard-coded literals throughout the codebase.
/// </remarks>
public static class SqlKeywords
{
    /// <summary>
    /// Represents the SQL SELECT keyword.
    /// </summary>
    public const string SELECT = "SELECT";
    /// <summary>
    /// Represents the SQL FROM keyword.
    /// </summary>
    public const string FROM = "FROM";
    /// <summary>
    /// Represents the SQL WHERE keyword.
    /// </summary>
    public const string WHERE = "WHERE";
    /// <summary>
    /// Represents the SQL INSERT keyword.
    /// </summary>
    public const string INSERT = "INSERT";
    /// <summary>
    /// Represents the SQL INTO keyword.
    /// </summary>
    public const string INTO = "INTO";
    /// <summary>
    /// Represents the SQL VALUES keyword.
    /// </summary>
    public const string VALUES = "VALUES";
    /// <summary>
    /// Represents the SQL CREATE keyword.
    /// </summary>
    public const string CREATE = "CREATE";
    /// <summary>
    /// Represents the SQL TABLE keyword.
    /// </summary>
    public const string TABLE = "TABLE";
    /// <summary>
    /// Represents the SQL DROP keyword.
    /// </summary>
    public const string DROP = "DROP";
    /// <summary>
    /// Represents the SQL INDEX keyword.
    /// </summary>
    public const string INDEX = "INDEX";
    /// <summary>
    /// Represents the SQL ON keyword.
    /// </summary>
    public const string ON = "ON";
    /// <summary>
    /// Represents the SQL SHOW keyword.
    /// </summary>
    public const string SHOW = "SHOW";
    /// <summary>
    /// Represents the SQL DATABASES keyword.
    /// </summary>
    public const string DATABASES = "DATABASES";
    /// <summary>
    /// Represents the SQL TABLES keyword.
    /// </summary>
    public const string TABLES = "TABLES";
    /// <summary>
    /// Represents the SQL USERS keyword.
    /// </summary>
    public const string USERS = "USERS";
    /// <summary>
    /// Represents the SQL ROLES keyword.
    /// </summary>
    public const string ROLES = "ROLES";
    /// <summary>
    /// Represents the SQL GRANTS keyword.
    /// </summary>
    public const string GRANTS = "GRANTS";
    /// <summary>
    /// Represents the SQL DESCRIBE keyword.
    /// </summary>
    public const string DESCRIBE = "DESCRIBE";
    /// <summary>
    /// Represents the SQL DELETE keyword.
    /// </summary>
    public const string DELETE = "DELETE";
    /// <summary>
    /// Represents the SQL UPDATE keyword.
    /// </summary>
    public const string UPDATE = "UPDATE";
    /// <summary>
    /// Represents the SQL SET keyword.
    /// </summary>
    public const string SET = "SET";
    /// <summary>
    /// Represents the SQL USE keyword.
    /// </summary>
    public const string USE = "USE";
    /// <summary>
    /// Represents the SQL GO keyword.
    /// </summary>
    public const string GO = "GO";
    /// <summary>
    /// Represents the SQL DATABASE keyword.
    /// </summary>
    public const string DATABASE = "DATABASE";
    /// <summary>
    /// Represents the SQL DEFAULT keyword.
    /// </summary>
    public const string DEFAULT = "DEFAULT";
    /// <summary>
    /// Represents the SQL PRIMARY keyword.
    /// </summary>
    public const string PRIMARY = "PRIMARY";
    /// <summary>
    /// Represents the SQL KEY keyword.
    /// </summary>
    public const string KEY = "KEY";
    /// <summary>
    /// Represents the SQL UNIQUE keyword.
    /// </summary>
    public const string UNIQUE = "UNIQUE";
    /// <summary>
    /// Represents the SQL REFERENCES keyword.
    /// </summary>
    public const string REFERENCES = "REFERENCES";
    /// <summary>
    /// Represents the SQL FOREIGN keyword.
    /// </summary>
    public const string FOREIGN = "FOREIGN";
    /// <summary>
    /// Represents the SQL INT keyword.
    /// </summary>
    public const string INT = "INT";
    /// <summary>
    /// Represents the SQL FLOAT keyword.
    /// </summary>
    public const string FLOAT = "FLOAT";
    /// <summary>
    /// Represents the SQL BIT keyword.
    /// </summary>
    public const string BIT = "BIT";
    /// <summary>
    /// Represents the SQL DATE keyword.
    /// </summary>
    public const string DATE = "DATE";
    /// <summary>
    /// Represents the SQL VARCHAR keyword.
    /// </summary>
    public const string VARCHAR = "VARCHAR";
    /// <summary>
    /// Represents the SQL VECTOR keyword.
    /// </summary>
    public const string VECTOR = "VECTOR";
    /// <summary>
    /// Represents the SQL AS keyword.
    /// </summary>
    public const string AS = "AS";
    /// <summary>
    /// Represents the SQL BY keyword.
    /// </summary>
    public const string BY = "BY";
    /// <summary>
    /// Represents the SQL GROUP keyword.
    /// </summary>
    public const string GROUP = "GROUP";
    /// <summary>
    /// Represents the SQL ORDER keyword.
    /// </summary>
    public const string ORDER = "ORDER";
    /// <summary>
    /// Represents the SQL HAVING keyword.
    /// </summary>
    public const string HAVING = "HAVING";
    /// <summary>
    /// Represents the SQL ASC keyword.
    /// </summary>
    public const string ASC = "ASC";
    /// <summary>
    /// Represents the SQL DESC keyword.
    /// </summary>
    public const string DESC = "DESC";
    /// <summary>
    /// Represents the SQL ALTER keyword.
    /// </summary>
    public const string ALTER = "ALTER";
    /// <summary>
    /// Represents the SQL ADD keyword.
    /// </summary>
    public const string ADD = "ADD";
    /// <summary>
    /// Represents the SQL MODIFY keyword.
    /// </summary>
    public const string MODIFY = "MODIFY";
    /// <summary>
    /// Represents the SQL JOIN keyword.
    /// </summary>
    public const string JOIN = "JOIN";
    /// <summary>
    /// Represents the SQL INNER keyword.
    /// </summary>
    public const string INNER = "INNER";
    /// <summary>
    /// Represents the SQL LEFT keyword.
    /// </summary>
    public const string LEFT = "LEFT";
    /// <summary>
    /// Represents the SQL RIGHT keyword.
    /// </summary>
    public const string RIGHT = "RIGHT";
    /// <summary>
    /// Represents the SQL FULL keyword.
    /// </summary>
    public const string FULL = "FULL";
    /// <summary>
    /// Represents the SQL OUTER keyword.
    /// </summary>
    public const string OUTER = "OUTER";
    /// <summary>
    /// Represents the SQL CROSS keyword.
    /// </summary>
    public const string CROSS = "CROSS";
    /// <summary>
    /// Represents the SQL DISTINCT keyword.
    /// </summary>
    public const string DISTINCT = "DISTINCT";
    /// <summary>
    /// Represents the SQL LIMIT keyword.
    /// </summary>
    public const string LIMIT = "LIMIT";
    /// <summary>
    /// Represents the SQL OFFSET keyword.
    /// </summary>
    public const string OFFSET = "OFFSET";
    /// <summary>
    /// Represents the SQL IN keyword.
    /// </summary>
    public const string IN = "IN";
    /// <summary>
    /// Represents the SQL BETWEEN keyword.
    /// </summary>
    public const string BETWEEN = "BETWEEN";
    /// <summary>
    /// Represents the SQL LIKE keyword.
    /// </summary>
    public const string LIKE = "LIKE";
    /// <summary>
    /// Represents the SQL UNION keyword.
    /// </summary>
    public const string UNION = "UNION";
    /// <summary>
    /// Represents the SQL ALL keyword.
    /// </summary>
    public const string ALL = "ALL";
    /// <summary>
    /// Represents the SQL IF keyword.
    /// </summary>
    public const string IF = "IF";
    /// <summary>
    /// Represents the SQL EXISTS keyword.
    /// </summary>
    public const string EXISTS = "EXISTS";
    /// <summary>
    /// Represents the SQL COLUMN keyword.
    /// </summary>
    public const string COLUMN = "COLUMN";
    /// <summary>
    /// Represents the SQL VACUUM keyword.
    /// </summary>
    public const string VACUUM = "VACUUM";
    /// <summary>
    /// Represents the SQL CASCADE keyword.
    /// </summary>
    public const string CASCADE = "CASCADE";
    /// <summary>
    /// Represents the SQL RESTRICT keyword.
    /// </summary>
    public const string RESTRICT = "RESTRICT";

    /// <summary>
    /// Gets the complete set of currently recognized SQL keywords.
    /// </summary>
    public const string IS = "IS";
    /// <summary>
    /// Represents the SQL NOT_KEYWORD keyword.
    /// </summary>
    public const string NOT_KEYWORD = "NOT";
    /// <summary>
    /// Represents the SQL NULL keyword.
    /// </summary>
    public const string NULL = "NULL";

    /// <summary>
    /// Represents the SQL BEGIN keyword.
    /// </summary>
    public const string BEGIN = "BEGIN";
    /// <summary>
    /// Represents the SQL TRANSACTION keyword.
    /// </summary>
    public const string TRANSACTION = "TRANSACTION";
    /// <summary>
    /// Represents the SQL COMMIT keyword.
    /// </summary>
    public const string COMMIT = "COMMIT";
    /// <summary>
    /// Represents the SQL ROLLBACK keyword.
    /// </summary>
    public const string ROLLBACK = "ROLLBACK";
    /// <summary>
    /// Represents the SQL SAVEPOINT keyword.
    /// </summary>
    public const string SAVEPOINT = "SAVEPOINT";
    /// <summary>
    /// Represents the SQL RELEASE keyword.
    /// </summary>
    public const string RELEASE = "RELEASE";
    /// <summary>
    /// Represents the SQL TO keyword.
    /// </summary>
    public const string TO = "TO";
    /// <summary>
    /// Represents the SQL FOR keyword.
    /// </summary>
    public const string FOR = "FOR";

    /// <summary>
    /// Represents the SQL USING keyword.
    /// </summary>
    public const string USING = "USING";
    /// <summary>
    /// Represents the SQL USER keyword.
    /// </summary>
    public const string USER = "USER";
    /// <summary>
    /// Represents the SQL ROLE keyword.
    /// </summary>
    public const string ROLE = "ROLE";
    /// <summary>
    /// Represents the SQL GRANT keyword.
    /// </summary>
    public const string GRANT = "GRANT";
    /// <summary>
    /// Represents the SQL REVOKE keyword.
    /// </summary>
    public const string REVOKE = "REVOKE";
    /// <summary>
    /// Represents the SQL IDENTIFIED keyword.
    /// </summary>
    public const string IDENTIFIED = "IDENTIFIED";
    /// <summary>
    /// Represents the SQL LOGIN keyword.
    /// </summary>
    public const string LOGIN = "LOGIN";
    /// <summary>
    /// Represents the SQL LOGOUT keyword.
    /// </summary>
    public const string LOGOUT = "LOGOUT";
    /// <summary>
    /// Represents the SQL READ keyword.
    /// </summary>
    public const string READ = "READ";
    /// <summary>
    /// Represents the SQL WRITE keyword.
    /// </summary>
    public const string WRITE = "WRITE";
    /// <summary>
    /// Represents the SQL SCHEMA keyword.
    /// </summary>
    public const string SCHEMA = "SCHEMA";
    /// <summary>
    /// Represents the SQL SECURITY keyword.
    /// </summary>
    public const string SECURITY = "SECURITY";
    /// <summary>
    /// Represents the SQL PASSWORD keyword.
    /// </summary>
    public const string PASSWORD = "PASSWORD";
    /// <summary>
    /// Represents the SQL HNSW keyword.
    /// </summary>
    public const string HNSW = "HNSW";

    /// <summary>
    /// Gets all supported SQL keywords as a flat array for quick membership checks.
    /// </summary>
    public static readonly string[] All =
    [
        SELECT, FROM, WHERE, INSERT, INTO, VALUES,
        CREATE, TABLE, DROP, INDEX, ON, SHOW, DATABASES,
        TABLES, DESCRIBE, DELETE, UPDATE, SET, USE, GO,
        DATABASE, DEFAULT, PRIMARY, KEY, UNIQUE, REFERENCES, FOREIGN,
        INT, FLOAT, BIT, DATE, VARCHAR, VECTOR, AS, BY, GROUP, ORDER,
        HAVING, ASC, DESC, ALTER, ADD, MODIFY,
        JOIN, INNER, LEFT, RIGHT, FULL, OUTER, CROSS, DISTINCT,
        LIMIT, OFFSET, IN, BETWEEN, LIKE, UNION, ALL, IF, EXISTS, COLUMN,
        VACUUM, CASCADE, RESTRICT, IS, NOT_KEYWORD, NULL,
        BEGIN, TRANSACTION, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, TO,
        USING, GRANT, REVOKE, LOGIN, LOGOUT, HNSW
    ];
}

/// <summary>
/// Defines the punctuation tokens used by SQL parsing.
/// </summary>
public static class SqlPunctuation
{
    /// <summary>
    /// Represents the OpenParen character.
    /// </summary>
    public const char OpenParen = '(';
    /// <summary>
    /// Represents the CloseParen character.
    /// </summary>
    public const char CloseParen = ')';
    /// <summary>
    /// Represents the Comma character.
    /// </summary>
    public const char Comma = ',';
    /// <summary>
    /// Represents the Star character.
    /// </summary>
    public const char Star = '*';
    /// <summary>
    /// Represents the Dot character.
    /// </summary>
    public const char Dot = '.';

    /// <summary>
    /// Represents the SQL OpenParenToken keyword.
    /// </summary>
    public const string OpenParenToken = "(";
    /// <summary>
    /// Represents the SQL CloseParenToken keyword.
    /// </summary>
    public const string CloseParenToken = ")";
    /// <summary>
    /// Represents the SQL CommaToken keyword.
    /// </summary>
    public const string CommaToken = ",";
    /// <summary>
    /// Represents the SQL StarToken keyword.
    /// </summary>
    public const string StarToken = "*";
    /// <summary>
    /// Represents the SQL DotToken keyword.
    /// </summary>
    public const string DotToken = ".";
}

/// <summary>
/// Defines literal expressions that are treated specially by the parser.
/// </summary>
public static class SqlLiterals
{
    /// <summary>
    /// Represents a Boolean expression that always evaluates to true.
    /// </summary>
    public const string TrueExpression = "1=1";
}
