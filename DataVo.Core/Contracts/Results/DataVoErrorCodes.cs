namespace DataVo.Core.Contracts.Results;

/// <summary>
/// Stable error codes reported in <see cref="QueryResult.ErrorCode"/> and on DataVo exceptions.
/// A code is never reused for a different meaning; messages may change, codes do not.
/// </summary>
public static class DataVoErrorCodes
{
    /// <summary>The statement text is not valid SQL (lexer or parser error). Line and column are set.</summary>
    public const string Syntax = "DV1001_SYNTAX";

    /// <summary>The input contains no statement.</summary>
    public const string EmptyStatement = "DV1002_EMPTY_STATEMENT";

    /// <summary>The statement needs a current database; run <c>USE</c> first.</summary>
    public const string NoDatabaseSelected = "DV2001_NO_DATABASE_SELECTED";

    /// <summary>The named database does not exist.</summary>
    public const string UnknownDatabase = "DV2002_UNKNOWN_DATABASE";

    /// <summary>The named table does not exist in the current database.</summary>
    public const string UnknownTable = "DV2003_UNKNOWN_TABLE";

    /// <summary>The named column does not exist in the referenced table.</summary>
    public const string UnknownColumn = "DV2004_UNKNOWN_COLUMN";

    /// <summary>The named index does not exist.</summary>
    public const string UnknownIndex = "DV2005_UNKNOWN_INDEX";

    /// <summary>An unqualified column name matches columns of several joined tables; qualify it.</summary>
    public const string AmbiguousColumn = "DV2006_AMBIGUOUS_COLUMN";

    /// <summary>A qualified name refers to a table or alias that is not part of the query.</summary>
    public const string UnknownTableAlias = "DV2007_UNKNOWN_TABLE_ALIAS";

    /// <summary>A database with that name already exists.</summary>
    public const string DatabaseExists = "DV2101_DATABASE_EXISTS";

    /// <summary>A table with that name already exists.</summary>
    public const string TableExists = "DV2102_TABLE_EXISTS";

    /// <summary>An index with that name already exists on the table.</summary>
    public const string IndexExists = "DV2103_INDEX_EXISTS";

    /// <summary>Another name-resolution or schema-binding failure.</summary>
    public const string Binding = "DV2900_BINDING";

    /// <summary>Another catalog (schema metadata) failure.</summary>
    public const string Catalog = "DV2901_CATALOG";

    /// <summary>Another index failure.</summary>
    public const string Index = "DV2902_INDEX";

    /// <summary>A row would repeat an existing primary key.</summary>
    public const string PrimaryKeyViolation = "DV3001_PRIMARY_KEY_VIOLATION";

    /// <summary>A primary key value is NULL.</summary>
    public const string NullPrimaryKey = "DV3002_NULL_PRIMARY_KEY";

    /// <summary>A row would repeat a value of a UNIQUE column.</summary>
    public const string UniqueViolation = "DV3003_UNIQUE_VIOLATION";

    /// <summary>A row references a missing parent row, or a referenced parent row would be removed.</summary>
    public const string ForeignKeyViolation = "DV3004_FOREIGN_KEY_VIOLATION";

    /// <summary>A NOT NULL column would hold NULL.</summary>
    public const string NotNullViolation = "DV3005_NOT_NULL_VIOLATION";

    /// <summary>A value does not match the column or operand type.</summary>
    public const string TypeMismatch = "DV4001_TYPE_MISMATCH";

    /// <summary>A string is longer than the column's declared length.</summary>
    public const string ValueTooLong = "DV4002_VALUE_TOO_LONG";

    /// <summary>The number of values does not match the number of columns.</summary>
    public const string ValueCountMismatch = "DV4003_VALUE_COUNT_MISMATCH";

    /// <summary>Integer division by zero.</summary>
    public const string DivisionByZero = "DV4004_DIVISION_BY_ZERO";

    /// <summary>Another expression-evaluation failure.</summary>
    public const string Evaluation = "DV4900_EVALUATION";

    /// <summary><c>COMMIT</c> or <c>ROLLBACK</c> without an active transaction.</summary>
    public const string NoActiveTransaction = "DV5001_NO_ACTIVE_TRANSACTION";

    /// <summary><c>BEGIN</c> while a transaction is already active in the session.</summary>
    public const string TransactionAlreadyActive = "DV5002_TRANSACTION_ALREADY_ACTIVE";

    /// <summary>The statement was chosen as a deadlock victim; retry it.</summary>
    public const string Deadlock = "DV5003_DEADLOCK";

    /// <summary>The session is not allowed to perform the statement.</summary>
    public const string PermissionDenied = "DV6001_PERMISSION_DENIED";

    /// <summary>The data directory is held by another process or opened with incompatible settings.</summary>
    public const string DatabaseInUse = "DV7001_DATABASE_IN_USE";

    /// <summary>A storage-level I/O failure.</summary>
    public const string Storage = "DV7900_STORAGE";

    /// <summary>A failure DataVo reported without a more specific code.</summary>
    public const string General = "DV9000_ERROR";

    /// <summary>An unexpected internal failure (a DataVo bug). The message names the underlying error.</summary>
    public const string Internal = "DV9001_INTERNAL";
}
