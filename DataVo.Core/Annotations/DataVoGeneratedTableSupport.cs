using System.ComponentModel;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Models.Catalog;

namespace DataVo.Core.Annotations;

/// <summary>
/// Runtime helpers called by code that <c>DataVo.Generators</c> emits for <see cref="DataVoTableAttribute"/>
/// models. Application code does not need to call these directly.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public static class DataVoGeneratedTableSupport
{
    /// <summary>Creates a model's table and indexes when missing, and verifies an existing table matches the model.</summary>
    /// <param name="context">The context whose current database owns the table.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="createTableSql">The statement that creates the table.</param>
    /// <param name="columns">Each model column and its SQL type.</param>
    /// <param name="indexedColumns">The columns that need a secondary index.</param>
    /// <exception cref="InvalidOperationException">No database is selected, a statement failed, or the table does not match.</exception>
    public static void EnsureTable(
        DataVoContext context,
        string tableName,
        string createTableSql,
        (string Name, string SqlType)[] columns,
        string[] indexedColumns)
    {
        string databaseName = context.Engine.Sessions.Get(context.SessionId)
            ?? throw new InvalidOperationException($"Select a database before ensuring table {tableName}.");

        if (!context.Engine.Catalog.TableExists(tableName, databaseName))
        {
            ThrowIfError(context.Execute(createTableSql)[^1], createTableSql);
        }

        VerifyColumns(context, tableName, databaseName, columns);
        foreach (string column in indexedColumns)
        {
            EnsureIndex(context, tableName, databaseName, column);
        }
    }

    /// <summary>Throws when a statement failed.</summary>
    /// <param name="result">The statement's result.</param>
    /// <param name="statement">Text identifying the statement in the error.</param>
    /// <exception cref="InvalidOperationException">The statement failed.</exception>
    public static void ThrowIfError(QueryResult result, string statement)
    {
        if (result.IsError)
        {
            throw new InvalidOperationException($"{statement} failed: {string.Join(" | ", result.Messages)}");
        }
    }

    /// <summary>Gets whether an UPDATE changed at least one row.</summary>
    /// <param name="result">The UPDATE's result.</param>
    /// <returns><see langword="true"/> when a row was changed.</returns>
    public static bool ChangedAnyRow(QueryResult result) =>
        !result.Messages.Exists(message => message.Equals("Rows affected: 0", StringComparison.Ordinal));

    /// <summary>Reads a required INT column.</summary>
    public static int ReadInt32(object? value, string column) => value switch
    {
        int number => number,
        null => throw NullColumn(column),
        _ => Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture),
    };

    /// <summary>Reads a nullable INT column.</summary>
    public static int? ReadNullableInt32(object? value, string column) => value is null ? null : ReadInt32(value, column);

    /// <summary>Reads a required FLOAT column.</summary>
    public static float ReadSingle(object? value, string column) => value switch
    {
        float number => number,
        null => throw NullColumn(column),
        _ => Convert.ToSingle(value, System.Globalization.CultureInfo.InvariantCulture),
    };

    /// <summary>Reads a nullable FLOAT column.</summary>
    public static float? ReadNullableSingle(object? value, string column) => value is null ? null : ReadSingle(value, column);

    /// <summary>Reads a required BIT column.</summary>
    public static bool ReadBoolean(object? value, string column) => value switch
    {
        bool flag => flag,
        null => throw NullColumn(column),
        _ => Convert.ToBoolean(value, System.Globalization.CultureInfo.InvariantCulture),
    };

    /// <summary>Reads a nullable BIT column.</summary>
    public static bool? ReadNullableBoolean(object? value, string column) => value is null ? null : ReadBoolean(value, column);

    /// <summary>Reads a required VARCHAR column.</summary>
    public static string ReadString(object? value, string column) => value as string ?? throw NullColumn(column);

    /// <summary>Reads a nullable VARCHAR column.</summary>
    public static string? ReadNullableString(object? value, string column) => value as string;

    /// <summary>Reads a required GUID column.</summary>
    public static Guid ReadGuid(object? value, string column) => value switch
    {
        Guid id => id,
        null => throw NullColumn(column),
        _ => Guid.Parse(value.ToString()!),
    };

    /// <summary>Reads a nullable GUID column.</summary>
    public static Guid? ReadNullableGuid(object? value, string column) => value is null ? null : ReadGuid(value, column);

    /// <summary>Reads a required DATE column.</summary>
    public static DataVoDate ReadDate(object? value, string column) => value switch
    {
        DataVoDate date => date,
        null => throw NullColumn(column),
        _ => throw new InvalidOperationException($"Column {column} does not hold a date."),
    };

    /// <summary>Reads a nullable DATE column.</summary>
    public static DataVoDate? ReadNullableDate(object? value, string column) => value is null ? null : ReadDate(value, column);

    private static void VerifyColumns(DataVoContext context, string tableName, string databaseName, (string Name, string SqlType)[] columns)
    {
        IReadOnlyList<Column> actual = context.Engine.Catalog.GetTableColumns(tableName, databaseName);
        foreach ((string name, string sqlType) in columns)
        {
            Column? match = actual.FirstOrDefault(column => column.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
            if (match is null)
            {
                throw new InvalidOperationException($"Table {tableName} has no column {name}, which the model requires.");
            }

            if (!match.Type.Equals(sqlType, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Column {tableName}.{name} is {match.Type}, but the model requires {sqlType}.");
            }
        }
    }

    private static void EnsureIndex(DataVoContext context, string tableName, string databaseName, string column)
    {
        bool exists = context.Engine.Catalog.GetTableIndexes(tableName, databaseName)
            .Any(index => index.AttributeNames.Count == 1 && index.AttributeNames[0].Equals(column, StringComparison.OrdinalIgnoreCase));
        if (!exists)
        {
            string sql = $"CREATE INDEX ix_{tableName}_{column} ON {tableName} ({column})";
            ThrowIfError(context.Execute(sql)[^1], sql);
        }
    }

    private static InvalidOperationException NullColumn(string column) =>
        new($"Column {column} is NULL, but the model's property cannot hold null.");
}
