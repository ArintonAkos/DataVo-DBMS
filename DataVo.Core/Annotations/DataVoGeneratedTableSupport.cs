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
        string[] indexedColumns) =>
        EnsureTable(context, tableName, createTableSql, columns, indexedColumns, []);

    /// <summary>Creates a model's table and indexes when missing, and verifies an existing table matches the model.</summary>
    /// <param name="context">The context whose current database owns the table.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="createTableSql">The statement that creates the table.</param>
    /// <param name="columns">Each model column and its SQL type.</param>
    /// <param name="indexedColumns">The columns that need a secondary index.</param>
    /// <param name="spatialIndexes">The position columns that need a Euclidean HNSW index, and each index's name.</param>
    /// <exception cref="InvalidOperationException">No database is selected, a statement failed, or the table does not match.</exception>
    public static void EnsureTable(
        DataVoContext context,
        string tableName,
        string createTableSql,
        (string Name, string SqlType)[] columns,
        string[] indexedColumns,
        (string Column, string IndexName)[] spatialIndexes)
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

        foreach ((string column, string indexName) in spatialIndexes)
        {
            EnsureSpatialIndex(context, tableName, databaseName, column, indexName);
        }
    }

    /// <summary>
    /// Creates the table's columnar index over the given columns if it is missing, and replaces it when it
    /// covers other columns than the model now marks.
    /// </summary>
    /// <param name="context">The context whose current database holds the table.</param>
    /// <param name="tableName">The table.</param>
    /// <param name="columns">The columns the model marks <c>[Columnar]</c>.</param>
    /// <exception cref="InvalidOperationException">No database is selected, or the index could not be created.</exception>
    public static void EnsureColumnarIndex(DataVoContext context, string tableName, string[] columns)
    {
        string databaseName = context.Engine.Sessions.Get(context.SessionId)
            ?? throw new InvalidOperationException($"Select a database before ensuring table {tableName}.");
        string indexName = $"cx_{tableName}";
        foreach (IndexFile index in context.Engine.Catalog.GetColumnarIndexesReadOnly(tableName, databaseName))
        {
            if (!index.IndexFileName.Equals(indexName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (index.AttributeNames.Count == columns.Length
                && columns.All(column => index.AttributeNames.Contains(column, StringComparer.OrdinalIgnoreCase)))
            {
                return;
            }

            string drop = $"DROP INDEX {index.IndexFileName} ON {tableName}";
            ThrowIfError(context.Execute(drop)[^1], drop);
            break;
        }

        string sql = $"CREATE INDEX {indexName} ON {tableName} ({string.Join(", ", columns)}) USING COLUMNAR";
        ThrowIfError(context.Execute(sql)[^1], sql);
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

    /// <summary>Reads a required BIGINT column.</summary>
    public static long ReadInt64(object? value, string column) => value switch
    {
        long number => number,
        null => throw NullColumn(column),
        _ => Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture),
    };

    /// <summary>Reads a nullable BIGINT column.</summary>
    public static long? ReadNullableInt64(object? value, string column) => value is null ? null : ReadInt64(value, column);

    /// <summary>Reads a required DOUBLE column.</summary>
    public static double ReadDouble(object? value, string column) => value switch
    {
        double number => number,
        null => throw NullColumn(column),
        _ => Convert.ToDouble(value, System.Globalization.CultureInfo.InvariantCulture),
    };

    /// <summary>Reads a nullable DOUBLE column.</summary>
    public static double? ReadNullableDouble(object? value, string column) => value is null ? null : ReadDouble(value, column);

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

            if (!SameType(match, sqlType))
            {
                throw new InvalidOperationException($"Column {tableName}.{name} is {match.Type}, but the model requires {sqlType}.");
            }
        }
    }

    // A declared VECTOR(3) is stored as type VECTOR with length 3.
    private static bool SameType(Column column, string sqlType)
    {
        int parenthesis = sqlType.IndexOf('(');
        if (parenthesis < 0)
        {
            return column.Type.Equals(sqlType, StringComparison.OrdinalIgnoreCase);
        }

        return column.Type.Equals(sqlType[..parenthesis], StringComparison.OrdinalIgnoreCase)
            && int.TryParse(sqlType[(parenthesis + 1)..^1], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out int length)
            && column.Length == length;
    }

    private static void EnsureSpatialIndex(DataVoContext context, string tableName, string databaseName, string column, string indexName)
    {
        IndexFile? existing = context.Engine.Catalog.GetTableIndexes(tableName, databaseName)
            .FirstOrDefault(index => index.IndexFileName.Equals(indexName, StringComparison.OrdinalIgnoreCase));
        if (existing is not null)
        {
            bool matches = existing.IndexKind.Equals("HNSW", StringComparison.OrdinalIgnoreCase)
                && existing.EffectiveMetric == IndexFile.EuclideanMetric
                && existing.AttributeNames.Count == 1
                && existing.AttributeNames[0].Equals(column, StringComparison.OrdinalIgnoreCase);
            if (!matches)
            {
                throw new InvalidOperationException(
                    $"Index {indexName} on {tableName} exists but is not a Euclidean HNSW index on {column}, which [SpatialIndex] requires. Drop it and ensure the table again.");
            }

            return;
        }

        string sql = $"CREATE INDEX {indexName} ON {tableName} ({column}) USING HNSW WITH (metric = 'euclidean')";
        ThrowIfError(context.Execute(sql)[^1], sql);
    }

    /// <summary>Reads a required VECTOR(3) column as a 3D point.</summary>
    public static System.Numerics.Vector3 ReadVector3(object? value, string column) => value switch
    {
        float[] { Length: 3 } xyz => new System.Numerics.Vector3(xyz[0], xyz[1], xyz[2]),
        null => throw NullColumn(column),
        float[] other => throw new InvalidOperationException($"Column {column} holds {other.Length} values, but a 3D position needs 3."),
        _ => throw new InvalidOperationException($"Column {column} does not hold a 3D position."),
    };

    /// <summary>Reads a nullable VECTOR(3) column as a 3D point.</summary>
    public static System.Numerics.Vector3? ReadNullableVector3(object? value, string column) =>
        value is null ? null : ReadVector3(value, column);

    /// <summary>Binds a 3D point to a VECTOR(3) parameter.</summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The point.</param>
    /// <returns>The parameter.</returns>
    public static DataVoQueryParameter Vector3Parameter(string name, System.Numerics.Vector3 value) =>
        DataVoQueryParameter.FromObject(name, new[] { value.X, value.Y, value.Z });

    /// <summary>Binds a nullable 3D point to a VECTOR(3) parameter.</summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The point, or null.</param>
    /// <returns>The parameter.</returns>
    public static DataVoQueryParameter Vector3Parameter(string name, System.Numerics.Vector3? value) =>
        value is { } point ? Vector3Parameter(name, point) : DataVoQueryParameter.Null(name);

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

    /// <summary>The error for a NULL read into a property that cannot hold one.</summary>
    internal static InvalidOperationException NullColumn(string column) =>
        new($"Column {column} is NULL, but the model's property cannot hold null.");
}
