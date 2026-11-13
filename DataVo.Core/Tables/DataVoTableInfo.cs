using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace DataVo.Core;

/// <summary>
/// What <c>DataVo.Generators</c> knows about one <c>[DataVoTable]</c> model: its table, its columns and how to
/// build it from a row. Generated code creates one per model; application code reaches it through the generated
/// table accessor instead.
/// </summary>
/// <typeparam name="T">The model type.</typeparam>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class DataVoTableInfo<T>
{
    private static DataVoTableInfo<T>? s_registered;

    private readonly ConditionalWeakTable<DataVoContext, DataVoTable<T>> _tables = new();

    /// <summary>Describes a model's table.</summary>
    /// <param name="tableName">The table the model is stored in.</param>
    /// <param name="columnList">The model's columns as a comma-separated SELECT list.</param>
    /// <param name="readRow">Builds a model from one row of a statement's result.</param>
    /// <param name="readStreamedRow">Builds a model from one row of a streamed scan.</param>
    public DataVoTableInfo(
        string tableName,
        string columnList,
        Func<Dictionary<string, object?>, T> readRow,
        Func<DataVoRow, T> readStreamedRow)
        : this(tableName, columnList, readRow, readStreamedRow, [])
    {
    }

    /// <summary>Describes a model's table, including the columns that hold 3D positions.</summary>
    /// <param name="tableName">The table the model is stored in.</param>
    /// <param name="columnList">The model's columns as a comma-separated SELECT list.</param>
    /// <param name="readRow">Builds a model from one row of a statement's result.</param>
    /// <param name="readStreamedRow">Builds a model from one row of a streamed scan.</param>
    /// <param name="spatialColumns">The model's 3D position columns, used by the spatial queries.</param>
    public DataVoTableInfo(
        string tableName,
        string columnList,
        Func<Dictionary<string, object?>, T> readRow,
        Func<DataVoRow, T> readStreamedRow,
        DataVoSpatialColumn[] spatialColumns)
    {
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        ColumnList = columnList ?? throw new ArgumentNullException(nameof(columnList));
        ReadRow = readRow ?? throw new ArgumentNullException(nameof(readRow));
        ReadStreamedRow = readStreamedRow ?? throw new ArgumentNullException(nameof(readStreamedRow));
        SpatialColumns = spatialColumns ?? throw new ArgumentNullException(nameof(spatialColumns));
        ColumnNames = [];
        ColumnTypes = [];
    }

    /// <summary>
    /// Creates table metadata for a model without 3D position columns that can also be read straight from
    /// packed in-memory rows.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="columnList">The comma-separated column list, in the order the readers expect.</param>
    /// <param name="readRow">Builds a model from one row of a statement's result.</param>
    /// <param name="readStreamedRow">Builds a model from one row of a streamed scan.</param>
    /// <param name="readPackedRow">Builds a model from a row read in place; it numbers columns as <paramref name="columnList"/> orders them.</param>
    /// <param name="columnTypes">The SQL type of each column of <paramref name="columnList"/>.</param>
    /// <param name="keyColumn">The primary-key column.</param>
    public DataVoTableInfo(
        string tableName,
        string columnList,
        Func<Dictionary<string, object?>, T> readRow,
        Func<DataVoRow, T> readStreamedRow,
        DataVoPackedReader<T> readPackedRow,
        string[] columnTypes,
        string keyColumn)
        : this(tableName, columnList, readRow, readStreamedRow, [], readPackedRow, columnTypes, keyColumn)
    {
    }

    /// <summary>Creates table metadata for a model that can also be read straight from packed in-memory rows.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="columnList">The comma-separated column list, in the order the readers expect.</param>
    /// <param name="readRow">Builds a model from one row of a statement's result.</param>
    /// <param name="readStreamedRow">Builds a model from one row of a streamed scan.</param>
    /// <param name="spatialColumns">The model's 3D position columns, each with the name of its spatial index or null.</param>
    /// <param name="readPackedRow">Builds a model from a row read in place; it numbers columns as <paramref name="columnList"/> orders them.</param>
    /// <param name="columnTypes">The SQL type of each column of <paramref name="columnList"/>.</param>
    /// <param name="keyColumn">The primary-key column.</param>
    public DataVoTableInfo(
        string tableName,
        string columnList,
        Func<Dictionary<string, object?>, T> readRow,
        Func<DataVoRow, T> readStreamedRow,
        DataVoSpatialColumn[] spatialColumns,
        DataVoPackedReader<T> readPackedRow,
        string[] columnTypes,
        string keyColumn)
        : this(tableName, columnList, readRow, readStreamedRow, spatialColumns)
    {
        ReadPackedRow = readPackedRow ?? throw new ArgumentNullException(nameof(readPackedRow));
        ColumnTypes = columnTypes ?? throw new ArgumentNullException(nameof(columnTypes));
        KeyColumn = keyColumn ?? throw new ArgumentNullException(nameof(keyColumn));
        ColumnNames = [.. columnList.Split(',').Select(static name => name.Trim())];
        if (ColumnNames.Length != ColumnTypes.Length)
        {
            throw new ArgumentException("There must be one column type for each column of the column list.", nameof(columnTypes));
        }
    }

    /// <summary>Gets the table the model is stored in.</summary>
    public string TableName { get; }

    internal string ColumnList { get; }

    internal Func<Dictionary<string, object?>, T> ReadRow { get; }

    internal Func<DataVoRow, T> ReadStreamedRow { get; }

    internal DataVoSpatialColumn[] SpatialColumns { get; }

    /// <summary>Builds a model from a row read in place; null for a model generated without one.</summary>
    internal DataVoPackedReader<T>? ReadPackedRow { get; }

    /// <summary>The model's columns, in the order <see cref="ReadPackedRow"/> numbers them.</summary>
    internal string[] ColumnNames { get; }

    /// <summary>The SQL type of each of <see cref="ColumnNames"/>.</summary>
    internal string[] ColumnTypes { get; }

    /// <summary>The primary-key column; null for a model generated without a packed reader.</summary>
    internal string? KeyColumn { get; }

    /// <summary>Gets the model's description, which its generated code registered.</summary>
    internal static DataVoTableInfo<T> Registered => s_registered ?? throw new InvalidOperationException(
        $"{typeof(T).Name} has no registered DataVo table. Mark it [DataVoTable] and reference DataVo.Generators; " +
        "where module initializers are unavailable, call the model's generated Ensure…Table method before querying it.");

    /// <summary>Makes a model available to <see cref="DataVoContext.Table{T}"/>. Generated code calls this once per model.</summary>
    /// <param name="info">The model's description.</param>
    public static void Register(DataVoTableInfo<T> info) =>
        Volatile.Write(ref s_registered, info ?? throw new ArgumentNullException(nameof(info)));

    /// <summary>Gets the table for a context. The same instance is returned until the context changes session.</summary>
    /// <param name="context">The context to query.</param>
    /// <returns>The context's table.</returns>
    public DataVoTable<T> For(DataVoContext context)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (_tables.TryGetValue(context, out DataVoTable<T>? cached) && cached.Session == context.SessionId)
        {
            return cached;
        }

        var table = new DataVoTable<T>(context, this);
        _tables.AddOrUpdate(context, table);
        return table;
    }
}

/// <summary>
/// A model column that holds a 3D position (<see cref="System.Numerics.Vector3"/>, stored as <c>VECTOR(3)</c>).
/// Generated code describes one per position property.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class DataVoSpatialColumn
{
    /// <summary>Describes a position column.</summary>
    /// <param name="column">The column name.</param>
    /// <param name="indexName">The Euclidean HNSW index on the column, or <see langword="null"/> when it has none.</param>
    public DataVoSpatialColumn(string column, string? indexName)
    {
        Column = column ?? throw new ArgumentNullException(nameof(column));
        IndexName = indexName;
    }

    /// <summary>Gets the column name.</summary>
    public string Column { get; }

    /// <summary>Gets the Euclidean HNSW index on the column, or <see langword="null"/> when it has none.</summary>
    public string? IndexName { get; }
}
