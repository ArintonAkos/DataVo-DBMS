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
    /// <param name="readRow">Builds a model from one result row.</param>
    public DataVoTableInfo(string tableName, string columnList, Func<Dictionary<string, object?>, T> readRow)
    {
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        ColumnList = columnList ?? throw new ArgumentNullException(nameof(columnList));
        ReadRow = readRow ?? throw new ArgumentNullException(nameof(readRow));
    }

    /// <summary>Gets the table the model is stored in.</summary>
    public string TableName { get; }

    internal string ColumnList { get; }

    internal Func<Dictionary<string, object?>, T> ReadRow { get; }

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
