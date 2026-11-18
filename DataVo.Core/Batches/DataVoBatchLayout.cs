namespace DataVo.Core;

/// <summary>
/// Names a table and the columns, in order, that the positional values of a <see cref="DataVoWriteBatch"/>
/// operation carry.
/// </summary>
/// <remarks>
/// A layout must list every primary-key column of the table. An upsert sets only the listed columns: a row
/// that already exists keeps the values of the others, and a new row gets their defaults. Create a layout once
/// and reuse it for every operation with the same shape; layouts are immutable and safe to share.
/// </remarks>
/// <example>
/// <code>
/// var players = new DataVoBatchLayout("Players", ["Id", "Name", "Hp"]);
/// var playerHp = new DataVoBatchLayout("Players", ["Id", "Hp"]); // touches only Hp
/// </code>
/// </example>
public sealed class DataVoBatchLayout
{
    private readonly string[] _columns;

    /// <summary>Creates a layout.</summary>
    /// <param name="tableName">The table the operations write to.</param>
    /// <param name="columns">The columns the values carry, in value order; names are matched without regard to case.</param>
    /// <exception cref="ArgumentException">The table name is blank, no column is given, or a column is listed twice.</exception>
    public DataVoBatchLayout(string tableName, IReadOnlyList<string> columns)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNullOrWhiteSpace(tableName);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(columns);
        if (columns.Count == 0)
        {
            throw new ArgumentException("A batch layout needs at least one column.", nameof(columns));
        }

        _columns = [.. columns];
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (string column in _columns)
        {
            if (string.IsNullOrWhiteSpace(column) || !seen.Add(column))
            {
                throw new ArgumentException($"Batch layout column '{column}' is blank or listed twice.", nameof(columns));
            }
        }

        TableName = tableName;
    }

    /// <summary>Gets the table the operations write to.</summary>
    public string TableName { get; }

    /// <summary>Gets the columns the values carry, in value order.</summary>
    public IReadOnlyList<string> Columns => _columns;
}
