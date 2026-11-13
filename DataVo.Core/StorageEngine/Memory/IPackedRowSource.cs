namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// A storage engine whose typed rows are packed records that a reader may take values from in place, rather
/// than have them decoded into cells first.
/// </summary>
internal interface IPackedRowSource
{
    /// <summary>A table of the engine, to keep and read from again; null when there is no such table.</summary>
    IPackedTable? TableOf(string databaseName, string tableName);
}

/// <summary>
/// One table of a packed-row engine, held by a reader so it does not look the table up by name for every row.
/// </summary>
internal interface IPackedTable
{
    /// <summary>
    /// The table's rows as they stand now, or null once the table has been dropped or replaced (by DROP, by a
    /// schema change, which recreates it, or by restoring a snapshot). A reader that gets null looks the
    /// table up again; one that got rows and has read from them asks once more afterwards, and discards what
    /// it read if the answer is now null.
    /// </summary>
    TableRows? CurrentRows { get; }
}
