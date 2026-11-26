using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;

namespace DataVo.Core.Parser.DDL;

/// <summary>
/// The rewrite shared by <c>ALTER TABLE</c> and <c>VACUUM</c>: both give a table's rows new ids, so everything
/// keyed by row id (its indexes of every kind, its columnar index and its MVCC version entries) is rebuilt or
/// discarded afterwards. The caller holds the table's exclusive write lock.
/// </summary>
internal static class TableRewrite
{
    /// <summary>
    /// Replaces a table's rows with <paramref name="rows"/> under the table's current catalog schema, then
    /// rebuilds what depends on row ids.
    /// </summary>
    /// <param name="engine">The owning engine.</param>
    /// <param name="databaseName">The database holding the table.</param>
    /// <param name="tableName">The table to rewrite.</param>
    /// <param name="rows">The rows to store, already shaped for the table's current columns.</param>
    /// <param name="indexes">The table's key and vector indexes, read from the catalog before the rewrite.</param>
    public static void ReplaceRows(
        DataVoEngine engine,
        string databaseName,
        string tableName,
        List<Dictionary<string, object?>> rows,
        IReadOnlyList<IndexFile> indexes)
    {
        engine.StorageContext.DropTable(tableName, databaseName);
        engine.StorageContext.InsertIntoTable(rows, tableName, databaseName);
        RebuildAfterRenumber(engine, databaseName, tableName, indexes);
    }

    /// <summary>
    /// Rebuilds what depends on row ids once a table's rows were renumbered: every index is recreated as its
    /// own kind (HNSW, FLAT, BTREE) from the stored rows, the columnar index is invalidated, and MVCC version
    /// entries (which name the old ids) are discarded.
    /// </summary>
    /// <param name="engine">The owning engine.</param>
    /// <param name="databaseName">The database holding the table.</param>
    /// <param name="tableName">The renumbered table.</param>
    /// <param name="indexes">The table's key and vector indexes.</param>
    public static void RebuildAfterRenumber(DataVoEngine engine, string databaseName, string tableName, IReadOnlyList<IndexFile> indexes)
    {
        // Version entries and retained images are keyed by the old ids, which now name other rows.
        engine.VersionStorageManager.ClearTableVersions(databaseName, tableName);

        // Dropping also deletes a vector index's persisted graph snapshot, which names the old ids.
        foreach (IndexFile index in indexes)
        {
            engine.IndexManager.DropIndex(index.IndexFileName, tableName, databaseName);
        }

        engine.RebuildIndexesFromRows(databaseName, tableName, indexes);
        InvalidateColumnar(engine, databaseName, tableName);
    }

    /// <summary>
    /// Whether a column is part of any index on the table, columnar indexes included. Names compare
    /// case-insensitively, as the catalog resolves them.
    /// </summary>
    public static bool IsIndexedColumn(EngineCatalog catalog, string databaseName, string tableName, string columnName)
    {
        return ContainsColumn(catalog.GetTableIndexes(tableName, databaseName), columnName)
            || ContainsColumn(catalog.GetColumnarIndexesReadOnly(tableName, databaseName), columnName);
    }

    /// <summary>The catalog's spelling of an existing column, whatever case the statement used.</summary>
    public static string StoredColumnName(EngineCatalog catalog, string databaseName, string tableName, string columnName) =>
        catalog.GetTableColumns(tableName, databaseName).First(column => SameColumn(column.Name, columnName)).Name;

    /// <summary>Whether two column names are the same column.</summary>
    public static bool SameColumn(string left, string right) =>
        string.Equals(left, right, StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether a key list (primary or unique keys) holds a column.</summary>
    public static bool ContainsColumn(IEnumerable<string> columns, string columnName) =>
        columns.Any(column => SameColumn(column, columnName));

    private static bool ContainsColumn(IEnumerable<IndexFile> indexes, string columnName) =>
        indexes.Any(index => ContainsColumn(index.AttributeNames, columnName));

    // A columnar index stores rows by position; asking for a rebuild makes the next query that uses it build
    // it again from the renumbered rows (a store that dropped it with the table builds it afresh anyway).
    private static void InvalidateColumnar(DataVoEngine engine, string databaseName, string tableName)
    {
        engine.StorageContext.ColumnarStore?.Columnar(databaseName, tableName)?.RequestRebuild();
    }
}
