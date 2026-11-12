using System.Collections.Concurrent;
using DataVo.Core.Models.Catalog;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Runtime;

/// <summary>
/// Engine-level catalog facade used by the execution pipeline.
/// </summary>
/// <remarks>
/// This currently delegates to the legacy process-wide <see cref="Catalog"/> implementation.
/// Keeping the dependency behind an engine-owned facade makes the later migration to a truly
/// instance-scoped catalog substantially smaller.
/// </remarks>
public sealed class EngineCatalog
{
    private readonly CatalogStore _store;

    // Per-table validation metadata cached by schema version. The hot insert path fetches this once
    // instead of re-walking the XML catalog (5 lookups) and rebuilding LINQ structures per row; a DDL
    // bumps the table's schema version, so a stale entry can never be served.
    private readonly ConcurrentDictionary<string, (int Version, TableValidationMetadata Metadata)> _validationMetadata = new();

    /// <summary>
    /// Initializes a new engine-scoped catalog facade.
    /// </summary>
    /// <param name="config">The engine configuration that determines catalog persistence behavior.</param>
    /// <summary>Finds the spelling a database was created with, or <see langword="null"/> when it does not exist.</summary>
    /// <param name="databaseName">The database name as written by the caller.</param>
    /// <returns>The stored spelling, or <see langword="null"/>.</returns>
    /// <example>
    /// <code>
    /// string stored = catalog.FindDatabaseName("SHOP") ?? "SHOP";
    /// </code>
    /// </example>
    public string? FindDatabaseName(string databaseName) => _store.FindDatabaseName(databaseName);

    /// <summary>Finds the spelling a table was created with, or <see langword="null"/> when it does not exist.</summary>
    /// <param name="tableName">The table name as written by the caller.</param>
    /// <param name="databaseName">The database that owns the table.</param>
    /// <returns>The stored spelling, or <see langword="null"/>.</returns>
    /// <example>
    /// <code>
    /// string stored = catalog.FindTableName("items", "Shop") ?? "items";
    /// </code>
    /// </example>
    public string? FindTableName(string tableName, string databaseName) => _store.FindTableName(tableName, databaseName);

    public EngineCatalog(DataVoConfig config)
    {
        _store = new CatalogStore(config);
    }

    /// <summary>
    /// Determines whether a database exists.
    /// </summary>
    public bool DatabaseExists(string databaseName) => _store.DatabaseExists(databaseName);

    /// <summary>
    /// Determines whether a table exists within a database.
    /// </summary>
    public bool TableExists(string tableName, string databaseName) => _store.TableExists(tableName, databaseName);

    /// <summary>
    /// Creates a database entry in the catalog.
    /// </summary>
    public void CreateDatabase(Database database) => _store.CreateDatabase(database);

    /// <summary>
    /// Removes a database from the catalog.
    /// </summary>
    public void DropDatabase(string databaseName) => _store.DropDatabase(databaseName);

    /// <summary>
    /// Creates a table entry in the catalog.
    /// </summary>
    public void CreateTable(Table table, string databaseName) => _store.CreateTable(table, databaseName);

    /// <summary>
    /// Adds a column to an existing table definition.
    /// </summary>
    public void AddColumn(string tableName, string databaseName, Field field) =>
        _store.AddColumn(tableName, databaseName, field);

    /// <summary>
    /// Drops a column from an existing table definition.
    /// </summary>
    public void DropColumn(string tableName, string databaseName, string columnName) =>
        _store.DropColumn(tableName, databaseName, columnName);

    /// <summary>
    /// Modifies an existing column definition.
    /// </summary>
    public void ModifyColumn(string tableName, string databaseName, Field field) =>
        _store.ModifyColumn(tableName, databaseName, field);

    /// <summary>
    /// Drops a table from the catalog.
    /// </summary>
    public void DropTable(string tableName, string databaseName) => _store.DropTable(tableName, databaseName);

    /// <summary>
    /// Creates an index entry for a table.
    /// </summary>
    public void CreateIndex(IndexFile indexFile, string tableName, string databaseName) =>
        _store.CreateIndex(indexFile, tableName, databaseName);

    /// <summary>
    /// Drops an index entry from a table.
    /// </summary>
    public void DropIndex(string indexName, string tableName, string databaseName) =>
        _store.DropIndex(indexName, tableName, databaseName);

    /// <summary>
    /// Returns all database names.
    /// </summary>
    public List<string> GetDatabases() => _store.GetDatabases();

    /// <summary>
    /// Returns all table names for a database.
    /// </summary>
    public List<string> GetTables(string databaseName) => _store.GetTables(databaseName);

    /// <summary>
    /// Returns the primary key columns for a table.
    /// </summary>
    /// <summary>Whether the table stores its rows under its primary key (see <see cref="Table.Clustered"/>).</summary>
    public bool IsTableClustered(string tableName, string databaseName) => _store.IsTableClustered(tableName, databaseName);

    public List<string> GetTablePrimaryKeys(string tableName, string databaseName) =>
        _store.GetTablePrimaryKeys(tableName, databaseName);

    /// <summary>
    /// Returns the foreign keys declared on a table.
    /// </summary>
    public List<ForeignKey> GetTableForeignKeys(string tableName, string databaseName) =>
        _store.GetTableForeignKeys(tableName, databaseName);

    /// <summary>
    /// Returns child foreign keys that reference a parent table.
    /// </summary>
    public List<(string ChildTable, string ChildColumn, string ParentColumn, string OnDeleteAction)> GetChildForeignKeys(
        string parentTableName,
        string databaseName) => _store.GetChildForeignKeys(parentTableName, databaseName);

    /// <summary>
    /// Returns the unique key columns for a table.
    /// </summary>
    public List<string> GetTableUniqueKeys(string tableName, string databaseName) =>
        _store.GetTableUniqueKeys(tableName, databaseName);

    /// <summary>
    /// Returns the index metadata registered for a table.
    /// </summary>
    public List<IndexFile> GetTableIndexes(string tableName, string databaseName) =>
        _store.GetTableIndexes(tableName, databaseName);

    /// <summary>
    /// Returns the column metadata registered for a table.
    /// </summary>
    public List<Column> GetTableColumns(string tableName, string databaseName) =>
        _store.GetTableColumns(tableName, databaseName);

    /// <summary>
    /// Shared, cached column metadata for read-only use on hot paths. Callers must not modify the
    /// returned objects; use <see cref="GetTableColumns"/> for a private copy.
    /// </summary>
    internal IReadOnlyList<Column> GetTableColumnsReadOnly(string tableName, string databaseName) =>
        _store.GetTableColumnsReadOnly(tableName, databaseName);

    /// <summary>
    /// Shared, cached index metadata for read-only use on hot paths (see <see cref="GetTableColumnsReadOnly"/>).
    /// </summary>
    internal IReadOnlyList<IndexFile> GetTableIndexesReadOnly(string tableName, string databaseName) =>
        _store.GetTableIndexesReadOnly(tableName, databaseName);

    /// <summary>The table's columnar indexes, which the other index lookups leave out: they hold no keys.</summary>
    internal IReadOnlyList<IndexFile> GetColumnarIndexesReadOnly(string tableName, string databaseName) =>
        _store.GetColumnarIndexesReadOnly(tableName, databaseName);

    /// <summary>
    /// Returns a single column definition.
    /// </summary>
    public Column GetTableColumn(string tableName, string databaseName, string columnName) =>
        _store.GetTableColumn(tableName, databaseName, columnName);

    /// <summary>
    /// Returns the type of a single column.
    /// </summary>
    public string GetTableColumnType(string tableName, string databaseName, string columnName) =>
        _store.GetTableColumnType(tableName, databaseName, columnName);

    /// <summary>
    /// Returns a map of indexed columns to index file names.
    /// </summary>
    public Dictionary<string, string> GetTableIndexedColumns(string tableName, string databaseName) =>
        _store.GetTableIndexedColumns(tableName, databaseName);

    /// <summary>
    /// Serializes the current catalog state.
    /// </summary>
    public string ExportState() => _store.ExportState();

    /// <summary>
    /// Loads a serialized catalog state.
    /// </summary>
    public void LoadState(string? xml) => _store.LoadState(xml);

    /// <summary>
    /// Returns the current schema version for a table.
    /// </summary>
    public int GetTableSchemaVersion(string tableName, string databaseName) =>
        _store.GetTableSchemaVersion(tableName, databaseName);

    /// <summary>
    /// Returns the table's insert-validation metadata (keys, indexes, columns), computed once and cached
    /// by schema version. Lets the insert path avoid re-reading the catalog and rebuilding LINQ
    /// structures per row; the schema-version key invalidates automatically on any DDL.
    /// </summary>
    internal TableValidationMetadata GetTableValidationMetadata(string tableName, string databaseName)
    {
        int version = _store.GetTableSchemaVersion(tableName, databaseName);
        string key = databaseName + " " + tableName;

        if (_validationMetadata.TryGetValue(key, out (int Version, TableValidationMetadata Metadata) entry)
            && entry.Version == version)
        {
            return entry.Metadata;
        }

        List<Column> columns = _store.GetTableColumns(tableName, databaseName);
        var columnNames = new HashSet<string>(columns.Count, StringComparer.OrdinalIgnoreCase);
        foreach (Column column in columns)
        {
            columnNames.Add(column.Name);
        }

        // Mirror the legacy GroupBy(...).ToDictionary(g => g.First()): first foreign key per attribute wins.
        var foreignKeysByAttribute = new Dictionary<string, ForeignKey>(StringComparer.OrdinalIgnoreCase);
        foreach (ForeignKey foreignKey in _store.GetTableForeignKeys(tableName, databaseName))
        {
            foreignKeysByAttribute.TryAdd(foreignKey.AttributeName, foreignKey);
        }

        var metadata = new TableValidationMetadata(
            _store.GetTablePrimaryKeys(tableName, databaseName),
            _store.GetTableUniqueKeys(tableName, databaseName),
            foreignKeysByAttribute,
            _store.GetTableIndexes(tableName, databaseName),
            columns,
            columnNames);

        _validationMetadata[key] = (version, metadata);
        return metadata;
    }
}

/// <summary>
/// Immutable per-table metadata needed to validate and index an inserted row, cached by schema version.
/// </summary>
internal sealed record TableValidationMetadata(
    IReadOnlyList<string> PrimaryKeys,
    IReadOnlyList<string> UniqueKeys,
    IReadOnlyDictionary<string, ForeignKey> ForeignKeysByAttribute,
    IReadOnlyList<IndexFile> Indexes,
    IReadOnlyList<Column> Columns,
    HashSet<string> ColumnNames);