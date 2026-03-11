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

    /// <summary>
    /// Initializes a new engine-scoped catalog facade.
    /// </summary>
    /// <param name="config">The engine configuration that determines catalog persistence behavior.</param>
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
    /// Returns the current schema version for a table.
    /// </summary>
    public int GetTableSchemaVersion(string tableName, string databaseName) =>
        _store.GetTableSchemaVersion(tableName, databaseName);
}