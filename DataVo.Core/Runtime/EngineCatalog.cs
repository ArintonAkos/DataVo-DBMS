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

    public EngineCatalog(DataVoConfig config)
    {
        _store = new CatalogStore(config);
    }

    public bool DatabaseExists(string databaseName) => _store.DatabaseExists(databaseName);

    public bool TableExists(string tableName, string databaseName) => _store.TableExists(tableName, databaseName);

    public void CreateDatabase(Database database) => _store.CreateDatabase(database);

    public void DropDatabase(string databaseName) => _store.DropDatabase(databaseName);

    public void CreateTable(Table table, string databaseName) => _store.CreateTable(table, databaseName);

    public void DropTable(string tableName, string databaseName) => _store.DropTable(tableName, databaseName);

    public void CreateIndex(IndexFile indexFile, string tableName, string databaseName) =>
        _store.CreateIndex(indexFile, tableName, databaseName);

    public void DropIndex(string indexName, string tableName, string databaseName) =>
        _store.DropIndex(indexName, tableName, databaseName);

    public List<string> GetDatabases() => _store.GetDatabases();

    public List<string> GetTables(string databaseName) => _store.GetTables(databaseName);

    public List<string> GetTablePrimaryKeys(string tableName, string databaseName) =>
        _store.GetTablePrimaryKeys(tableName, databaseName);

    public List<ForeignKey> GetTableForeignKeys(string tableName, string databaseName) =>
        _store.GetTableForeignKeys(tableName, databaseName);

    public List<(string ChildTable, string ChildColumn, string ParentColumn, string OnDeleteAction)> GetChildForeignKeys(
        string parentTableName,
        string databaseName) => _store.GetChildForeignKeys(parentTableName, databaseName);

    public List<string> GetTableUniqueKeys(string tableName, string databaseName) =>
        _store.GetTableUniqueKeys(tableName, databaseName);

    public List<IndexFile> GetTableIndexes(string tableName, string databaseName) =>
        _store.GetTableIndexes(tableName, databaseName);

    public List<Column> GetTableColumns(string tableName, string databaseName) =>
        _store.GetTableColumns(tableName, databaseName);

    public Column GetTableColumn(string tableName, string databaseName, string columnName) =>
        _store.GetTableColumn(tableName, databaseName, columnName);

    public string GetTableColumnType(string tableName, string databaseName, string columnName) =>
        _store.GetTableColumnType(tableName, databaseName, columnName);

    public Dictionary<string, string> GetTableIndexedColumns(string tableName, string databaseName) =>
        _store.GetTableIndexedColumns(tableName, databaseName);

    public int GetTableSchemaVersion(string tableName, string databaseName) =>
        _store.GetTableSchemaVersion(tableName, databaseName);
}