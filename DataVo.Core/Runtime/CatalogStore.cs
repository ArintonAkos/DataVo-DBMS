using System.Collections.Concurrent;
using System.Xml.Linq;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime.Catalog;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Utils;

namespace DataVo.Core.Runtime;

internal sealed class CatalogStore
{
    private readonly object _syncRoot = new();
    private readonly ConcurrentDictionary<string, int> _tableSchemaVersions = new();
    private readonly bool _persistToDisk;
    private readonly string? _catalogDirectory;
    private readonly string? _catalogFilePath;
    private XDocument _doc;

    public CatalogStore(DataVoConfig config)
    {
        // Every storage mode with a data directory (Disk, Lsm) must persist its schema, or a reopened
        // engine would find the data files but no tables.
        _catalogDirectory = config.ResolveDataDirectory();
        _persistToDisk = _catalogDirectory is not null;
        _catalogFilePath = _persistToDisk ? Path.Combine(_catalogDirectory!, "Catalog.xml") : null;
        _doc = new XDocument(new XElement("Databases"));

        _tableShapes.Clear();
        _childForeignKeys.Clear();

        Initialize();
    }

    public bool DatabaseExists(string databaseName)
    {
        lock (_syncRoot)
        {
            return GetDatabaseElement(databaseName) != null;
        }
    }

    public bool TableExists(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            return GetTableElement(databaseName, tableName) != null;
        }
    }

    public void CreateDatabase(Database database)
    {
        lock (_syncRoot)
        {
            var existingDatabase = GetDatabaseElement(database.DatabaseName);

            if (existingDatabase != null)
            {
                throw new CatalogException("Database already exists!");
            }

            var root = _doc.Elements("Databases").First();
            InsertIntoXml(database, root);
        }
    }

    public void CreateTable(Table table, string databaseName)
    {
        lock (_syncRoot)
        {
            var rootDatabase = GetDatabaseElement(databaseName)
                               ?? throw new CatalogException($"Database {databaseName} does not exist!");

            var existingTable = GetTableElement(rootDatabase, table.TableName);
            if (existingTable != null)
            {
                throw new CatalogException($"Table {table.TableName} already exists in database {databaseName}!");
            }

            ValidateForeignKeys(table, databaseName);

            var root = rootDatabase.Elements("Tables").First();
            InsertIntoXml(table, root);
            BumpTableSchemaVersion(databaseName, table.TableName);
        }
    }

    public void AddColumn(string tableName, string databaseName, Field field)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName)
                        ?? throw new CatalogException($"Table {tableName} does not exist in database {databaseName}!");

            if (GetTableAttributeElement(table, field.Name) != null)
            {
                throw new CatalogException($"Column {field.Name} already exists in table {tableName}!");
            }

            var structure = table.Elements("Structure").FirstOrDefault()
                            ?? throw new CatalogException($"Table {tableName} has invalid catalog structure.");

            structure.Add(CatalogXml.ToXElement(field));
            SaveDocument();
            BumpTableSchemaVersion(databaseName, tableName);
        }
    }

    public void DropColumn(string tableName, string databaseName, string columnName)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName)
                        ?? throw new CatalogException($"Table {tableName} does not exist in database {databaseName}!");

            var attribute = GetTableAttributeElement(table, columnName)
                           ?? throw new CatalogException($"Column {columnName} does not exist in table {tableName}!");

            attribute.Remove();
            SaveDocument();
            BumpTableSchemaVersion(databaseName, tableName);
        }
    }

    public void ModifyColumn(string tableName, string databaseName, Field field)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName)
                        ?? throw new CatalogException($"Table {tableName} does not exist in database {databaseName}!");

            var attribute = GetTableAttributeElement(table, field.Name)
                           ?? throw new CatalogException($"Column {field.Name} does not exist in table {tableName}!");

            attribute.SetAttributeValue("Type", field.Type.ToString());

            if (field.Length > 0)
            {
                attribute.SetAttributeValue("Length", field.Length);
            }
            else
            {
                attribute.SetAttributeValue("Length", null);
            }

            if (field.DefaultValue != null)
            {
                attribute.SetAttributeValue("DefaultValue", field.DefaultValue);
            }
            else
            {
                attribute.SetAttributeValue("DefaultValue", null);
            }

            // MODIFY redefines nullability too: 0 = NOT NULL; the attribute is omitted for nullable columns.
            attribute.SetAttributeValue("IsNull", field.IsNull == 0 ? 0 : null);

            SaveDocument();
            BumpTableSchemaVersion(databaseName, tableName);
        }
    }

    public void DropDatabase(string databaseName)
    {
        lock (_syncRoot)
        {
            var database = GetDatabaseElement(databaseName)
                           ?? throw new CatalogException($"Database {databaseName} does not exist!");

            RemoveFromXml(database);
            InvalidateDatabaseSchemaVersions(databaseName);
        }
    }

    public void DropTable(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName)
                        ?? throw new CatalogException($"Table {tableName} does not exist in database {databaseName}!");

            RemoveFromXml(table);
            BumpTableSchemaVersion(databaseName, tableName);
        }
    }

    public int GetTableSchemaVersion(string tableName, string databaseName)
    {
        string tableKey = GetTableSchemaVersionKey(databaseName, tableName);
        return _tableSchemaVersions.GetOrAdd(tableKey, 0);
    }

    public void CreateIndex(IndexFile indexFile, string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName);

            if (table == null)
            {
                throw new CatalogException("Table referred by index file doesn't exist!");
            }

            var indexElement = GetTableIndexElement(table, indexFile.IndexFileName);
            if (indexElement != null)
            {
                throw new CatalogException($"Index file {indexFile.IndexFileName} already exists in table {tableName}!");
            }

            if (indexFile.AttributeNames.Select(columnName => GetTableAttributeElement(table, columnName))
                .Any(column => column == null))
            {
                throw new CatalogException("Column referred by index file doesn't exist!");
            }

            var root = table.Elements("IndexFiles").First();
            InsertIntoXml(indexFile, root);
            BumpTableSchemaVersion(databaseName, tableName);
        }
    }

    public void DropIndex(string indexName, string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            var indexFile = GetTableIndexElement(indexName, tableName, databaseName)
                            ?? throw new CatalogException($"Index file {indexName} doesn't exist!");

            RemoveFromXml(indexFile);
            BumpTableSchemaVersion(databaseName, tableName);
        }
    }

    public List<string> GetDatabases()
    {
        lock (_syncRoot)
        {
            return _doc.Elements("Databases")
                .Elements("Database")
                .Select(e => e.Attribute("DatabaseName")!.Value)
                .ToList();
        }
    }

    public List<string> GetTables(string databaseName)
    {
        lock (_syncRoot)
        {
            var rootDatabase = GetDatabaseElement(databaseName)
                               ?? throw new CatalogException($"Database {databaseName} does not exist!");

            return rootDatabase.Elements("Tables")
                .Elements("Table")
                .Select(e => e.Attribute("TableName")!.Value)
                .ToList();
        }
    }

    public List<string> GetTablePrimaryKeys(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName);
            if (table == null)
            {
                return [];
            }

            return table.Elements("PrimaryKeys")
                .Elements("PkAttribute")
                .Select(e => e.Value)
                .ToList();
        }
    }

    public List<ForeignKey> GetTableForeignKeys(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName);
            if (table == null)
            {
                return [];
            }

            return table.Elements("ForeignKeys")
                .Elements("ForeignKey")
                .Select(e => ConvertFromXml<ForeignKey>(e)!)
                .ToList();
        }
    }

    public List<(string ChildTable, string ChildColumn, string ParentColumn, string OnDeleteAction)> GetChildForeignKeys(
        string parentTableName,
        string databaseName)
    {
        lock (_syncRoot)
        {
            // Asked on every DELETE/UPDATE of a parent table; the answer only changes with the catalog.
            if (_childForeignKeys.TryGetValue((databaseName, parentTableName), out var cached))
            {
                return [.. cached];
            }

            var result = new List<(string, string, string, string)>();
            var database = GetDatabaseElement(databaseName);
            if (database == null) return result;

            var tableElements = database.Descendants("Table").ToList();

            foreach (var tableEl in tableElements)
            {
                string? childTableName = tableEl.Attribute("TableName")?.Value;
                if (childTableName == null) continue;

                var fks = tableEl.Descendants("ForeignKey")
                    .Select(e => ConvertFromXml<ForeignKey>(e)!)
                    .ToList();

                foreach (var fk in fks)
                {
                    foreach (var reference in fk.References)
                    {
                        if (reference.ReferenceTableName == parentTableName)
                        {
                            result.Add((childTableName, fk.AttributeName, reference.ReferenceAttributeName, fk.OnDeleteAction));
                        }
                    }
                }
            }

            _childForeignKeys[(databaseName, parentTableName)] = [.. result];
            return result;
        }
    }

    private readonly Dictionary<(string Database, string Table), (string ChildTable, string ChildColumn, string ParentColumn, string OnDeleteAction)[]> _childForeignKeys =
        new(new TableShapeKeyComparer());

    public List<string> GetTableUniqueKeys(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName);
            if (table == null)
            {
                return [];
            }

            return table.Elements("UniqueKeys")
                .Elements("UniqueAttribute")
                .Select(e => e.Value)
                .ToList();
        }
    }

    public List<IndexFile> GetTableIndexes(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            TableShape? shape = GetTableShapeNoLock(databaseName, tableName);
            if (shape is null)
            {
                return [];
            }

            // Copies: callers may change the returned objects, the cache must stay as the catalog says.
            var indexes = new List<IndexFile>(shape.Indexes.Length);
            foreach (IndexFile index in shape.Indexes)
            {
                indexes.Add(new IndexFile
                {
                    IndexFileName = index.IndexFileName,
                    AttributeNames = [.. index.AttributeNames],
                    IndexKind = index.IndexKind,
                });
            }

            return indexes;
        }
    }

    public List<Column> GetTableColumns(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            TableShape shape = GetTableShapeNoLock(databaseName, tableName)
                ?? throw new CatalogException($"Table {tableName} doesn't exist in database {databaseName}");

            var columns = new List<Column>(shape.Columns.Length);
            foreach (Column column in shape.Columns)
            {
                columns.Add(new Column
                {
                    Name = column.Name,
                    Type = column.Type,
                    Length = column.Length,
                    DefaultValue = column.DefaultValue,
                    IsNotNull = column.IsNotNull,
                });
            }

            return columns;
        }
    }

    internal IReadOnlyList<Column> GetTableColumnsReadOnly(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            return GetTableShapeNoLock(databaseName, tableName)?.Columns
                ?? throw new CatalogException($"Table {tableName} doesn't exist in database {databaseName}");
        }
    }

    internal IReadOnlyList<IndexFile> GetTableIndexesReadOnly(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            return GetTableShapeNoLock(databaseName, tableName)?.Indexes ?? [];
        }
    }

    /// <summary>Parsed columns and indexes of one table, cached until the next catalog change.</summary>
    private sealed record TableShape(Column[] Columns, IndexFile[] Indexes);

    private readonly Dictionary<(string Database, string Table), TableShape> _tableShapes =
        new(new TableShapeKeyComparer());

    private sealed class TableShapeKeyComparer : IEqualityComparer<(string Database, string Table)>
    {
        public bool Equals((string Database, string Table) x, (string Database, string Table) y) =>
            string.Equals(x.Database, y.Database, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.Table, y.Table, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode((string Database, string Table) key) =>
            HashCode.Combine(
                StringComparer.OrdinalIgnoreCase.GetHashCode(key.Database),
                StringComparer.OrdinalIgnoreCase.GetHashCode(key.Table));
    }

    private TableShape? GetTableShapeNoLock(string databaseName, string tableName)
    {
        if (_tableShapes.TryGetValue((databaseName, tableName), out TableShape? cached))
        {
            return cached;
        }

        XElement? table = GetTableElement(databaseName, tableName);
        if (table is null)
        {
            return null;
        }

        Column[] columns = [.. table.Elements("Structure")
            .Elements("Attribute")
            .Select(e => new Column
            {
                Name = e.Attribute("Name")!.Value,
                Type = e.Attribute("Type")!.Value,
                Length = string.IsNullOrEmpty(e.Attribute("Length")?.Value)
                    ? 0
                    : int.Parse(e.Attribute("Length")!.Value),
                DefaultValue = e.Attribute("DefaultValue")?.Value,
                IsNotNull = e.Attribute("IsNull")?.Value == "0",
            })];
        IndexFile[] indexes = [.. table.Elements("IndexFiles")
            .Elements("IndexFile")
            .Select(e => ConvertFromXml<IndexFile>(e)!)];

        var shape = new TableShape(columns, indexes);
        _tableShapes[(databaseName, tableName)] = shape;
        return shape;
    }

    public Column GetTableColumn(string tableName, string databaseName, string columnName)
    {
        List<Column> columns = GetTableColumns(tableName, databaseName);

        var column = columns.Find(c => c.Name == columnName);

        if (column is null)
        {
            throw new CatalogException($"Column {columnName} doesn't exist in table {tableName}!");
        }

        return column;
    }

    public string GetTableColumnType(string tableName, string databaseName, string columnName)
    {
        return GetTableColumn(tableName, databaseName, columnName).Type;
    }

    public Dictionary<string, string> GetTableIndexedColumns(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            Dictionary<string, string> result = [];
            TableShape? shape = GetTableShapeNoLock(databaseName, tableName);
            if (shape is null)
            {
                return result;
            }

            foreach (IndexFile index in shape.Indexes)
            {
                foreach (string attribute in index.AttributeNames)
                {
                    result.Add(attribute, index.IndexFileName);
                }
            }

            return result;
        }
    }

    public string ExportState()
    {
        lock (_syncRoot)
        {
            using var writer = new StringWriter();
            _doc.Save(writer);
            return writer.ToString();
        }
    }

    public void LoadState(string? xml)
    {
        lock (_syncRoot)
        {
            if (string.IsNullOrWhiteSpace(xml))
            {
                _doc = new XDocument(new XElement("Databases"));

                _tableShapes.Clear();
        _childForeignKeys.Clear();
                _tableSchemaVersions.Clear();
                return;
            }

            var loaded = XDocument.Parse(xml);
            if (loaded.Root?.Name != "Databases")
            {
                throw new CatalogException("Invalid catalog state. Root element 'Databases' was expected.");
            }

            _doc = loaded;


            _tableShapes.Clear();
        _childForeignKeys.Clear();
            _tableSchemaVersions.Clear();
        }
    }

    private void Initialize()
    {
        if (!_persistToDisk)
        {
            _doc = new XDocument(new XElement("Databases"));

            _tableShapes.Clear();
        _childForeignKeys.Clear();
            return;
        }

        Directory.CreateDirectory(_catalogDirectory!);

        if (!File.Exists(_catalogFilePath))
        {
            _doc = new XDocument(new XElement("Databases"));

            _tableShapes.Clear();
        _childForeignKeys.Clear();
            SaveDocument();
            Logger.Info("Created Catalog.xml");
            return;
        }

        _doc = XDocument.Load(_catalogFilePath!);
    }

    private XElement? GetDatabaseElement(string databaseName)
    {
        // Databases are direct children of the root; scanning every descendant made each lookup cost
        // the size of the whole catalog (all databases, tables, columns, and indexes).
        XElement? direct = _doc.Root?.Elements("Database")
            .FirstOrDefault(e => string.Equals(e.Attribute("DatabaseName")?.Value, databaseName, StringComparison.OrdinalIgnoreCase));
        return direct ?? _doc.Descendants()
            .FirstOrDefault(e => e.Name == "Database"
                && string.Equals(e.Attribute("DatabaseName")?.Value, databaseName, StringComparison.OrdinalIgnoreCase));
    }

    private XElement? GetTableElement(string databaseName, string tableName)
    {
        var rootDatabase = GetDatabaseElement(databaseName);

        if (rootDatabase == null)
        {
            return null;
        }

        return GetTableElement(rootDatabase, tableName);
    }

    private static XElement? GetTableElement(XElement database, string tableName)
    {
        XElement? direct = database.Element("Tables")?.Elements("Table")
            .FirstOrDefault(e => string.Equals(e.Attribute("TableName")?.Value, tableName, StringComparison.OrdinalIgnoreCase));
        return direct ?? database.Descendants()
            .FirstOrDefault(e => e.Name == "Table"
                && string.Equals(e.Attribute("TableName")?.Value, tableName, StringComparison.OrdinalIgnoreCase));
    }

    private XElement? GetTableIndexElement(string indexName, string tableName, string databaseName)
    {
        var table = GetTableElement(databaseName, tableName);

        if (table == null)
        {
            return null;
        }

        return GetTableIndexElement(table, indexName);
    }

    private static XElement? GetTableAttributeElement(XElement table, string attributeName)
    {
        return table.Descendants()
            .FirstOrDefault(e => e.Name == "Attribute"
                && string.Equals(e.Attribute("Name")?.Value, attributeName, StringComparison.OrdinalIgnoreCase));
    }

    private static XElement? GetTableIndexElement(XContainer table, string indexName)
    {
        return table.Descendants()
            .FirstOrDefault(e => e.Name == "IndexFile"
                && string.Equals(e.Attribute("IndexName")?.Value, indexName, StringComparison.OrdinalIgnoreCase));
    }

    private void ValidateForeignKeys(Table table, string databaseName)
    {
        foreach (var foreignKey in table.ForeignKeys)
        {
            foreach (var reference in foreignKey.References)
            {
                var refTable = GetTableElement(databaseName, reference.ReferenceTableName);

                if (refTable == null)
                {
                    throw new CatalogException($"Foreign key attribute {foreignKey.AttributeName} has invalid references!");
                }

                var refAttribute = GetTableAttributeElement(refTable, reference.ReferenceAttributeName);

                if (refAttribute == null)
                {
                    throw new CatalogException($"Foreign key attribute {foreignKey.AttributeName} has invalid references!");
                }
            }
        }
    }

    private void InsertIntoXml<T>(T obj, XContainer root) where T : class
    {
        try
        {
            XElement element = obj switch
            {
                Database database => CatalogXml.ToXElement(database),
                Table table => CatalogXml.ToXElement(table),
                IndexFile indexFile => CatalogXml.ToXElement(indexFile),
                _ => throw new NotSupportedException($"No catalog XML mapper for type {typeof(T).Name}."),
            };

            root.Add(element);
            SaveDocument();
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            throw;
        }
    }

    private void RemoveFromXml(XNode element)
    {
        element.Remove();
        SaveDocument();
    }

    private static T? ConvertFromXml<T>(XNode node) where T : class
    {
        try
        {
            var element = (XElement)node;
            object result = typeof(T) switch
            {
                var t when t == typeof(ForeignKey) => CatalogXml.ForeignKeyFromXElement(element),
                var t when t == typeof(IndexFile) => CatalogXml.IndexFileFromXElement(element),
                _ => throw new NotSupportedException($"No catalog XML mapper for type {typeof(T).Name}."),
            };

            return (T)result;
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
        }

        return null;
    }

    /// <summary>
    /// Persists the catalog XML to disk using an atomic write pattern.
    /// Writes to a temporary file first, then replaces the target to prevent corruption
    /// if the process crashes mid-save.
    /// </summary>
    private void SaveDocument()
    {
        // Every catalog change goes through here; drop parsed table shapes so none can go stale.
        _tableShapes.Clear();
        _childForeignKeys.Clear();

        if (!_persistToDisk)
        {
            return;
        }

        string tempPath = _catalogFilePath! + ".tmp";
        _doc.Save(tempPath);

        // Use a browser-safe fallback where File.Replace is unavailable.
        AtomicFileOperations.ReplaceFromTemp(tempPath, _catalogFilePath!);
    }

    private static string GetTableSchemaVersionKey(string databaseName, string tableName)
    {
        return $"{databaseName}::{tableName}";
    }

    /// <summary>
    /// Increments the schema version counter for the specified table.
    /// This invalidates any cached schema snapshots held by the row serializer.
    /// </summary>
    private void BumpTableSchemaVersion(string databaseName, string tableName)
    {
        _tableShapes.Clear();
        _childForeignKeys.Clear();
        string tableKey = GetTableSchemaVersionKey(databaseName, tableName);
        _tableSchemaVersions.AddOrUpdate(tableKey, 1, (_, currentVersion) => currentVersion + 1);
    }

    private void InvalidateDatabaseSchemaVersions(string databaseName)
    {
        string prefix = $"{databaseName}::";
        foreach (string key in _tableSchemaVersions.Keys)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
            {
                _tableSchemaVersions.AddOrUpdate(key, 1, (_, currentVersion) => currentVersion + 1);
            }
        }
    }
}