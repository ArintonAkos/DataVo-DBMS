using System.Collections.Concurrent;
using System.Xml.Linq;
using System.Xml.Serialization;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.StorageEngine.Config;

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
        _persistToDisk = config.StorageMode == StorageMode.Disk;
        _catalogDirectory = _persistToDisk ? (config.DiskStoragePath ?? "./datavo_data") : null;
        _catalogFilePath = _persistToDisk ? Path.Combine(_catalogDirectory!, "Catalog.xml") : null;
        _doc = new XDocument(new XElement("Databases"));

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
                throw new Exception("Database already exists!");
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
                               ?? throw new Exception($"Database {databaseName} does not exist!");

            var existingTable = GetTableElement(rootDatabase, table.TableName);
            if (existingTable != null)
            {
                throw new Exception($"Table {table.TableName} already exists in database {databaseName}!");
            }

            ValidateForeignKeys(table, databaseName);

            var root = rootDatabase.Elements("Tables").First();
            InsertIntoXml(table, root);
            TouchTableSchemaVersion(databaseName, table.TableName);
        }
    }

    public void AddColumn(string tableName, string databaseName, Field field)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName)
                        ?? throw new Exception($"Table {tableName} does not exist in database {databaseName}!");

            if (GetTableAttributeElement(table, field.Name) != null)
            {
                throw new Exception($"Column {field.Name} already exists in table {tableName}!");
            }

            var structure = table.Elements("Structure").FirstOrDefault()
                            ?? throw new Exception($"Table {tableName} has invalid catalog structure.");

            using var writer = new StringWriter();
            var namespaces = new XmlSerializerNamespaces();
            var serializer = new XmlSerializer(typeof(Field));

            namespaces.Add("", "");
            serializer.Serialize(writer, field, namespaces);

            structure.Add(XElement.Parse(writer.ToString()));
            SaveDocument();
            TouchTableSchemaVersion(databaseName, tableName);
        }
    }

    public void DropColumn(string tableName, string databaseName, string columnName)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName)
                        ?? throw new Exception($"Table {tableName} does not exist in database {databaseName}!");

            var attribute = GetTableAttributeElement(table, columnName)
                           ?? throw new Exception($"Column {columnName} does not exist in table {tableName}!");

            attribute.Remove();
            SaveDocument();
            TouchTableSchemaVersion(databaseName, tableName);
        }
    }

    public void ModifyColumn(string tableName, string databaseName, Field field)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName)
                        ?? throw new Exception($"Table {tableName} does not exist in database {databaseName}!");

            var attribute = GetTableAttributeElement(table, field.Name)
                           ?? throw new Exception($"Column {field.Name} does not exist in table {tableName}!");

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

            SaveDocument();
            TouchTableSchemaVersion(databaseName, tableName);
        }
    }

    public void DropDatabase(string databaseName)
    {
        lock (_syncRoot)
        {
            var database = GetDatabaseElement(databaseName)
                           ?? throw new Exception($"Database {databaseName} does not exist!");

            RemoveFromXml(database);
            InvalidateDatabaseSchemaVersions(databaseName);
        }
    }

    public void DropTable(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName)
                        ?? throw new Exception($"Table {tableName} does not exist in database {databaseName}!");

            RemoveFromXml(table);
            InvalidateTableSchemaVersion(databaseName, tableName);
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
                throw new Exception("Table referred by index file doesn't exist!");
            }

            var indexElement = GetTableIndexElement(table, indexFile.IndexFileName);
            if (indexElement != null)
            {
                throw new Exception($"Index file {indexFile.IndexFileName} already exists in table {tableName}!");
            }

            if (indexFile.AttributeNames.Select(columnName => GetTableAttributeElement(table, columnName))
                .Any(column => column == null))
            {
                throw new Exception("Column referred by index file doesn't exist!");
            }

            var root = table.Elements("IndexFiles").First();
            InsertIntoXml(indexFile, root);
        }
    }

    public void DropIndex(string indexName, string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            var indexFile = GetTableIndexElement(indexName, tableName, databaseName)
                            ?? throw new Exception($"Index file {indexName} doesn't exist!");

            RemoveFromXml(indexFile);
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
                               ?? throw new Exception($"Database {databaseName} does not exist!");

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

            return result;
        }
    }

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
            var table = GetTableElement(databaseName, tableName);

            if (table == null)
            {
                return [];
            }

            return table.Elements("IndexFiles")
                .Elements("IndexFile")
                .Select(e => ConvertFromXml<IndexFile>(e)!)
                .ToList();
        }
    }

    public List<Column> GetTableColumns(string tableName, string databaseName)
    {
        lock (_syncRoot)
        {
            var table = GetTableElement(databaseName, tableName);

            if (table == null)
            {
                throw new Exception($"Table {tableName} doesn't exist in database {databaseName}");
            }

            return table.Elements("Structure")
                .Elements("Attribute")
                .Select(e => new Column
                {
                    Name = e.Attribute("Name")!.Value,
                    Type = e.Attribute("Type")!.Value,
                    Length = string.IsNullOrEmpty(e.Attribute("Length")?.Value)
                        ? 0
                        : int.Parse(e.Attribute("Length")!.Value),
                    DefaultValue = e.Attribute("DefaultValue")?.Value,
                })
                .ToList();
        }
    }

    public Column GetTableColumn(string tableName, string databaseName, string columnName)
    {
        List<Column> columns = GetTableColumns(tableName, databaseName);

        var column = columns.Find(c => c.Name == columnName);

        if (column is null)
        {
            throw new Exception($"Column {columnName} doesn't exist in table {tableName}!");
        }

        return column;
    }

    public string GetTableColumnType(string tableName, string databaseName, string columnName)
    {
        return GetTableColumn(tableName, databaseName, columnName).Type;
    }

    public Dictionary<string, string> GetTableIndexedColumns(string tableName, string databaseName)
    {
        Dictionary<string, string> result = [];
        List<IndexFile> indexFiles = GetTableIndexes(tableName, databaseName);

        foreach (var index in indexFiles)
        {
            foreach (string attribute in index.AttributeNames)
            {
                result.Add(attribute, index.IndexFileName);
            }
        }

        return result;
    }

    private void Initialize()
    {
        if (!_persistToDisk)
        {
            _doc = new XDocument(new XElement("Databases"));
            return;
        }

        Directory.CreateDirectory(_catalogDirectory!);

        if (!File.Exists(_catalogFilePath))
        {
            _doc = new XDocument(new XElement("Databases"));
            SaveDocument();
            Logger.Info("Created Catalog.xml");
            return;
        }

        _doc = XDocument.Load(_catalogFilePath!);
    }

    private XElement? GetDatabaseElement(string databaseName)
    {
        return _doc.Descendants()
            .FirstOrDefault(e => e.Name == "Database" && e.Attribute("DatabaseName")?.Value == databaseName);
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
        return database.Descendants()
            .FirstOrDefault(e => e.Name == "Table" && e.Attribute("TableName")?.Value == tableName);
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
            .FirstOrDefault(e => e.Name == "Attribute" && e.Attribute("Name")?.Value == attributeName);
    }

    private static XElement? GetTableIndexElement(XContainer table, string indexName)
    {
        return table.Descendants()
            .FirstOrDefault(e => e.Name == "IndexFile" && e.Attribute("IndexName")?.Value == indexName);
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
                    throw new Exception($"Foreign key attribute {foreignKey.AttributeName} has invalid references!");
                }

                var refAttribute = GetTableAttributeElement(refTable, reference.ReferenceAttributeName);

                if (refAttribute == null)
                {
                    throw new Exception($"Foreign key attribute {foreignKey.AttributeName} has invalid references!");
                }
            }
        }
    }

    private void InsertIntoXml<T>(T obj, XContainer root) where T : class
    {
        try
        {
            using var writer = new StringWriter();
            var namespaces = new XmlSerializerNamespaces();
            var serializer = new XmlSerializer(obj.GetType());

            namespaces.Add("", "");
            serializer.Serialize(writer, obj, namespaces);

            var element = XElement.Parse(writer.ToString());
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

    private static T? ConvertFromXml<T>(XNode element) where T : class
    {
        try
        {
            var serializer = new XmlSerializer(typeof(T));
            var reader = element.CreateReader();
            return (T?)serializer.Deserialize(reader);
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
        }

        return null;
    }

    private void SaveDocument()
    {
        if (_persistToDisk)
        {
            _doc.Save(_catalogFilePath!);
        }
    }

    private static string GetTableSchemaVersionKey(string databaseName, string tableName)
    {
        return $"{databaseName}::{tableName}";
    }

    private void TouchTableSchemaVersion(string databaseName, string tableName)
    {
        string tableKey = GetTableSchemaVersionKey(databaseName, tableName);
        _tableSchemaVersions.AddOrUpdate(tableKey, 1, (_, currentVersion) => currentVersion + 1);
    }

    private void InvalidateTableSchemaVersion(string databaseName, string tableName)
    {
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