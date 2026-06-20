using System.Xml.Linq;
using System.Collections.Concurrent;
using DataVo.Core.Exceptions;
using DataVo.Core.Logging;
using DataVo.Core.Runtime.Catalog;

namespace DataVo.Core.Models.Catalog;

/// <summary>
/// Provides a centralized catalog for managing databases, tables, columns, indexes, and constraints.
/// Serializes metadata to an XML document for persistence.
/// </summary>
public static class Catalog
{
    private const string DIR_NAME = "databases";
    private const string FILE_NAME = "Catalog.xml";
    private static XDocument _doc = new();
    private static readonly ConcurrentDictionary<string, int> _tableSchemaVersions = new();

    static Catalog()
    {
        CreateCatalogIfDoesntExist();
    }

    private static string FilePath
    {
        get => Path.Combine(DIR_NAME, FILE_NAME);
    }

    /// <summary>
    /// Checks whether a database with the given name exists in the catalog.
    /// </summary>
    /// <param name="databaseName">The name of the database to check.</param>
    /// <returns><c>true</c> if the database exists; otherwise, <c>false</c>.</returns>
    /// <example>
    /// <code>
    /// bool exists = Catalog.DatabaseExists("customer_db");
    /// </code>
    /// </example>
    public static bool DatabaseExists(string databaseName)
    {
        return GetDatabaseElement(databaseName) != null;
    }

    /// <summary>
    /// Checks whether a table exists within a specified database.
    /// </summary>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="databaseName">The name of the database.</param>
    /// <returns><c>true</c> if the table exists; otherwise, <c>false</c>.</returns>
    /// <example>
    /// <code>
    /// bool exists = Catalog.TableExists("Users", "customer_db");
    /// </code>
    /// </example>
    public static bool TableExists(string tableName, string databaseName)
    {
        return GetTableElement(databaseName, tableName) != null;
    }

    /// <summary>
    /// Creates a new database in the catalog and persists the structure to XML.
    /// </summary>
    /// <param name="database">The database object to insert.</param>
    /// <exception cref="Exception">Thrown if the database already exists.</exception>
    /// <example>
    /// <code>
    /// Catalog.CreateDatabase(new Database { DatabaseName = "test_db", Tables = [] });
    /// </code>
    /// </example>
    public static void CreateDatabase(Database database)
    {
        var existingDatabase = GetDatabaseElement(database.DatabaseName);

        if (existingDatabase != null)
        {
            throw new CatalogException("Database already exists!");
        }

        var root = _doc.Elements("Databases")
            .ToList()
            .First();

        InsertIntoXml(database, root);
    }

    /// <summary>
    /// Adds a new table definition to an existing database.
    /// </summary>
    /// <param name="table">The table definition to add.</param>
    /// <param name="databaseName">The database to append the table to.</param>
    /// <exception cref="Exception">Thrown if the database doesn't exist, table already exists, or foreign key validations fail.</exception>
    /// <example>
    /// <code>
    /// Catalog.CreateTable(new Table { TableName = "Orders", ... }, "customer_db");
    /// </code>
    /// </example>
    public static void CreateTable(Table table, string databaseName)
    {
        var rootDatabase = GetDatabaseElement(databaseName);

        if (rootDatabase == null)
        {
            throw new CatalogException($"Database {databaseName} does not exist!");
        }

        var existingTable = GetTableElement(rootDatabase, table.TableName);
        if (existingTable != null)
        {
            throw new CatalogException($"Table {table.TableName} already exists in database {databaseName}!");
        }

        ValidateForeignKeys(table, databaseName);

        var root = rootDatabase.Elements("Tables")
            .ToList()
            .First();

        InsertIntoXml(table, root);
        BumpTableSchemaVersion(databaseName, table.TableName);
    }

    /// <summary>
    /// Removes a database completely from the catalog XML.
    /// </summary>
    /// <param name="databaseName">The name of the database to drop.</param>
    /// <exception cref="Exception">Thrown if the database does not exist.</exception>
    /// <example>
    /// <code>
    /// Catalog.DropDatabase("customer_db");
    /// </code>
    /// </example>
    public static void DropDatabase(string databaseName)
    {
        var database = GetDatabaseElement(databaseName)
                       ?? throw new CatalogException($"Database {databaseName} does not exist!");

        RemoveFromXml(database);
        InvalidateDatabaseSchemaVersions(databaseName);
    }

    /// <summary>
    /// Removes a table definition from a database in the catalog.
    /// </summary>
    /// <param name="tableName">The name of the table to drop.</param>
    /// <param name="databaseName">The database the table resides in.</param>
    /// <exception cref="Exception">Thrown if the table does not exist in the specified database.</exception>
    /// <example>
    /// <code>
    /// Catalog.DropTable("Users", "customer_db");
    /// </code>
    /// </example>
    public static void DropTable(string tableName, string databaseName)
    {
        var table = GetTableElement(databaseName, tableName)
                    ?? throw new CatalogException($"Table {tableName} does not exist in database {databaseName}!");

        RemoveFromXml(table);
        BumpTableSchemaVersion(databaseName, tableName);
    }

    /// <summary>
    /// Retrieves the schema version number for a table. Incremented on DDL operations.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="databaseName">The database name.</param>
    /// <returns>The integer representing the current schema version of the table.</returns>
    /// <example>
    /// <code>
    /// int version = Catalog.GetTableSchemaVersion("Users", "customer_db");
    /// </code>
    /// </example>
    public static int GetTableSchemaVersion(string tableName, string databaseName)
    {
        string tableKey = GetTableSchemaVersionKey(databaseName, tableName);
        return _tableSchemaVersions.GetOrAdd(tableKey, 0);
    }

    /// <summary>
    /// Appends a new index file definition to an existing table in the catalog.
    /// </summary>
    /// <param name="indexFile">The index definition.</param>
    /// <param name="tableName">The table the index applies to.</param>
    /// <param name="databaseName">The database the table resides in.</param>
    /// <exception cref="Exception">Thrown if the table doesn't exist, the index already exists, or the indexed column doesn't exist.</exception>
    /// <example>
    /// <code>
    /// Catalog.CreateIndex(new IndexFile { IndexFileName = "idx_user_email", AttributeNames = ["Email"] }, "Users", "customer_db");
    /// </code>
    /// </example>
    public static void CreateIndex(IndexFile indexFile, string tableName, string databaseName)
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

        var root = table.Elements("IndexFiles")
            .ToList()
            .First();

        InsertIntoXml(indexFile, root);
    }

    /// <summary>
    /// Drops an index definition from a table in the catalog.
    /// </summary>
    /// <param name="indexName">The name of the index to drop.</param>
    /// <param name="tableName">The table owning the index.</param>
    /// <param name="databaseName">The database the table resides in.</param>
    /// <exception cref="Exception">Thrown if the index file doesn't exist.</exception>
    /// <example>
    /// <code>
    /// Catalog.DropIndex("idx_user_email", "Users", "customer_db");
    /// </code>
    /// </example>
    public static void DropIndex(string indexName, string tableName, string databaseName)
    {
        var indexFile = GetTableIndexElement(indexName, tableName, databaseName)
                        ?? throw new CatalogException($"Index file {indexName} doesn't exist!");

        RemoveFromXml(indexFile);
    }

    /// <summary>
    /// Gets a list of all database names in the catalog.
    /// </summary>
    /// <returns>A list of database name strings.</returns>
    /// <example>
    /// <code>
    /// List&lt;string&gt; databases = Catalog.GetDatabases();
    /// </code>
    /// </example>
    public static List<string> GetDatabases()
    {
        return _doc.Elements("Databases")
            .Elements("Database")
            .Select(e => e.Attribute("DatabaseName")!.Value)
            .ToList();
    }

    /// <summary>
    /// Gets a list of all table names within a specified database.
    /// </summary>
    /// <param name="databaseName">The database to query.</param>
    /// <returns>A list of table name strings.</returns>
    /// <exception cref="Exception">Thrown if the database doesn't exist.</exception>
    /// <example>
    /// <code>
    /// List&lt;string&gt; tables = Catalog.GetTables("customer_db");
    /// </code>
    /// </example>
    public static List<string> GetTables(string databaseName)
    {
        var rootDatabase = GetDatabaseElement(databaseName)
                           ?? throw new CatalogException($"Database {databaseName} does not exist!");

        return rootDatabase.Elements("Tables")
            .Elements("Table")
            .Select(e => e.Attribute("TableName")!.Value)
            .ToList();
    }

    /// <summary>
    /// Gets a list of primary key column names for a specific table.
    /// </summary>
    /// <param name="tableName">The table to query.</param>
    /// <param name="databaseName">The database the table resides in.</param>
    /// <returns>A list of primary key attribute names.</returns>
    /// <example>
    /// <code>
    /// List&lt;string&gt; pks = Catalog.GetTablePrimaryKeys("Users", "customer_db");
    /// </code>
    /// </example>
    public static List<string> GetTablePrimaryKeys(string tableName, string databaseName)
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

    /// <summary>
    /// Gets the foreign key constraints defined on a specific table.
    /// </summary>
    /// <param name="tableName">The child table to query.</param>
    /// <param name="databaseName">The database the table resides in.</param>
    /// <returns>A list of <see cref="ForeignKey"/> constraints.</returns>
    /// <example>
    /// <code>
    /// List&lt;ForeignKey&gt; fks = Catalog.GetTableForeignKeys("Orders", "customer_db");
    /// </code>
    /// </example>
    public static List<ForeignKey> GetTableForeignKeys(string tableName, string databaseName)
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

    /// <summary>
    /// Reverse FK lookup: finds all child tables that reference the given parent table.
    /// Returns (childTableName, childColumnName, parentColumnName, onDeleteAction).
    /// </summary>
    /// <param name="parentTableName">The parent table being referenced.</param>
    /// <param name="databaseName">The database to query.</param>
    /// <returns>A list of tuples defining the foreign key relationships pointing to the parent table.</returns>
    /// <example>
    /// <code>
    /// var relationships = Catalog.GetChildForeignKeys("Users", "customer_db");
    /// </code>
    /// </example>
    public static List<(string ChildTable, string ChildColumn, string ParentColumn, string OnDeleteAction)>
        GetChildForeignKeys(string parentTableName, string databaseName)
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

    /// <summary>
    /// Gets a list of column names designated as UNIQUE for a specific table.
    /// </summary>
    /// <param name="tableName">The table to query.</param>
    /// <param name="databaseName">The database the table resides in.</param>
    /// <returns>A list of unique attribute names.</returns>
    /// <example>
    /// <code>
    /// List&lt;string&gt; uniqueKeys = Catalog.GetTableUniqueKeys("Users", "customer_db");
    /// </code>
    /// </example>
    public static List<string> GetTableUniqueKeys(string tableName, string databaseName)
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

    /// <summary>
    /// Gets a list of all custom index files declared for a specific table.
    /// </summary>
    /// <param name="tableName">The table to query.</param>
    /// <param name="databaseName">The database the table resides in.</param>
    /// <returns>A list of <see cref="IndexFile"/> definitions.</returns>
    /// <example>
    /// <code>
    /// List&lt;IndexFile&gt; indexes = Catalog.GetTableIndexes("Users", "customer_db");
    /// </code>
    /// </example>
    public static List<IndexFile> GetTableIndexes(string tableName, string databaseName)
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

    /// <summary>
    /// Retrieves all column definitions for a given table.
    /// </summary>
    /// <param name="tableName">The table to query.</param>
    /// <param name="databaseName">The database the table resides in.</param>
    /// <returns>A list of <see cref="Column"/> objects parsing the table structure.</returns>
    /// <exception cref="Exception">Thrown if the table does not exist.</exception>
    /// <example>
    /// <code>
    /// List&lt;Column&gt; cols = Catalog.GetTableColumns("Users", "customer_db");
    /// </code>
    /// </example>
    public static List<Column> GetTableColumns(string tableName, string databaseName)
    {
        var table = GetTableElement(databaseName, tableName);

        if (table == null)
        {
            throw new CatalogException($"Table {tableName} doesn't exist in database {databaseName}");
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

    /// <summary>
    /// Retrieves a specific column definition from a table.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="databaseName">The database name.</param>
    /// <param name="columnName">The column to find.</param>
    /// <returns>The <see cref="Column"/> definition.</returns>
    /// <exception cref="Exception">Thrown if the column or table does not exist.</exception>
    /// <example>
    /// <code>
    /// Column col = Catalog.GetTableColumn("Users", "customer_db", "Id");
    /// </code>
    /// </example>
    public static Column GetTableColumn(string tableName, string databaseName, string columnName)
    {
        List<Column> columns = GetTableColumns(tableName, databaseName);

        var column = columns.Find(c => c.Name == columnName);

        if (column is null)
        {
            throw new CatalogException($"Column {columnName} doesn't exist in table {tableName}!");
        }

        return column!;
    }

    /// <summary>
    /// Gets the raw data type string of a specific column.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="databaseName">The database name.</param>
    /// <param name="columnName">The column name.</param>
    /// <returns>The string representation of the data type (e.g., INT, VARCHAR).</returns>
    /// <example>
    /// <code>
    /// string type = Catalog.GetTableColumnType("Users", "customer_db", "Email");
    /// </code>
    /// </example>
    public static string GetTableColumnType(string tableName, string databaseName, string columnName)
    {
        return GetTableColumn(tableName, databaseName, columnName).Type;
    }

    /// <summary>
    /// Retrieves a mapping of indexed columns to their respective index file names.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="databaseName">The database name.</param>
    /// <returns>A dictionary where the key is the column name and the value is the index name.</returns>
    /// <example>
    /// <code>
    /// var indexedCols = Catalog.GetTableIndexedColumns("Users", "customer_db");
    /// </code>
    /// </example>
    public static Dictionary<string, string> GetTableIndexedColumns(string tableName, string databaseName)
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

    private static XElement? GetDatabaseElement(string databaseName)
    {
        List<XElement> databases;

        lock (_doc)
        {
            databases = _doc.Descendants()
                .Where(e => e.Name == "Database" && e.Attribute("DatabaseName")?.Value == databaseName)
                .ToList();
        }

        return databases.FirstOrDefault();
    }

    /// <summary>
    /// Gets the raw XML element defining a table.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The table's <see cref="XElement"/> if it exists; otherwise, null.</returns>
    public static XElement? GetTableElement(string databaseName, string tableName)
    {
        var rootDatabase = GetDatabaseElement(databaseName);

        if (rootDatabase == null)
        {
            return null;
        }

        return GetTableElement(rootDatabase, tableName);
    }

    /// <summary>
    /// Gets the raw XML element defining a table, starting from a known database root element.
    /// </summary>
    /// <param name="database">The root database XML element.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The table's <see cref="XElement"/> if it exists; otherwise, null.</returns>
    public static XElement? GetTableElement(XElement database, string tableName)
    {
        var tables = database.Descendants()
            .Where(e => e.Name == "Table" && e.Attribute("TableName")?.Value == tableName)
            .ToList();

        return tables.FirstOrDefault();
    }

    private static XElement? GetTableIndexElement(string indexName, string tableName, string databaseName)
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
        var attributes = table.Descendants()
            .Where(e => e.Name == "Attribute" && e.Attribute("Name")?.Value == attributeName)
            .ToList();

        return attributes.FirstOrDefault();
    }

    private static XElement? GetTableIndexElement(XContainer table, string indexName)
    {
        var indexFiles = table.Descendants()
            .Where(e => e.Name == "IndexFile" && e.Attribute("IndexName")?.Value == indexName)
            .ToList();

        return indexFiles.FirstOrDefault();
    }

    private static void ValidateForeignKeys(Table table, string databaseName)
    {
        foreach (var foreignKey in table.ForeignKeys)
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

    private static void CreateCatalogIfDoesntExist()
    {
        if (!Directory.Exists(DIR_NAME))
        {
            Directory.CreateDirectory(DIR_NAME);
        }

        lock (_doc)
        {
            if (!File.Exists(FilePath))
            {
                _doc.Add(new XElement("Databases"));
                _doc.Save(FilePath);

                Logger.Info($"Created {FILE_NAME}");

                return;
            }

            _doc = XDocument.Load(FilePath);
        }
    }

    private static void InsertIntoXml<T>(T obj, XContainer root) where T : class
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
            _doc.Save(FilePath);
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
        }
    }

    private static void RemoveFromXml(XNode element)
    {
        element.Remove();
        _doc.Save(FilePath);
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

    private static string GetTableSchemaVersionKey(string databaseName, string tableName)
    {
        return $"{databaseName}::{tableName}";
    }

    /// <summary>
    /// Increments the schema version counter for the specified table.
    /// </summary>
    private static void BumpTableSchemaVersion(string databaseName, string tableName)
    {
        string tableKey = GetTableSchemaVersionKey(databaseName, tableName);
        _tableSchemaVersions.AddOrUpdate(tableKey, 1, (_, currentVersion) => currentVersion + 1);
    }

    private static void InvalidateDatabaseSchemaVersions(string databaseName)
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