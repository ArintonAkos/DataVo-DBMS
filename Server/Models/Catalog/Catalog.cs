using MongoDB.Driver;
using Server.Logging;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace Server.Models.Catalog
{
    internal class Catalog
    {
        private static XDocument _doc = new();
        private static readonly string _dirName = "databases";
        private static readonly string _fileName = "Catalog.xml";

        private static string FilePath
        {
            get
            {
                return _dirName + "\\" + _fileName;
            }
        }

        static Catalog()
        {
            CreateCatalogIfDoesntExist();
        }

        public static void CreateDatabase(Database database)
        {
            XElement? existingDatabase = GetDatabaseElement(database.DatabaseName);
            if (existingDatabase != null)
            {
                throw new Exception("Database already exists!");
            }

            XElement root = _doc.Elements("Databases")
                .ToList()
                .First();

            InsertIntoXML(database, root);
        }

        public static void CreateTable(Table table, string databaseName)
        {
            XElement? rootDatabase = GetDatabaseElement(databaseName) 
                ?? throw new Exception($"Database {databaseName} does not exist!");
            
            XElement? existingTable = GetTableElement(rootDatabase, table.TableName);
            if (existingTable != null)
            {
                throw new Exception($"Table {table.TableName} already exists in database {databaseName}!");
            }

            ValidateForeignKeys(table, databaseName);

            XElement root = rootDatabase.Elements("Tables")
                .ToList()
                .First();

            InsertIntoXML(table, root);
        }

        public static void DropDatabase(string databaseName)
        {
            XElement? database = GetDatabaseElement(databaseName)
                ?? throw new Exception($"Database {databaseName} does not exist!");

            RemoveFromXML(database);
        }

        public static void DropTable(string tableName, string databaseName)
        {
            XElement? table = GetTableElement(databaseName, tableName)
                ?? throw new Exception($"Table {tableName} does not exist in database {databaseName}!");

            RemoveFromXML(table);
        }

        public static void CreateIndex(IndexFile indexFile, string tableName, string databaseName)
        {
            XElement? table = GetTableElement(databaseName, tableName) 
                ?? throw new Exception("Table referred by index file doesn't exist!");

            XElement? indexElement = GetTableIndexElement(table, indexFile.IndexFileName);
            if (indexElement != null)
            {
                throw new Exception($"Index file {indexFile.IndexFileName} already exists in table {tableName}!");
            }

            foreach (string columnName in indexFile.AttributeNames)
            {
                XElement? column = GetTableAttributeElement(table, columnName) 
                    ?? throw new Exception("Column refered by index file doesn't exist!");
            }

            XElement root = table.Elements("IndexFiles")
                .ToList()
                .First();

            InsertIntoXML(indexFile, root);
        }

        public static void DropIndex(string indexName, string tableName, string databaseName)
        {
            XElement? indexFile = GetTableIndexElement(indexName, tableName, databaseName) 
                ?? throw new Exception($"Index file {indexName} doesn't exist!");
            
            RemoveFromXML(indexFile);
        }

        public static List<string> GetDatabases()
        {
            return _doc.Elements("Databases")
                .Elements("Database")
                .Select(e => e.Attribute("DatabaseName")!.Value)
                .ToList();
        }

        public static List<string> GetTables(string databaseName)
        {
            XElement? rootDatabase = GetDatabaseElement(databaseName)
                ?? throw new Exception($"Database {databaseName} does not exist!");
            
            return rootDatabase.Elements("Tables")
                .Elements("Table")
                .Select(e => e.Attribute("TableName")!.Value)
                .ToList();
        }

        public static List<string> GetTablePrimaryKeys(string tableName, string databaseName)
        {
            XElement? table = GetTableElement(databaseName, tableName);
            if (table == null)
            {
                return new();
            }

            return table.Elements("PrimaryKeys")
                .Select(e => e.Value)
                .ToList();
        }

        public static List<ForeignKey> GetTableForeignKeys(string tableName, string databaseName)
        {
            XElement? table = GetTableElement(databaseName, tableName);
            if (table == null)
            {
                return new();
            }

            return table.Elements("ForeignKeys")
                .Elements("ForeignKey")
                .Select(e => ConvertFromXml<ForeignKey>(e)!)
                .ToList();
        }

        public static List<string> GetTableUniqueKeys(string tableName, string databaseName)
        {
            XElement? table = GetTableElement(databaseName, tableName);
            if (table == null)
            {
                return new();
            }

            return table.Elements("UniqueKeys")
                .Select(e => e.Value)
                .ToList();
        }

        public static List<IndexFile> GetTableIndexes(string tableName, string databaseName)
        {
            XElement? table = GetTableElement(databaseName, tableName);
            if (table == null)
            {
                return new();
            }

            return table.Elements("IndexFiles")
                .Elements("IndexFile")
                .Select(e => ConvertFromXml<IndexFile>(e)!)
                .ToList();
        }

        public static List<Column> GetTableColumns(string tableName, string databaseName)
        {
            XElement? table = GetTableElement(databaseName, tableName)
                ?? throw new Exception($"Table {tableName} doesn't exist in database {databaseName}");

            return table.Elements("Structure")
                .Elements("Attribute")
                .Select(e => new Column()
                {
                    Name = e.Attribute("Name")!.Value,
                    Type = e.Attribute("Type")!.Value,
                    Length = string.IsNullOrEmpty(e.Attribute("Length")?.Value) ? 
                             0 : int.Parse(e.Attribute("Length")!.Value),
                })
                .ToList();
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

        private static XElement? GetTableElement(string databaseName, string tableName)
        {
            XElement? rootDatabase = GetDatabaseElement(databaseName);
            if (rootDatabase == null)
            {
                return null;
            }

            return GetTableElement(rootDatabase, tableName);
        }

        private static XElement? GetTableElement(XElement database, string tableName)
        {
            List<XElement> tables = database.Descendants()
                .Where(e => e.Name == "Table" && e.Attribute("TableName")?.Value == tableName)
                .ToList();

            return tables.FirstOrDefault();
        }

        private static XElement? GetTableIndexElement(string indexName, string tableName, string databaseName)
        {
            XElement? table = GetTableElement(databaseName, tableName);
            if (table == null)
            {
                return null;
            }

            return GetTableIndexElement(table, indexName);
        }

        private static XElement? GetTableAttributeElement(XElement table, string attributeName)
        {
            List<XElement> attributes = table.Descendants()
                .Where(e => e.Name == "Attribute" && e.Attribute("Name")?.Value == attributeName)
                .ToList();

            return attributes.FirstOrDefault();
        }

        private static XElement? GetTableIndexElement(XElement table, string indexName) 
        {
            List<XElement> indexFiles = table.Descendants()
                .Where(e => e.Name == "IndexFile" && e.Attribute("IndexName")?.Value == indexName)
                .ToList();

            return indexFiles.FirstOrDefault();
        }

        private static void ValidateForeignKeys(Table table, string databaseName)
        {
            foreach (ForeignKey foreignKey in table.ForeignKeys)
            {
                foreach (Reference reference in foreignKey.References)
                {
                    XElement? refTable = GetTableElement(databaseName, reference.ReferenceTableName) 
                        ?? throw new Exception($"Foreign key attribute {foreignKey.AttributeName} has invalid references!");
                    
                    XElement? refAttribute = GetTableAttributeElement(refTable, reference.ReferenceAttributeName) 
                        ?? throw new Exception($"Foreign key attribute {foreignKey.AttributeName} has invalid references!");
                }
            }
        }

        private static void CreateCatalogIfDoesntExist()
        {
            if (!Directory.Exists(_dirName))
            {
                Directory.CreateDirectory(_dirName);
            }

            lock (_doc)
            {
                if (!File.Exists(FilePath))
                {
                    _doc.Add(new XElement("Databases"));
                    _doc.Save(FilePath);

                    Logger.Info($"Created {_fileName}");

                    return;
                }

                _doc = XDocument.Load(FilePath);
            }
        }

        private static void InsertIntoXML<T>(T obj, XElement root) where T : class
        {
            try
            {
                using var writer = new StringWriter();
                var namespaces = new XmlSerializerNamespaces();
                var serializer = new XmlSerializer(obj.GetType());

                namespaces.Add("", "");
                serializer.Serialize(writer, obj, namespaces);

                XElement element = XElement.Parse(writer.ToString());
                root.Add(element);

                writer.Close();

                _doc.Save(FilePath);
            }
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
            }
        }

        private static void RemoveFromXML(XElement element)
        {
            element.Remove();
            _doc.Save(FilePath);
        }

        private static T? ConvertFromXml<T>(XElement element) where T : class
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
    }
}
