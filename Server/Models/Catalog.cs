using Server.Logging;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace Server.Models
{
    internal class Catalog
    {
        private static XDocument _doc = new();
        private static readonly String _dirName = "databases";
        private static readonly String _fileName = "Catalog.xml";

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

        public static void CreateTable(Table table, String databaseName)
        {
            XElement? rootDatabase = GetDatabaseElement(databaseName);
            if (rootDatabase == null)
            {
                throw new Exception($"Database {databaseName} does not exist!");
            }

            XElement? existingTable = GetTableElement(rootDatabase, table.TableName);
            if (existingTable != null)
            {
                throw new Exception($"Table {table.TableName} already exists in database {databaseName}!");
            }
            
            XElement root = rootDatabase.Elements("Tables")
                .ToList() 
                .First();

            InsertIntoXML(table, root);
        }

        public static void DropDatabase(String databaseName)
        {
            XElement? database = GetDatabaseElement(databaseName);

            if (database == null)
            {
                return;
            }

            RemoveFromXML(database);
        }

        public static void DropTable(String tableName, String databaseName)
        {
            XElement? table = GetTableElement(databaseName, tableName);

            if (table == null)
            {
                return;
            }

            RemoveFromXML(table);
        }

        private static XElement? GetDatabaseElement(String databaseName)
        {
            List<XElement> databases = _doc.Descendants()
                .Where(e => e.Name == "Database" && e.Attribute("DatabaseName")?.Value == databaseName)
                .ToList();

            return databases.FirstOrDefault();
        } 

        private static XElement? GetTableElement(String databaseName, String tableName)
        {
            XElement? rootDatabase = GetDatabaseElement(databaseName);
            if (rootDatabase == null)
            {
                return null;
            }

            return GetTableElement(rootDatabase, tableName);
        }

        private static XElement? GetTableElement(XElement database, String tableName)
        {
            List<XElement> tables = database.Descendants()
                .Where(e => e.Name == "Table" && e.Attribute("TableName")?.Value == tableName)
                .ToList();

            return tables.FirstOrDefault();
        }

        private static void CreateCatalogIfDoesntExist()
        {
            bool dirExists = Directory.Exists(_dirName);
            if (!dirExists)
            {
                Directory.CreateDirectory(_dirName);
            }

            if (!File.Exists(FilePath))
            {
                _doc.Add(new XElement("Databases"));
                _doc.Save(FilePath);

                Logger.Info($"Created {_fileName}");

                return;
            }

            _doc = XDocument.Load(FilePath);
        }

        private static void InsertIntoXML<T>(T obj, XElement root) where T : class
        {
            try
            {
                using var writer = new StringWriter();
                var serializer = new XmlSerializer(obj.GetType());
                
                serializer.Serialize(writer, obj);

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

        private static String FilePath
        {
            get
            {
                return _dirName + "\\" + _fileName;
            }
        }

        private static void RemoveFromXML(XElement element)
        {
            element.Remove();
            _doc.Save(FilePath);
        }
    }
}
