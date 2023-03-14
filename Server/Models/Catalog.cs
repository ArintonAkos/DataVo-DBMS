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

            XElement root = _doc.Descendants()
                .Where(e => e.Name == "Databases")
                .ToList()
                .First();

            InsertIntoXML(database, root);
        }

        public static void CreateTable(Table table, String databaseName)
        {
            XElement? rootDatabase = GetDatabaseElement(databaseName);
            if (rootDatabase == null)
            {
                throw new Exception("Database does not exist!");
            }

            XElement? existingTable = GetTableElement(databaseName, table.TableName);
            if (existingTable != null)
            {
                throw new Exception($"Table already exists in database {databaseName}");
            }
            
            XElement root = rootDatabase.Elements("Tables")
                .ToList() 
                .First();

            InsertIntoXML(table, root);
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

            List<XElement> tables = rootDatabase.Descendants()
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

            bool fileExists = File.Exists(_dirName + "\\" + _fileName);
            if (!fileExists)
            {
                _doc.Add(new XElement("Databases"));
                _doc.Save(_dirName + "\\" + _fileName);

                Logger.Info($"Created {_fileName}");

                return;
            }

            _doc = XDocument.Load(_dirName + "\\" + _fileName);
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

                _doc.Save(_dirName + "\\" + _fileName);
            } 
            catch (Exception ex)
            {
                Logger.Error(ex.Message);
            }
        }
    }
}
