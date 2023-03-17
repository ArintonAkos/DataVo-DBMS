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
            XElement root = _doc.Descendants()
                .Where(e => e.Name == "Databases")
                .ToList()
                .First();

            InsertIntoXML(database, root);
        }

        public static void CreateTable(Table table, String databaseName)
        {
            XElement root = _doc.Descendants()
                .Where(e => e.Name == "Database" && e.Attribute("DatabaseName")?.Value == databaseName)
                .Elements("Tables")
                .ToList()
                .First();

            InsertIntoXML(table, root);
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
    }
}
