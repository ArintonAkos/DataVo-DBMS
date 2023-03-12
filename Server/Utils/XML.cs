using System.Xml;
using System.Xml.Linq;
using System.Xml.Serialization;
using System.Xml.XPath;
using Server.Logging;

namespace Server.Utils
{
    internal class XML
    {
        internal static void InsertObjIntoXML<T>(T obj, String tag, String folder, String fileName) where T : class
        {
            try
            {
                XDocument doc = CreateDocumentIfDoesntExist(folder, fileName);

                var rootNodeList = doc!
                    .Descendants()
                    .Where(e => e.Name == tag)
                    .ToList();

                if (rootNodeList.Count == 0) 
                {
                    throw new Exception("Tag doesn't exist!");
                }

                using (var writer = new StringWriter())
                {
                    var rootNode = rootNodeList.First();

                    var serializer = new XmlSerializer(obj.GetType());
                    serializer.Serialize(writer, obj);

                    XElement element = XElement.Parse(writer.ToString());
                    rootNode.Add(element);

                    writer.Close();
                }

                doc.Save(folder + "\\" + fileName);

            } catch(Exception ex)
            {
                Logger.Error(ex.Message);
            }
        }

        private static XDocument CreateDocumentIfDoesntExist(String dirName, String fileName)
        {
            XDocument doc = new();

            bool dirExists = Directory.Exists(dirName);
            if (!dirExists)
            {
                Directory.CreateDirectory(dirName);
            }

            bool fileExists = File.Exists(dirName + "\\" + fileName);
            if (!fileExists)
            {
                doc.Add(new XElement("Databases"));
                doc.Save(dirName + "\\" + fileName);

                Logger.Info("Created Catalog.xml");

                return doc;
            }

            doc = XDocument.Load(dirName+ "\\" + fileName);

            return doc;
        }
    }
}
