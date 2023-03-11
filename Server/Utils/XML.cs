using System.Xml;
using System.Xml.Serialization;
using Server.Logging;

namespace Server.Utils
{
    internal class XML<T> where T : class
    {
        internal static void InsertObjIntoXML(T obj, String tag, String folder, String fileName)
        {
            try
            {
                XmlDocument doc = CreateDocumentIfDoesntExist(folder, fileName);
                var rootNodeList = doc.GetElementsByTagName(tag);

                if (rootNodeList.Count == 0)
                {
                    throw new Exception($"Tag {tag} doesnt exist!");
                }

                var nav = rootNodeList[0].CreateNavigator();
                using (var writer = nav.AppendChild())
                {
                    var serializer = new XmlSerializer(obj.GetType());
                    writer.WriteWhitespace("");
                    serializer.Serialize(writer, obj);
                    writer.Close();
                }

                doc.Save(folder + "\\" + fileName);

            } catch(Exception ex)
            {
                Logger.Error(ex.Message);
            }
        }

        private static XmlDocument CreateDocumentIfDoesntExist(String dirName, String fileName)
        {
            XmlDocument doc = new();

            bool dirExists = Directory.Exists(dirName);
            if (!dirExists)
            {
                Directory.CreateDirectory(dirName);
            }

            bool fileExists = File.Exists(dirName + "\\" + fileName);
            if (!fileExists)
            {
                doc.LoadXml("<Databases></Databases>");

                using StreamWriter writer = new(dirName + "\\" + fileName);
                doc.Save(writer);

                Logger.Info("Created Catalog.xml");

                return doc;
            }

            doc.Load(dirName + "\\" + fileName);
            return doc;
        }
    }
}
