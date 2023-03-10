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

                if (rootNodeList.Count == 0 )
                {
                    throw new Exception("Tag doesnt exist!");
                }

                var nav = rootNodeList[0].CreateNavigator();
                var emptyNamepsaces = new XmlSerializerNamespaces(new[] {
                    XmlQualifiedName.Empty
                });

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
            XmlDocument doc = new XmlDocument();

            bool dirExists = System.IO.Directory.Exists(dirName);
            if (!dirExists)
            {
                System.IO.Directory.CreateDirectory(dirName);
            }

            bool fileExists = System.IO.File.Exists(fileName);
            if (!fileExists)
            {
                doc.LoadXml("<Databases></Databases>");

                using StreamWriter writer = new(dirName + "\\" + fileName);
                doc.Save(writer);

                return doc;
            }

            doc.LoadXml(dirName + "\\" + fileName);
            return doc;
        }
    }
}
