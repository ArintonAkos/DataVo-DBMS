using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace Server.Utils
{
    internal class XML<T> where T : class
    {
        internal static void CreateAndSave(T obj, String folder, String fileName)
        {
            CreateDirIfDoesntExist(folder);

            XmlSerializer serializer = new(typeof(T));
            
            // If we need a string not an xml file, this should be rewriteteen to StringWriter
            using StreamWriter writer = new(folder + "\\" + fileName);

            serializer.Serialize(writer, obj);
        }

        private static void CreateDirIfDoesntExist(String dirName)
        {
            bool exists = System.IO.Directory.Exists(dirName);

            if (!exists)
            {
                System.IO.Directory.CreateDirectory(dirName);
            }
        }
    }
}
