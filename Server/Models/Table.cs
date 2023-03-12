using System.Xml.Serialization;

namespace Server.Models
{
    public class Table
    {
        public List<Field> Fields { get; set; }
        
        [XmlArray("PrimaryKeys")]
        [XmlArrayItem("PkAttribute")]
        public List<string> PrimaryKeys { get; set; }

        [XmlArray("ForeignKeys")]
        [XmlArrayItem("ForeignKey")]
        public List<ForeignKey> ForeignKeys { get; set; }
    }
}
