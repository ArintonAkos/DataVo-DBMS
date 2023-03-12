using System.Xml.Serialization;

namespace Server.Models
{
    [Serializable]
    [XmlRoot("Table")]
    public class Table
    {
        [XmlAttribute]
        public String TableName { get; set; }

        [XmlArray("Structure")]
        [XmlArrayItem("Attribute")]
        public List<Field> Fields { get; set; }
        
        [XmlArray("PrimaryKeys")]
        [XmlArrayItem("pkAttribute")]
        public List<String> PrimaryKeys { get; set; }

        [XmlArray("ForeignKeys")]
        [XmlArrayItem("ForeignKey")]
        public List<ForeignKey> ForeignKeys { get; set; }
    }
}
