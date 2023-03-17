using System.Xml.Serialization;

namespace Server.Models
{
    [Serializable]
    [XmlRoot("IndexFile")]
    public class IndexFile
    {
        [XmlAttribute("IndexName")]
        public String IndexFileName { get; set; }

        [XmlArray("IndexAttributes")]
        [XmlArrayItem("IAttribute")]
        List<String> FieldNames { get; set; }
    }
}
