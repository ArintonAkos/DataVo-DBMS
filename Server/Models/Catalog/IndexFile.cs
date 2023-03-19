using System.Xml.Serialization;

namespace Server.Models.Catalog
{
    [Serializable]
    [XmlRoot("IndexFile")]
    public class IndexFile
    {
        [XmlAttribute("IndexName")]
        public string IndexFileName { get; set; }

        [XmlArray("IndexAttributes")]
        [XmlArrayItem("IAttribute")]
        List<string> FieldNames { get; set; }
    }
}
