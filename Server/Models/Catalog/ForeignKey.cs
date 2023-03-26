using System.Xml.Serialization;

namespace Server.Models.Catalog
{
    [Serializable]
    [XmlRoot("ForeignKey")]
    public class ForeignKey
    {
        [XmlElement("FkAttribute")]
        public string AttributeName { get; set; }

        [XmlArray("References")]
        public List<Reference> References { get; set; }
    }
}
