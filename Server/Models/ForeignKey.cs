using System.Xml.Serialization;

namespace Server.Models
{
    [Serializable]
    [XmlRoot("ForeignKey")]
    public class ForeignKey
    {
        [XmlElement("FkAttribute")]
        public String AttributeName { get; set; }

        [XmlArray("References")]
        public List<Reference> References { get; set; }
    }
}
