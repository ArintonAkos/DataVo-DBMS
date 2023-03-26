using System.Xml.Serialization;

namespace Server.Models.Catalog
{
    [Serializable]
    [XmlRoot("References")]
    public class Reference
    {
        [XmlElement("RefTable")]
        public string ReferenceTableName { get; set; }

        [XmlElement("RefAttribute")]
        public string ReferenceAttributeName { get; set; }
    }
}
