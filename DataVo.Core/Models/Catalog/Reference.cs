using System.Xml.Serialization;

namespace DataVo.Core.Models.Catalog
{
    [Serializable]
    [XmlRoot("References")]
    public class Reference
    {
        [XmlElement("RefTable")]
        public required string ReferenceTableName { get; set; }

        [XmlElement("RefAttribute")]
        public required string ReferenceAttributeName { get; set; }
    }
}
