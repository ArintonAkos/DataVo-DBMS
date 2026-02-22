using System.Xml.Serialization;

namespace DataVo.Core.Models.Catalog;

[Serializable]
[XmlRoot("ForeignKey")]
public class ForeignKey
{
    [XmlElement("FkAttribute")] public required string AttributeName { get; set; }

    [XmlArray("References")] public List<Reference> References { get; set; } = [];
}