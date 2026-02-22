using System.Xml.Serialization;

namespace DataVo.Core.Models.Catalog;

[Serializable]
[XmlRoot("IndexFile")]
public class IndexFile
{
    [XmlAttribute("IndexName")] public required string IndexFileName { get; set; }

    [XmlArray("IndexAttributes")]
    [XmlArrayItem("IAttribute")]
    public required List<string> AttributeNames { get; set; }
}