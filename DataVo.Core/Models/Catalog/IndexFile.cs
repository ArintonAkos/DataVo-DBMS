using System.Xml.Serialization;

namespace DataVo.Core.Models.Catalog;

[Serializable]
[XmlRoot("IndexFile")]
public class IndexFile
{
    [XmlAttribute("IndexName")] public string IndexFileName { get; set; }

    [XmlArray("IndexAttributes")]
    [XmlArrayItem("IAttribute")]
    public List<string> AttributeNames { get; set; }
}