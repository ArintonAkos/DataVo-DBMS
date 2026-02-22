using System.Xml.Serialization;

namespace DataVo.Core.Models.Catalog;

[Serializable]
[XmlRoot("Database")]
public class Database
{
    [XmlAttribute] public string DatabaseName { get; set; } = null!;

    [XmlArray("Tables")]
    [XmlArrayItem("Table")]
    public List<Table> Tables { get; set; } = null!;
}