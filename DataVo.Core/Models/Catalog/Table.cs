using System.Xml.Serialization;

namespace DataVo.Core.Models.Catalog;

[Serializable]
[XmlRoot("Table")]
public class Table
{
    [XmlAttribute] public required string TableName { get; set; }

    [XmlArray("Structure")]
    [XmlArrayItem("Attribute")]
    public required List<Field> Fields { get; set; }

    [XmlArray("PrimaryKeys")]
    [XmlArrayItem("PkAttribute")]
    public required List<string> PrimaryKeys { get; set; }

    [XmlArray("ForeignKeys")]
    [XmlArrayItem("ForeignKey")]
    public required List<ForeignKey> ForeignKeys { get; set; }

    [XmlIgnore]
    public bool ForeignKeysSpecified
    {
        get => ForeignKeys.Count > 0;
    }

    [XmlArray("UniqueKeys")]
    [XmlArrayItem("UniqueAttribute")]
    public required List<string> UniqueAttributes { get; set; }

    [XmlIgnore]
    public bool UniqueAttributesSpecified
    {
        get => UniqueAttributes.Count > 0;
    }

    [XmlArray("IndexFiles")]
    [XmlArrayItem("IndexFile")]
    public required List<IndexFile> IndexFiles { get; set; }
}