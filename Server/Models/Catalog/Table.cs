using System.Xml.Serialization;

namespace Server.Models.Catalog;

[Serializable]
[XmlRoot("Table")]
public class Table
{
    [XmlAttribute] public string TableName { get; set; }

    [XmlArray("Structure")]
    [XmlArrayItem("Attribute")]
    public List<Field> Fields { get; set; }

    [XmlArray("PrimaryKeys")]
    [XmlArrayItem("PkAttribute")]
    public List<string> PrimaryKeys { get; set; }

    [XmlArray("ForeignKeys")]
    [XmlArrayItem("ForeignKey")]
    public List<ForeignKey> ForeignKeys { get; set; }

    [XmlIgnore] public bool ForeignKeysSpecified => ForeignKeys.Count > 0;

    [XmlArray("UniqueKeys")]
    [XmlArrayItem("UniqueAttribute")]
    public List<string> UniqueAttributes { get; set; }

    [XmlIgnore] public bool UniqueAttributesSpecified => UniqueAttributes.Count > 0;

    [XmlArray("IndexFiles")]
    [XmlArrayItem("IndexFile")]
    public List<IndexFile> IndexFiles { get; set; }
}