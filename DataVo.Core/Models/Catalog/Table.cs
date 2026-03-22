using System.Xml.Serialization;

namespace DataVo.Core.Models.Catalog;

/// <summary>
/// Represents a database table definition including its schema, foreign keys, and indexes.
/// </summary>
/// <example>
/// <code>
/// var table = new Table { TableName = "Users", Fields = new List&lt;Field&gt;(), PrimaryKeys = ["Id"], ForeignKeys = new List&lt;ForeignKey&gt;(), UniqueAttributes = new List&lt;string&gt;(), IndexFiles = new List&lt;IndexFile&gt;() };
/// </code>
/// </example>
[Serializable]
[XmlRoot("Table")]
public class Table
{
    /// <summary>Gets or sets the name of the table.</summary>
    [XmlAttribute] public required string TableName { get; set; }

    /// <summary>Gets or sets the attributes or columns defined in the table.</summary>
    [XmlArray("Structure")]
    [XmlArrayItem("Attribute")]
    public required List<Field> Fields { get; set; }

    /// <summary>Gets or sets the primary key column names.</summary>
    [XmlArray("PrimaryKeys")]
    [XmlArrayItem("PkAttribute")]
    public required List<string> PrimaryKeys { get; set; }

    /// <summary>Gets or sets the declared foreign key constraints.</summary>
    [XmlArray("ForeignKeys")]
    [XmlArrayItem("ForeignKey")]
    public required List<ForeignKey> ForeignKeys { get; set; }

    /// <summary>Indicates whether any foreign keys exist.</summary>
    [XmlIgnore]
    public bool ForeignKeysSpecified
    {
        get => ForeignKeys.Count > 0;
    }

    /// <summary>Gets or sets the column names marked as UNIQUE.</summary>
    [XmlArray("UniqueKeys")]
    [XmlArrayItem("UniqueAttribute")]
    public required List<string> UniqueAttributes { get; set; }

    /// <summary>Indicates whether any unique constraints exist.</summary>
    [XmlIgnore]
    public bool UniqueAttributesSpecified
    {
        get => UniqueAttributes.Count > 0;
    }

    /// <summary>Gets or sets the index files associated with the table.</summary>
    [XmlArray("IndexFiles")]
    [XmlArrayItem("IndexFile")]
    public required List<IndexFile> IndexFiles { get; set; }
}