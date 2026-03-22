using System.Xml.Serialization;

namespace DataVo.Core.Models.Catalog;

/// <summary>
/// Represents an index file definition associated with a specific table.
/// </summary>
/// <example>
/// <code>
/// var index = new IndexFile { IndexFileName = "idx_name", AttributeNames = ["Name"] };
/// </code>
/// </example>
[Serializable]
[XmlRoot("IndexFile")]
public class IndexFile
{
    /// <summary>Gets or sets the logical name of the index.</summary>
    [XmlAttribute("IndexName")] public required string IndexFileName { get; set; }

    /// <summary>Gets or sets the list of column names that are indexed.</summary>
    [XmlArray("IndexAttributes")]
    [XmlArrayItem("IAttribute")]
    public required List<string> AttributeNames { get; set; }
}