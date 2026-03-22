using System.Xml.Serialization;
using System.ComponentModel;

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

    /// <summary>Gets or sets the logical index implementation kind (for example BTREE or HNSW).</summary>
    [XmlAttribute("IndexKind")]
    [DefaultValue("BTREE")]
    public string IndexKind { get; set; } = "BTREE";
}