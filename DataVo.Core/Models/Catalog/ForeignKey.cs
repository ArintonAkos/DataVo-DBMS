using System.Xml.Serialization;

namespace DataVo.Core.Models.Catalog;

/// <summary>
/// Represents a foreign key constraint linking a column to a reference table.
/// </summary>
/// <example>
/// <code>
/// var fk = new ForeignKey { AttributeName = "UserId", OnDeleteAction = "CASCADE", References = new List&lt;Reference&gt;() };
/// </code>
/// </example>
[Serializable]
[XmlRoot("ForeignKey")]
public class ForeignKey
{
    /// <summary>Gets or sets the local attribute (column) name that holds the foreign key.</summary>
    [XmlElement("FkAttribute")] public required string AttributeName { get; set; }

    /// <summary>Gets or sets the collection of references to primary tables and columns.</summary>
    [XmlArray("References")] public List<Reference> References { get; set; } = [];

    /// <summary>Gets or sets the action to perform on deletion (e.g., CASCADE, RESTRICT, SET NULL).</summary>
    [XmlElement("OnDeleteAction")] public string OnDeleteAction { get; set; } = "RESTRICT";
}