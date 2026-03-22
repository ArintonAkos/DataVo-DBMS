using System.Xml.Serialization;

namespace DataVo.Core.Models.Catalog
{
    /// <summary>
    /// Represents a foreign key reference pointing to a parent table and attribute.
    /// </summary>
    /// <example>
    /// <code>
    /// var refTarget = new Reference { ReferenceTableName = "Users", ReferenceAttributeName = "Id" };
    /// </code>
    /// </example>
    [Serializable]
    [XmlRoot("References")]
    public class Reference
    {
        /// <summary>Gets or sets the name of the referenced primary table.</summary>
        [XmlElement("RefTable")]
        public required string ReferenceTableName { get; set; }

        /// <summary>Gets or sets the name of the referenced primary key attribute.</summary>
        [XmlElement("RefAttribute")]
        public required string ReferenceAttributeName { get; set; }
    }
}
