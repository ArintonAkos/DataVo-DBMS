using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Text.RegularExpressions;
using System.Xml.Serialization;
using DataVo.Core.Enums;

namespace DataVo.Core.Models.Catalog;

/// <summary>
/// Represents a structured field (attribute) definition within a database table.
/// Defines the schema constraints and type information for columns.
/// </summary>
/// <example>
/// <code>
/// var field = new Field { Name = "Id", Type = DataTypes.Int, Table = "Users" };
/// </code>
/// </example>
[XmlType("Attribute")]
[Serializable]
public class Field
{
    /// <summary>Gets or sets the table name this field belongs to.</summary>
    [XmlIgnore]
    [Required(ErrorMessage = "Field must belong to a table!")]
    public required string Table { get; set; }

    /// <summary>Gets or sets the data type of the field.</summary>
    [XmlAttribute]
    [Required(ErrorMessage = "Field must have a type!")]
    public required DataTypes Type { get; set; }

    /// <summary>Gets or sets the name of the field.</summary>
    [XmlAttribute]
    [Required(ErrorMessage = "Field must have a name!")]
    public required string Name { get; set; }

    /// <summary>Gets or sets whether the field allows NULL values (1) or not (0/-1).</summary>
    [XmlAttribute]
    [DefaultValue(value: -1)]
    public int IsNull { get; set; }

    /// <summary>Gets or sets the maximum length for variable-length fields.</summary>
    [XmlAttribute]
    [DefaultValue(value: 0)]
    public int Length { get; set; }

    /// <summary>Gets or sets whether this field is part of the primary key.</summary>
    [XmlIgnore] public bool? IsPrimaryKey { get; set; }

    /// <summary>Gets or sets the foreign key constraints applied to this field.</summary>
    [XmlIgnore] public ForeignKey? ForeignKey { get; set; }

    /// <summary>Gets or sets whether this field has a UNIQUE constraint.</summary>
    [XmlIgnore] public bool? IsUnique { get; set; }

    /// <summary>Gets or sets the default value applied during insertion if missing.</summary>
    [XmlAttribute] public string? DefaultValue { get; set; }

}