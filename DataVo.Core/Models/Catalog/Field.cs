using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Text.RegularExpressions;
using System.Xml.Serialization;
using DataVo.Core.Enums;

namespace DataVo.Core.Models.Catalog;

[XmlType("Attribute")]
[Serializable]
public class Field
{
    [XmlIgnore]
    [Required(ErrorMessage = "Field must belong to a table!")]
    public required string Table { get; set; }

    [XmlAttribute]
    [Required(ErrorMessage = "Field must have a type!")]
    public required DataTypes Type { get; set; }

    [XmlAttribute]
    [Required(ErrorMessage = "Field must have a name!")]
    public required string Name { get; set; }

    [XmlAttribute]
    [DefaultValue(value: -1)]
    public int IsNull { get; set; }

    [XmlAttribute]
    [DefaultValue(value: 0)]
    public int Length { get; set; }

    [XmlIgnore] public bool? IsPrimaryKey { get; set; }

    [XmlIgnore] public ForeignKey? ForeignKey { get; set; }

    [XmlIgnore] public bool? IsUnique { get; set; }

}