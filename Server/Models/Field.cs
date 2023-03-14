using MongoDB.Bson.IO;
using Newtonsoft.Json.Linq;
using Server.Enums;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Serialization;
using static MongoDB.Driver.WriteConcern;

namespace Server.Models
{
    [XmlType("Attribute")]
    [Serializable]
    public class Field
    {
        [XmlIgnore]
        [Required(ErrorMessage = "Field must belong to a table!")]
        public string Table { get; set; }

        [XmlAttribute]
        [Required(ErrorMessage = "Field must have a type!")]
        public DataTypes Type { get; set; }

        [XmlAttribute]
        [Required(ErrorMessage = "Field must have a name!")]
        public string Name { get; set; }

        [XmlAttribute, DefaultValue(-1)]
        public int IsNull { get; set; }

        [XmlAttribute, DefaultValue(0)]
        public int Length { get; set; }

        [XmlIgnore]
        public bool? IsPrimaryKey { get; set; }

        [XmlIgnore]
        public ForeignKey? ForeignKey { get; set; }

        public static Field FromMatch(Match match, string tableName)
        {
            DataTypes type = (DataTypes)Enum.Parse(typeof(DataTypes), GetTypeString(match.Groups["Type"].Value), true);

            Field field = new()
            {
                Name = match.Groups["FieldName"].Value,
                Type = type,
                Table = tableName,
                IsPrimaryKey = !string.IsNullOrEmpty(match.Groups["PrimaryKey"]?.Value),
                IsNull = -1,
            };

            if (field.Type == DataTypes.Varchar)
            {
                field.Length = int.Parse(match.Groups["Length"].Value);
            }

            if (!string.IsNullOrEmpty(match.Groups["ForeignKey"]?.Value))
            {
                field.CreateForeignKey(new Field()
                {
                    Table = match.Groups["ForeignTable"].Value,
                    Name = match.Groups["ForeignColumn"].Value,
                    Type = field.Type
                });
            }

            return field;
        }

        public void CreateForeignKey(Field reference)
        {
            ForeignKey = new ForeignKey
            {
                SourceField = this,
                DestinationField = reference
            };
        }

        private static string GetTypeString(string type)
        {
            if (type.Contains("int"))
            {
                return "int";
            }

            if (type.Contains("float"))
            {
                return "float";
            }

            return "varchar";
        }
    }
}
