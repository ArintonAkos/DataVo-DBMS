using MongoDB.Bson.IO;
using Newtonsoft.Json.Linq;
using Server.Enums;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Serialization;
using static MongoDB.Driver.WriteConcern;

namespace Server.Models.DDL
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
        public String Name { get; set; }

        [XmlAttribute, DefaultValue(-1)]
        public Int32 IsNull { get; set; }

        [XmlAttribute, DefaultValue(0)]
        public Int32 Length { get; set; }

        [XmlIgnore]
        public Boolean? IsPrimaryKey { get; set; }

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
                IsPrimaryKey = !String.IsNullOrEmpty(match.Groups["PrimaryKey"]?.Value),
                IsNull = -1,
            };

            if (field.Type == DataTypes.Varchar)
            {
                field.Length = Int32.Parse(match.Groups["Length"].Value);
            }

            if (!String.IsNullOrEmpty(match.Groups["ForeignKey"]?.Value))
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

        private static String GetTypeString(String type)
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
