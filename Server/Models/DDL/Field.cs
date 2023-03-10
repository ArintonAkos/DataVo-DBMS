using Newtonsoft.Json.Linq;
using Server.Enums;
using System.ComponentModel.DataAnnotations;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Server.Models.DDL
{
    [Serializable]
    public class Field
    {
        [Required(ErrorMessage = "Field must belong to a table!")]
        public string Table { get; set; }

        [Required(ErrorMessage = "Field must have a type!")]
        public DataTypes Type { get; set; }

        [Required(ErrorMessage = "Field must have a name!")]
        public String Name { get; set; }

        public Boolean? IsNull { get; set; }

        public Int32? Length { get; set; }

        public Boolean? IsPrimaryKey { get; set; }

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
