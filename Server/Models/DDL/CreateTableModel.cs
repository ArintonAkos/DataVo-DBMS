using Newtonsoft.Json;
using Server.Parser.Commands;
using Server.Utils;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Server.Models.DDL
{
    [XmlRoot("Table")]
    [Serializable]
    public class CreateTableModel
    {
        [XmlAttribute]
        [Required(ErrorMessage = "Table name not found!")]
        public string TableName { get; set; }

        [XmlArray("Structure")]
        [XmlArrayItem("Attribute")]
        [Required(ErrorMessage = "Table fields are missing!")]
        public List<Field> Fields { get; set; }

        [XmlArray("primaryKey")]
        [XmlArrayItem("pkAttribute")]
        public List<string> PrimaryKeys { get; set; }

        [XmlArray("foreignKeys")]
        [XmlArrayItem("foreignKey")]
        public List<ForeignKey> ForeignKeys { get; set; }

        [XmlIgnore]
        public bool ForeignKeysSpecified => ForeignKeys.Count > 0;

        private CreateTableModel() { }

        public CreateTableModel(string tableName, List<Field> fields)
        {
            this.TableName = tableName;
            this.Fields = fields;
            this.PrimaryKeys = new ();
            this.ForeignKeys= new ();

            foreach (Field field in Fields)
            {
                if (field.IsPrimaryKey == true)
                {
                    PrimaryKeys.Add(field.Name);
                }

                if (field.ForeignKey != null)
                {
                    ForeignKeys.Add(field.ForeignKey);
                }
            }
        }

        public static CreateTableModel FromMatch(Match match)
        {
            String tableName = match.Groups["TableName"].Value;
            List<Field> fields = new();

            String pattern = Patterns.Column;
            Match columns = Regex.Match(match.Groups["Columns"].Value, pattern, RegexOptions.IgnoreCase | RegexOptions.Multiline);

            while (columns.Success)
            {
                fields.Add(Field.FromMatch(columns, tableName));

                columns = columns.NextMatch();
            }


            return new CreateTableModel(tableName, fields);
        }
    }
}
