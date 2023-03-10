using Newtonsoft.Json;
using Server.Parser.Commands;
using Server.Utils;
using System;
using System.Collections.Generic;
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

        [XmlArray("Columns")]
        [XmlArrayItem("Column")]
        [Required(ErrorMessage = "Table fields are missing!")]
        public List<Field> Fields { get; set; }
       
        private CreateTableModel() { }

        public CreateTableModel(string tableName, List<Field> fields)
        {
            this.TableName = tableName;
            this.Fields = fields;
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
