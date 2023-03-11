using Server.Parser.Commands;
using System.Text.RegularExpressions;

namespace Server.Models.DDL
{
    public class CreateTableModel
    {
        public string TableName { get; set; }

        public List<Field> Fields { get; set; }

        public List<String> PrimaryKeys
        {
            get
            {
                return Fields.FindAll(f => f.IsPrimaryKey == true)
                    .Select(f => f.Name)
                    .ToList();
            }
        }

        public List<ForeignKey> ForeignKeys
        {
            get
            {
                return Fields.FindAll(f => f.ForeignKey != null)
                    .Select(f => f.ForeignKey!)
                    .ToList();
            }
        }

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

        public Table ToTable()
        {
            return new()
            {
                Fields = Fields,
                PrimaryKeys = PrimaryKeys,
                ForeignKeys = ForeignKeys,
            };
        }
    }
}
