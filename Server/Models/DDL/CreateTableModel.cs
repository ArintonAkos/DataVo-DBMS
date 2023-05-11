using System.Text.RegularExpressions;
using Server.Models.Catalog;
using Server.Parser.Utils;

namespace Server.Models.DDL;

public class CreateTableModel
{
    private CreateTableModel()
    {
    }

    public CreateTableModel(string tableName, List<Field> fields)
    {
        TableName = tableName;
        Fields = fields;
    }

    public string TableName { get; set; }

    public List<Field> Fields { get; set; }

    public List<string> PrimaryKeys
    {
        get
        {
            return Fields.FindAll(f => f.IsPrimaryKey == true)
                .Select(f => f.Name)
                .ToList();
        }
    }

    public List<string> UniqueAttributes
    {
        get
        {
            return Fields.FindAll(f => f.IsUnique == true)
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

    public static CreateTableModel FromMatch(Match match)
    {
        string tableName = match.Groups["TableName"].Value;
        List<Field> fields = new();

        string pattern = Patterns.Column;
        var columns = Regex.Match(match.Groups["Columns"].Value, pattern,
            RegexOptions.IgnoreCase | RegexOptions.Multiline);

        while (columns.Success)
        {
            fields.Add(Field.FromMatch(columns, tableName));

            columns = columns.NextMatch();
        }


        return new CreateTableModel(tableName, fields);
    }

    public Table ToTable() =>
        new()
        {
            TableName = TableName,
            Fields = Fields,
            PrimaryKeys = PrimaryKeys,
            UniqueAttributes = UniqueAttributes,
            ForeignKeys = ForeignKeys,
            IndexFiles = new List<IndexFile>(),
        };
}