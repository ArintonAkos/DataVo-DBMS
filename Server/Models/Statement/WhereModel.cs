using Server.Models.Statement.Utils;
using Server.Parser.Statements;

namespace Server.Models.Statement;

internal class WhereModel
{
    public Node Statement { get; set; }

    public static WhereModel? FromString(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return null;
        }

        value = value.Remove(startIndex: 0, count: 5);
        var statement = StatementParser.Parse(value);

        return new WhereModel
        {
            Statement = statement,
        };
    }
}