using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Statements;

namespace DataVo.Core.Models.Statement;

internal class WhereModel
{
    public required Node Statement { get; set; }

    public static WhereModel? FromString(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return null;
        }

        value = value.Trim().Remove(startIndex: 0, count: 5);
        var statement = StatementParser.Parse(value);

        return new WhereModel
        {
            Statement = statement,
        };
    }

    public static WhereModel FromNode(Node node)
    {
        return new WhereModel
        {
            Statement = node
        };
    }
}