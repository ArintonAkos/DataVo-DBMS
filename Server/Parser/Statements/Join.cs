using System.Text.RegularExpressions;
using Server.Models.Statement;
using Server.Parser.Utils;

namespace Server.Parser.Statements;

public class Join
{
    private readonly bool _isValid;
    private readonly JoinModel _model;

    public Join(string joinStatement)
    {
        var match = Regex.Match(joinStatement, Patterns.Join, RegexOptions.IgnoreCase);

        if (match.Success)
        {
            _model = JoinModel.FromMatch(match);
            _isValid = true;
        }
    }

    public bool ContainsJoin() => _isValid;

    public HashSet<string> Evaluate(string tableName, string databaseName) => new();
}