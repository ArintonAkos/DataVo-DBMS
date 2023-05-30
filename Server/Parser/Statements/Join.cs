using System.Text.RegularExpressions;
using Server.Models.Statement;

namespace Server.Parser.Statements;

public class Join
{
    private readonly bool _isValid;
    public readonly JoinModel Model;

    public Join(Group group)
    {
        if (group.Success)
        {
            Model = JoinModel.FromMatchGroup(group);
            _isValid = true;
        }
        else
        {
            _isValid = false;
            Model = new();
        }
    }

    public bool ContainsJoin() => _isValid;

    public HashSet<string> Evaluate(string tableName, string databaseName) => new();
}