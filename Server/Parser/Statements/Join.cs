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

    // Lista<Táblanév, <Oszlopnév érték>>
    public List<Dictionary<string, dynamic>> Evaluate(List<Dictionary<string, dynamic>> table)
    {
        
    }
}