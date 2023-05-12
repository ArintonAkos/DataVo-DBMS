using Server.Models.Statement;

namespace Server.Parser.Statements;

internal class Where
{
    private readonly WhereModel _model;

    public Where(string match) => _model = WhereModel.FromString(match);

    public HashSet<string> Evaluate(string tableName, string databaseName)
    {
        return new StatementEvaluator(databaseName, tableName).Evaluate(_model.Statement);
    }
}