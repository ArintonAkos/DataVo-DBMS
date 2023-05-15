using Server.Models.Statement;

namespace Server.Parser.Statements;

internal class Where
{
    private readonly WhereModel? _model;

    public Where(string match) => _model = WhereModel.FromString(match);

    public HashSet<string> Evaluate(string tableName, string databaseName)
    {
        if (_model is null)
        {
            throw new Exception("Cannot evaluate null where statement.");
        }

        return new StatementEvaluator(databaseName, tableName).Evaluate(_model.Statement);
    }

    public bool IsEvaluatable() => _model is not null;
}