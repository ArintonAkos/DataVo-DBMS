using Server.Models.Statement;
using Server.Services;

namespace Server.Parser.Statements;

internal class Where
{
    private readonly WhereModel? _model;

    public Where(string match) => _model = WhereModel.FromString(match);

    public HashSet<string> EvaluateWithoutJoin(string tableName, string databaseName)
    {
        if (_model is null)
        {
            throw new Exception("Cannot evaluate null where statement.");
        }

        return new StatementEvaluator(databaseName, tableName).EvaluateWithoutJoin(_model.Statement);
    }

    public Dictionary<string, Dictionary<string, dynamic>> EvaluateWithJoin(TableService tableService, Join joinStatements)
    {
        if (_model is null)
        {
            throw new Exception("Cannot evaluate null where statement.");
        }

        return new StatementEvaluator(tableService, joinStatements).Evaluate(_model.Statement);
    }

    public bool IsEvaluatable() => _model is not null;
}