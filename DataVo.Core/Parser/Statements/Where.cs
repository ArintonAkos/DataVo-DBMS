using DataVo.Core.Models.Statement;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Statements.Mechanism;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.Statements;

internal class Where
{
    private readonly WhereModel? _model;
    private readonly TableDetail? _fromTable;

    public Where(string match)
    {
        _model = WhereModel.FromString(match);
    }

    public Where(string match, TableDetail fromTable)
    {
        _model = WhereModel.FromString(match);
        _fromTable = fromTable;
    }

    public HashSet<string> EvaluateWithoutJoin(string tableName, string databaseName)
    {
        if (_model is null)
        {
            throw new Exception("Cannot evaluate null where statement.");
        }

        return new StatementEvaluatorWOJoin(databaseName, tableName).Evaluate(_model.Statement);
    }

    public ListedTable EvaluateWithJoin(TableService tableService, Join joinStatements)
    {
        if (_model is null)
        {
            throw new Exception("Cannot evaluate null where statement.");
        }

        return new StatementEvaluator(tableService, joinStatements, _fromTable!).Evaluate(_model.Statement)
            .Select(row => row.Value)
            .ToListedTable();
    }

    public bool IsEvaluatable() => _model is not null;
}