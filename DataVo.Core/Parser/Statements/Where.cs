using DataVo.Core.Models.Statement;
using DataVo.Core.Parser.Statements.Mechanism;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;
using DataVo.Core.Utils;
using DataVo.Core.Parser.Binding;
using DataVo.Core.Models.Statement.Utils;

namespace DataVo.Core.Parser.Statements;

internal class Where
{
    private readonly WhereModel? _model;
    private readonly TableDetail? _fromTable;
    private ExpressionNode? _boundStatement;

    public Where(ExpressionNode node)
    {
        _model = WhereModel.FromExpression(node);
    }

    public Where(ExpressionNode node, TableDetail fromTable)
    {
        _model = WhereModel.FromExpression(node);
        _fromTable = fromTable;
    }

    public HashSet<long> EvaluateWithoutJoin(string tableName, string databaseName)
    {
        if (_model is null)
        {
            throw new Exception("Cannot evaluate null where statement.");
        }

        var tableService = new TableService(databaseName);
        tableService.AddTableDetail(new TableDetail(tableName, null));
        var boundStatement = BindStatement(tableService);

        return new StatementEvaluatorWOJoin(databaseName, tableName).Evaluate(boundStatement);
    }

    public ListedTable EvaluateWithJoin(TableService tableService, Join joinStatements)
    {
        if (_model is null)
        {
            throw new Exception("Cannot evaluate null where statement.");
        }

        var boundStatement = BindStatement(tableService);

        return new StatementEvaluator(tableService, joinStatements, _fromTable!).Evaluate(boundStatement)
            .Select(row => row.Value)
            .ToListedTable();
    }

    private ExpressionNode BindStatement(TableService tableService)
    {
        if (_boundStatement is not null)
        {
            return _boundStatement;
        }

        if (_model is null)
        {
            throw new Exception("Cannot bind null where statement.");
        }

        _boundStatement = SelectBinder.BindWhere(_model.Statement, tableService)
            ?? throw new Exception("Failed to bind where statement.");

        return _boundStatement;
    }

    public bool IsEvaluatable() => _model is not null;

    public ExpressionNode? GetExpression() => _model?.Statement;
}