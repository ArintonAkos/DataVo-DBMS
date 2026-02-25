using DataVo.Core.Logging;
using DataVo.Core.Models.DQL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Parser.Types;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Cache;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Enums;
using DataVo.Core.Constants;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.DQL;

internal class Select(SelectStatement ast) : BaseDbAction
{
    private readonly SelectModel _model = SelectModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        try
        {
            string database = ValidateDatabase(session);

            ListedTable result = EvaluateStatements();

            GroupedTable groupedTable = GroupResults(result);

            result = AggregateGroupedTable(groupedTable);
            result = ApplyHaving(result);
            result = ApplyOrderBy(result);

            Logger.Info($"Rows selected: {result.Count}");
            Messages.Add($"Rows selected: {result.Count}");

            Fields = CreateFieldsFromColumns(result);
            Data = CreateDataFromResult(result, Fields);
        }
        catch (Exception ex)
        {
            Messages.Add(ex.ToString());
            Logger.Error(ex.ToString());
        }
    }

    private GroupedTable GroupResults(ListedTable tableData)
    {
        return _model.GroupByStatement.Evaluate(tableData);
    }

    private ListedTable AggregateGroupedTable(GroupedTable groupedTable)
    {
        return _model.AggregateStatement.Perform(groupedTable);
    }

    private ListedTable ApplyHaving(ListedTable tableData)
    {
        var havingExpression = _model.GetHavingExpression();
        if (havingExpression == null)
        {
            return tableData;
        }

        var filtered = tableData
            .Where(row => EvaluatePredicate(havingExpression, row))
            .ToList();

        return new ListedTable(filtered);
    }

    private ListedTable ApplyOrderBy(ListedTable tableData)
    {
        var orderByExpression = _model.GetOrderByExpression();
        if (orderByExpression == null || orderByExpression.Columns.Count == 0)
        {
            return tableData;
        }

        IOrderedEnumerable<JoinedRow>? ordered = null;

        foreach (var orderCol in orderByExpression.Columns)
        {
            Func<JoinedRow, object?> keySelector = row => ResolveColumnValue(row, orderCol.Column.Name);

            if (ordered == null)
            {
                ordered = orderCol.IsAscending
                    ? tableData.OrderBy(keySelector, DynamicObjectComparer.Instance)
                    : tableData.OrderByDescending(keySelector, DynamicObjectComparer.Instance);
            }
            else
            {
                ordered = orderCol.IsAscending
                    ? ordered.ThenBy(keySelector, DynamicObjectComparer.Instance)
                    : ordered.ThenByDescending(keySelector, DynamicObjectComparer.Instance);
            }
        }

        return ordered == null ? tableData : new ListedTable(ordered.ToList());
    }

    private string ValidateDatabase(Guid session)
    {
        string databaseName = CacheStorage.Get(session)
            ?? throw new Exception("No database in use!");

        bool hasMissingColumns = _model.Validate(databaseName);

        if (!_model.JoinStatement.ContainsJoin() && hasMissingColumns)
        {
            throw new Exception("Invalid columns specified'");
        }

        return databaseName;
    }

    private void ValidateColumns(string databaseName)
    {
        if (_model.Validate(databaseName))
        {
            throw new Exception("Invalid columns specified'");
        }
    }

    private ListedTable EvaluateStatements()
    {
        ListedTable result;

        if (_model.WhereStatement.IsEvaluatable())
        {
            result = _model.WhereStatement.EvaluateWithJoin(_model.TableService!, _model.JoinStatement);
        }
        else if (_model.JoinStatement.ContainsJoin())
        {
            result = EvaluateJoin();
        }
        else
        {
            var listResult = _model.FromTable!.TableContentValues!
                .Select(row => new JoinedRow(_model.FromTable.TableName, row.ToRow()))
                .ToList();

            result = new ListedTable(listResult);
        }

        return result;
    }

    private ListedTable EvaluateJoin()
    {
        HashedTable groupedInitialTable = [];

        foreach (var row in _model.FromTable.TableContent!)
        {
            groupedInitialTable.Add(row.Key, new JoinedRow(_model.FromTable.TableName, row.Value.ToRow()));
        }

        return _model.JoinStatement!.Evaluate(groupedInitialTable).ToListedTable();
    }


    private List<string> CreateFieldsFromColumns(ListedTable filteredTable)
    {
        List<string> selectedColumns = _model.GetSelectedColumns();
        List<string> fields = [];

        foreach (string column in selectedColumns)
        {
            string[] splittedColumn = column.Split('.');
            string tableName = splittedColumn[0];
            string columnName = splittedColumn[1];

            if (_model.JoinStatement.ContainsJoin())
            {
                string inUseNameOfTable = _model.TableService!.GetTableDetailByAliasOrName(tableName).GetTableNameInUse();
                fields.Add($"{inUseNameOfTable}.{columnName}");
            }
            else
            {
                fields.Add(columnName);
            }
        }

        JoinedRow? firstRow = filteredTable.FirstOrDefault();
        if (firstRow != null && firstRow.ContainsKey(GroupBy.HASH_VALUE))
        {
            foreach (var columnName in firstRow[GroupBy.HASH_VALUE].Keys)
            {
                fields.Add(columnName);
            }
        }

        return fields;
    }

    private List<Dictionary<string, dynamic>> CreateDataFromResult(ListedTable filteredTable, List<string> fieldsList)
    {
        List<Dictionary<string, dynamic>> result = new();

        foreach (var row in filteredTable)
        {
            Dictionary<string, dynamic> data = new();
            int fieldIndex = 0;

            foreach (string nameAssembly in _model.GetSelectedColumns())
            {
                // Format could be "TableName.ColumnName" or "TableName.ColumnName AS AliasName"
                string extractedOriginalName = nameAssembly;
                if (extractedOriginalName.Contains(" AS "))
                {
                    extractedOriginalName = extractedOriginalName.Split(" AS ")[0];
                }

                string[] splittedAssembly = extractedOriginalName.Split('.');
                string tableName = splittedAssembly[0];
                string columnName = splittedAssembly[1];

                string currentFieldName = fieldsList[fieldIndex++];
                data[currentFieldName] = row[tableName][columnName];
            }

            if (row.ContainsKey(GroupBy.HASH_VALUE))
            {
                foreach (var columnName in row[GroupBy.HASH_VALUE].Keys)
                {
                    string currentFieldName = fieldsList[fieldIndex++];
                    data[currentFieldName] = row[GroupBy.HASH_VALUE][columnName];
                }
            }

            result.Add(data);
        }

        return result;
    }

    private bool EvaluatePredicate(ExpressionNode node, JoinedRow row)
    {
        if (node is LiteralNode literalNode)
        {
            if (literalNode.Value is bool b) return b;
            if (literalNode.Value is string s && s == SqlLiterals.TrueExpression) return true;
            return false;
        }

        if (node is not BinaryExpressionNode binNode)
        {
            throw new Exception($"Unsupported HAVING predicate node type: {node.GetType().Name}");
        }

        if (binNode.Operator == Operators.AND)
        {
            return EvaluatePredicate(binNode.Left, row) && EvaluatePredicate(binNode.Right, row);
        }

        if (binNode.Operator == Operators.OR)
        {
            return EvaluatePredicate(binNode.Left, row) || EvaluatePredicate(binNode.Right, row);
        }

        object? leftValue = ResolveNodeValue(binNode.Left, row);
        object? rightValue = ResolveNodeValue(binNode.Right, row);
        string op = binNode.Operator;

        return op switch
        {
            Operators.EQUALS => EvaluateEquality(leftValue, rightValue),
            Operators.NOT_EQUALS => !EvaluateEquality(leftValue, rightValue),
            Operators.LESS_THAN => CompareDynamics(leftValue, rightValue) < 0,
            Operators.GREATER_THAN => CompareDynamics(leftValue, rightValue) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => CompareDynamics(leftValue, rightValue) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => CompareDynamics(leftValue, rightValue) >= 0,
            _ => throw new Exception($"Unsupported HAVING operator: {op}")
        };
    }

    private static bool EvaluateEquality(object? val1, object? val2)
    {
        return ExpressionValueComparer.AreEqual(val1, val2, trimQuotedStrings: true, useNumericTolerance: true);
    }

    private static int CompareDynamics(object? leftVal, object? rightVal)
    {
        return ExpressionValueComparer.Compare(leftVal, rightVal, trimQuotedStrings: true);
    }

    private object? ResolveNodeValue(ExpressionNode node, JoinedRow row)
    {
        if (node is LiteralNode literalNode)
        {
            return literalNode.Value;
        }
        else if (node is ResolvedColumnRefNode resolvedColNode)
        {
            return ResolveColumnValue(row, $"{resolvedColNode.TableName}.{resolvedColNode.Column}");
        }
        else if (node is ColumnRefNode colNode)
        {
            return ResolveColumnValue(row, string.IsNullOrEmpty(colNode.TableOrAlias) ? colNode.Column : $"{colNode.TableOrAlias}.{colNode.Column}");
        }

        throw new Exception($"Unsupported HAVING value node type: {node.GetType().Name}");
    }

    private object? ResolveColumnValue(JoinedRow row, string columnReference)
    {
        var parseResult = _model.TableService!.ParseAndFindTableNameByColumn(columnReference);
        string tableName = parseResult.Item1;
        string columnName = parseResult.Item2;

        if (!row.ContainsKey(tableName) || !row[tableName].ContainsKey(columnName))
        {
            throw new Exception($"Column '{columnReference}' not found in row scope.");
        }

        return row[tableName][columnName];
    }

    private sealed class DynamicObjectComparer : IComparer<object?>
    {
        public static readonly DynamicObjectComparer Instance = new();

        public int Compare(object? x, object? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;

            if (x is IComparable comparableX)
            {
                try
                {
                    return comparableX.CompareTo(y);
                }
                catch
                {
                }
            }

            return string.CompareOrdinal(x.ToString(), y.ToString());
        }
    }
}