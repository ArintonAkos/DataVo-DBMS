using DataVo.Core.Logging;
using DataVo.Core.Models.DQL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Parser.Types;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Cache;
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
            if (literalNode.Value is string s && s == "1=1") return true;
            return false;
        }

        if (node is not BinaryExpressionNode binNode)
        {
            throw new Exception($"Unsupported HAVING predicate node type: {node.GetType().Name}");
        }

        if (binNode.Operator == "AND")
        {
            return EvaluatePredicate(binNode.Left, row) && EvaluatePredicate(binNode.Right, row);
        }

        if (binNode.Operator == "OR")
        {
            return EvaluatePredicate(binNode.Left, row) || EvaluatePredicate(binNode.Right, row);
        }

        object? leftValue = ResolveNodeValue(binNode.Left, row);
        object? rightValue = ResolveNodeValue(binNode.Right, row);
        string op = binNode.Operator;

        return op switch
        {
            "=" => EvaluateEquality(leftValue, rightValue),
            "!=" => !EvaluateEquality(leftValue, rightValue),
            "<" => CompareDynamics(leftValue, rightValue) < 0,
            ">" => CompareDynamics(leftValue, rightValue) > 0,
            "<=" => CompareDynamics(leftValue, rightValue) <= 0,
            ">=" => CompareDynamics(leftValue, rightValue) >= 0,
            _ => throw new Exception($"Unsupported HAVING operator: {op}")
        };
    }

    private static bool EvaluateEquality(object? val1, object? val2)
    {
        if (val1 is string s1 && val2 is string s2)
        {
            s1 = s1.Trim('\'');
            s2 = s2.Trim('\'');
            return s1 == s2;
        }

        if (val1 == null && val2 == null) return true;
        if (val1 == null || val2 == null) return false;

        if (val1 is IConvertible c1 && val2 is IConvertible c2)
        {
            try
            {
                double d1 = c1.ToDouble(null);
                double d2 = c2.ToDouble(null);
                return Math.Abs(d1 - d2) < 0.0001; // float tolerance
            }
            catch { }
        }

        return val1.Equals(val2);
    }

    private static int CompareDynamics(object? leftVal, object? rightVal)
    {
        if (leftVal is string s1 && rightVal is string s2)
        {
            s1 = s1.Trim('\'');
            s2 = s2.Trim('\'');
            return string.Compare(s1, s2, StringComparison.Ordinal);
        }

        if (leftVal is IConvertible c1 && rightVal is IConvertible c2)
        {
            try
            {
                double d1 = c1.ToDouble(null);
                double d2 = c2.ToDouble(null);
                return d1.CompareTo(d2);
            }
            catch { }
        }

        return Comparer<object?>.Default.Compare(leftVal, rightVal);
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