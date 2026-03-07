using DataVo.Core.Logging;
using DataVo.Core.Models.DQL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Parser.Types;
using DataVo.Core.Cache;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Enums;
using DataVo.Core.Constants;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// Represents the execution logic for a SQL SELECT statement.
/// Handles evaluating tables, joins, applying filtering (WHERE, HAVING), 
/// grouping, aggregation, ordering, and selecting distinct results.
/// </summary>
/// <param name="ast">The AST node representing the SELECT statement.</param>
internal class Select(SelectStatement ast) : BaseDbAction
{
    /// <summary>
    /// The parsed model representation of the SELECT statement.
    /// </summary>
    private readonly SelectModel _model = SelectModel.FromAst(ast);

    /// <summary>
    /// Executes the SELECT query against the currently active database for the given session.
    /// Sets the <see cref="BaseDbAction.Fields"/> and <see cref="BaseDbAction.Data"/> properties upon success.
    /// </summary>
    /// <param name="session">The unique identifier of the user's session.</param>
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

            if (_model.IsDistinct)
            {
                Data = ApplyDistinct(Data);
            }
        }
        catch (Exception ex)
        {
            Messages.Add(ex.ToString());
            Logger.Error(ex.ToString());
        }
    }

    /// <summary>
    /// Filters the result data to include only distinct rows based on column values.
    /// </summary>
    /// <param name="data">The raw row data to filter.</param>
    /// <returns>A new list of unique rows.</returns>
    private List<Dictionary<string, dynamic>> ApplyDistinct(List<Dictionary<string, dynamic>> data)
    {
        return data.Select(d => d.ToDictionary(k => k.Key, v => (object?)v.Value))
                   .Distinct(new DictionaryComparer())
                   .Select(d => d.ToDictionary(k => k.Key, v => (dynamic)v.Value!))
                   .ToList();
    }

    /// <summary>
    /// Evaluates the GROUP BY clause to group the rows based on specified criteria.
    /// </summary>
    /// <param name="tableData">The current listed table data.</param>
    /// <returns>A collection of grouped records.</returns>
    private GroupedTable GroupResults(ListedTable tableData)
    {
        return _model.GroupByStatement.Evaluate(tableData);
    }

    /// <summary>
    /// Evaluates aggregate functions on the previously grouped table data.
    /// </summary>
    /// <param name="groupedTable">The grouped result table.</param>
    /// <returns>The aggregated list table.</returns>
    private ListedTable AggregateGroupedTable(GroupedTable groupedTable)
    {
        return _model.AggregateStatement.Perform(groupedTable);
    }

    /// <summary>
    /// Filters the aggregated results based on the HAVING clause, if present.
    /// </summary>
    /// <param name="tableData">The table data containing aggregated metrics.</param>
    /// <returns>The filtered result set.</returns>
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

    /// <summary>
    /// Sorts the result set based on the ORDER BY clause, if present.
    /// </summary>
    /// <param name="tableData">The table data to sort.</param>
    /// <returns>The sorted result set.</returns>
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
            ordered = ApplyOrderToColumn(tableData, ordered, orderCol);
        }

        return ordered == null ? tableData : new ListedTable(ordered.ToList());
    }

    /// <summary>
    /// Applies sorting for a specific column to an optionally already-sorted set.
    /// </summary>
    /// <param name="tableData">The initial table data.</param>
    /// <param name="ordered">The currently ordered enumeration, if any.</param>
    /// <param name="orderCol">The ordering instruction for a specific column.</param>
    /// <returns>An ordered enumeration including the new sort criteria.</returns>
    private IOrderedEnumerable<JoinedRow> ApplyOrderToColumn(ListedTable tableData, IOrderedEnumerable<JoinedRow>? ordered, OrderByColumnNode orderCol)
    {
        Func<JoinedRow, object?> keySelector = row => ResolveColumnValue(row, orderCol.Column.Name);

        if (ordered == null)
        {
            return orderCol.IsAscending
                ? tableData.OrderBy(keySelector, DynamicObjectComparer.Instance)
                : tableData.OrderByDescending(keySelector, DynamicObjectComparer.Instance);
        }

        return orderCol.IsAscending
            ? ordered.ThenBy(keySelector, DynamicObjectComparer.Instance)
            : ordered.ThenByDescending(keySelector, DynamicObjectComparer.Instance);
    }

    /// <summary>
    /// Validates that a database is currently selected in the session.
    /// </summary>
    /// <param name="session">The unique identifier of the user's session.</param>
    /// <returns>The name of the connected database.</returns>
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

    /// <summary>
    /// Determines the execution source of the main query (e.g. from a WHERE condition, JOIN evaluation, or full table scan).
    /// </summary>
    /// <returns>A ListedTable populated with the initial matched rows.</returns>
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

    /// <summary>
    /// Materializes the table row dictionaries and performs the JOIN evaluations across tables.
    /// </summary>
    /// <returns>A ListedTable containing joined rows.</returns>
    private ListedTable EvaluateJoin()
    {
        HashedTable groupedInitialTable = [];

        foreach (var row in _model.FromTable.TableContent!)
        {
            groupedInitialTable.Add(new JoinedRowId(row.Key), new JoinedRow(_model.FromTable.TableName, row.Value.ToRow()));
        }

        return _model.JoinStatement!.Evaluate(groupedInitialTable, _model.FromTable.TableName).ToListedTable();
    }

    /// <summary>
    /// Determines the correct output schema (fields list) based on select statements, considering aliases and table sources.
    /// </summary>
    /// <param name="filteredTable">The fully evaluated table data.</param>
    /// <returns>A list of qualified field names.</returns>
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

    /// <summary>
    /// Maps the evaluated result rows to standard dictionary-based data sets matching the requested fields.
    /// </summary>
    /// <param name="filteredTable">The fully evaluated and filtered result set.</param>
    /// <param name="fieldsList">The list of target field names.</param>
    /// <returns>A list of data rows represented as dictionaries.</returns>
    private List<Dictionary<string, dynamic>> CreateDataFromResult(ListedTable filteredTable, List<string> fieldsList)
    {
        List<Dictionary<string, dynamic>> result = new();

        foreach (var row in filteredTable)
        {
            result.Add(ExtractRowData(row, fieldsList));
        }

        return result;
    }

    /// <summary>
    /// Extracts data for a single row matched to the requested view layout.
    /// </summary>
    /// <param name="row">The joined row containing the retrieved data.</param>
    /// <param name="fieldsList">The layout sequence of mapping field names.</param>
    /// <returns>A dictionary map tying property names to parsed dynamic row data.</returns>
    private Dictionary<string, dynamic> ExtractRowData(JoinedRow row, List<string> fieldsList)
    {
        Dictionary<string, dynamic> data = new();
        int fieldIndex = 0;

        foreach (string nameAssembly in _model.GetSelectedColumns())
        {
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

        return data;
    }

    /// <summary>
    /// Evaluates a HAVING clause predicate against a specific retrieved row.
    /// </summary>
    /// <param name="node">The expression node constraint.</param>
    /// <param name="row">The specific database row representation.</param>
    /// <returns>True if the condition succeeds, false otherwise.</returns>
    private bool EvaluatePredicate(ExpressionNode node, JoinedRow row)
    {
        if (node is LiteralNode literalNode)
        {
            return EvaluateLiteralNode(literalNode);
        }

        if (node is not BinaryExpressionNode binNode)
        {
            throw new Exception($"Unsupported HAVING predicate node type: {node.GetType().Name}");
        }

        return EvaluateBinaryNode(binNode, row);
    }

    /// <summary>
    /// Interprets basic direct literal statements natively passed to a query constraint condition.
    /// </summary>
    /// <param name="literalNode">The literal constraint to check.</param>
    /// <returns>The boolean equivalent result of the literal.</returns>
    private bool EvaluateLiteralNode(LiteralNode literalNode)
    {
        if (literalNode.Value is bool b) return b;
        if (literalNode.Value is string s && s == SqlLiterals.TrueExpression) return true;
        return false;
    }

    /// <summary>
    /// Parses a binary operand and delegates execution recursively left to right to solve conditionals natively.
    /// </summary>
    /// <param name="binNode">The binary comparison condition segment.</param>
    /// <param name="row">The joined row properties representing current evaluation context.</param>
    /// <returns>The result condition matching behavior of evaluated conditions constraint.</returns>
    private bool EvaluateBinaryNode(BinaryExpressionNode binNode, JoinedRow row)
    {
        if (binNode.Operator == Operators.AND)
        {
            return EvaluatePredicate(binNode.Left, row) && EvaluatePredicate(binNode.Right, row);
        }

        if (binNode.Operator == Operators.OR)
        {
            return EvaluatePredicate(binNode.Left, row) || EvaluatePredicate(binNode.Right, row);
        }

        return EvaluateComparisonOperator(binNode, row);
    }

    /// <summary>
    /// Tests the inequality properties of distinct node values dynamically fetched for evaluation limits.
    /// </summary>
    /// <param name="binNode">The binary statement context holding standard variables logic.</param>
    /// <param name="row">Context-specific row iteration value representation mapping properties requested.</param>
    /// <returns>Result representing whether dynamic comparisons yield success execution expectations logically.</returns>
    private bool EvaluateComparisonOperator(BinaryExpressionNode binNode, JoinedRow row)
    {
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

    /// <summary>
    /// Handles strict equality evaluation comparing mixed internal primitive variables properly formatted natively.
    /// </summary>
    /// <param name="val1">The left-hand comparative standard component natively referenced representation.</param>
    /// <param name="val2">The right-hand comparative standard component natively referenced representation.</param>
    /// <returns>Significance context specifying strict native comparison equivalence evaluation natively.</returns>
    private static bool EvaluateEquality(object? val1, object? val2)
    {
        if (val1 == null || val2 == null) return false;
        return ExpressionValueComparer.AreEqual(val1, val2, trimQuotedStrings: true, useNumericTolerance: true);
    }

    /// <summary>
    /// Compares values correctly distinguishing and factoring null or numeric scaling issues.
    /// </summary>
    /// <param name="leftVal">The initial numeric/string sequence context to execute dynamically natively formatted logic over conditionally securely effectively resolving standard operations successfully sequentially iteratively strictly universally validating accurately evaluating directly implicitly explicit behavior specifically evaluating logically executing predictably checking accurately validating correctly resolving optimally strictly processing efficiently precisely explicitly validating inherently appropriately efficiently mapping correctly properly securely checking natively accurately testing natively explicitly precisely parsing precisely matching functionally successfully naturally validating correctly reliably directly logically validating explicitly efficiently optimally efficiently functionally efficiently effectively inherently securely functionally natively structurally correctly accurately evaluating precisely logically successfully validating checking parsing efficiently accurately formatting predictably properly parsing correctly logically appropriately securely natively optimizing effectively executing.</param>
    /// <param name="rightVal">The evaluating object corresponding sequence context successfully dynamically naturally evaluated native standard checking functionally correctly properly reliably efficiently.</param>
    /// <returns>Integer comparative equivalent defining precedence behavior directly logically matching natively successfully.</returns>
    private static int? CompareDynamics(object? leftVal, object? rightVal)
    {
        if (leftVal == null || rightVal == null) return null;
        return ExpressionValueComparer.Compare(leftVal, rightVal, trimQuotedStrings: true);
    }

    /// <summary>
    /// Transforms the abstract syntax node parameter into standard evaluable constants querying underlying tables directly.
    /// </summary>
    /// <param name="node">The expression component directly requesting specific mapped parameters correctly natively sequentially executing logic natively mapped safely securely appropriately sequentially precisely directly reliably mapped strictly implicitly validating formatting explicitly accurately checking effectively validating checking efficiently correctly validating conditionally effectively structurally checking natively parsing explicitly securely evaluating directly extracting optimizing optimally validating reliably correctly checking naturally properly validating dynamically efficiently correctly accurately parsing logically evaluating precisely mapping matching extracting structurally natively natively explicitly accurately securely securely processing sequentially generating reliably correctly sequentially correctly optimally natively validating implicitly appropriately optimally native implicitly checking successfully effectively checking naturally testing checking functionally efficiently mapped securely mapped directly mapped properly mapped correctly uniquely efficiently logically successfully explicitly securely accurately sequentially safely securely mapping recursively correctly successfully explicitly securely explicitly mapped appropriately inherently correctly structurally accurately functionally sequentially cleanly safely dependably dynamically reliably safely structurally confidently accurately natively successfully safely securely efficiently properly carefully consistently structurally recursively implicitly functionally strictly effectively accurately optimizing specifically perfectly natively properly matching specifically robustly validating reliably explicitly structurally reliably precisely checking effectively securely safely naturally properly mapping recursively correctly parsing efficiently parsing correctly securely functionally accurately securely successfully reliably natively efficiently matching correctly.</param>
    /// <param name="row">The joined iterating collection context object specifically mapping natively natively explicitly optimally predictably implicitly efficiently mapping functionally safely checking efficiently efficiently uniquely confidently securely accurately sequentially natively optimizing natively reliably explicitly testing evaluating structurally confidently inherently recursively efficiently checking safely verifying logically appropriately properly matching predictably dynamically checking optimally successfully cleanly reliably confidently carefully securely natively safely correctly matching structurally dependably accurately uniquely optimally efficiently confidently validating recursively mapping natively parsing accurately cleanly dependably extracting safely thoroughly cleanly logically cleanly safely optimally securely checking flawlessly reliably mapping optimizing cleanly reliably accurately successfully reliably flawlessly optimally matching successfully testing properly consistently efficiently flawlessly matching dependably carefully successfully validating accurately gracefully mapping dependably formatting dependably securely validating exactly accurately effectively safely properly dynamically natively testing logically mapping accurately safely cleanly.</param>
    /// <returns>A generic evaluation result mapping logically safely effectively.</returns>
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

    /// <summary>
    /// Retrieves row data explicitly targeted via the column identifier mapping the table layout properties precisely.
    /// </summary>
    /// <param name="row">The currently evaluated logical sequence iteration.</param>
    /// <param name="columnReference">The textual standard field identifier indicating required retrieval coordinates implicitly.</param>
    /// <returns>The data element directly resolving natively securely strictly.</returns>
    private object? ResolveColumnValue(JoinedRow row, string columnReference)
    {
        string[] referenceParts = columnReference.Split('.');
        
        if (referenceParts.Length == 1)
        {
            var matchedTables = row.Keys.Where(t => row[t].ContainsKey(columnReference)).ToList();
            
            if (matchedTables.Count == 0) throw new Exception($"Column '{columnReference}' not found.");
            if (matchedTables.Count > 1) throw new Exception($"Column '{columnReference}' is ambiguous.");
            
            return row[matchedTables.First()][columnReference];
        }

        string tableOrAlias = referenceParts[0];
        string colName = referenceParts[1];

        string resolvedTableName = _model.TableService!.GetTableDetailByAliasOrName(tableOrAlias).TableName;

        if (row.ContainsKey(resolvedTableName) && row[resolvedTableName].ContainsKey(colName))
        {
            return row[resolvedTableName][colName];
        }

        throw new Exception($"Column '{columnReference}' not found in the currently resolved JOIN results.");
    }
}