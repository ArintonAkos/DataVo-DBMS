using DataVo.Core.Logging;
using DataVo.Core.Models.DQL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Parser.Statements.Mechanism;
using DataVo.Core.Parser.Types;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Enums;
using DataVo.Core.Constants;
using DataVo.Core.Utils;
using DataVo.Core.Transactions;
using DataVo.Core.Models.Statement.Utils;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// Executes a SQL <c>SELECT</c> statement against the currently active database.
/// <para>
/// Orchestrates the full query pipeline: table resolution, WHERE filtering,
/// JOIN evaluation, GROUP BY grouping, aggregate computation, HAVING filtering,
/// ORDER BY sorting, and DISTINCT de-duplication.
/// </para>
/// <para>
/// On successful execution, the <see cref="BaseDbAction.Fields"/> and
/// <see cref="BaseDbAction.Data"/> properties are populated with the query result.
/// </para>
/// </summary>
/// <param name="ast">The parsed <see cref="SelectStatement"/> AST node representing the SELECT query.</param>
internal class Select(SelectStatement ast) : BaseDbAction
{
    /// <summary>
    /// The parsed model representation of the SELECT statement.
    /// </summary>
    private readonly SelectModel _model = SelectModel.FromAst(ast);
    private readonly Dictionary<JoinedRow, Dictionary<string, object?>> _windowValues = [];

    /// <summary>
    /// Executes the SELECT query end-to-end.
    /// <para>
    /// Pipeline: validate database → evaluate WHERE / JOIN → GROUP BY → aggregate → HAVING → ORDER BY → project columns → DISTINCT.
    /// </para>
    /// </summary>
    /// <param name="session">The session identifier used to resolve the active database from the cache.</param>
    /// <exception cref="Exception">
    /// Caught internally. Error details are appended to <see cref="BaseDbAction.Messages"/> and logged via <see cref="Logger"/>.
    /// </exception>
    public override void PerformAction(Guid session)
    {
        try
        {
            if (ast.Ctes.Count > 0)
            {
                _model.SetCteTables(MaterializeCtes(ast.Ctes, session));
            }

            string database = ValidateDatabase(session);

            var lockedTables = AcquireReadLocks(database);

            try
            {
                ListedTable result = EvaluateStatements();

                GroupedTable groupedTable = GroupResults(result);

                result = AggregateGroupedTable(groupedTable);
                result = ApplyHaving(result);
                result = ApplyOrderBy(result);
                ComputeWindowFunctionValues(result);

                Fields = CreateFieldsFromColumns(result);
                Data = CreateDataFromResult(result, Fields);

                if (_model.IsDistinct)
                {
                    Data = ApplyDistinct(Data);
                }

                if (_model.LimitSkip.HasValue && _model.LimitSkip.Value > 0)
                {
                    Data = Data.Skip(_model.LimitSkip.Value).ToList();
                }

                if (_model.LimitTake.HasValue)
                {
                    Data = Data.Take(_model.LimitTake.Value).ToList();
                }

                Logger.Info($"Rows selected: {Data.Count}");
                Messages.Add($"Rows selected: {Data.Count}");
            }
            finally
            {
                ReleaseReadLocks(database, lockedTables);
            }
        }
        catch (Exception ex)
        {
            Messages.Add(ex.ToString());
            Logger.Error(ex.ToString());
        }
    }

    private Dictionary<string, TableDetail> MaterializeCtes(List<CteDefinitionNode> ctes, Guid session)
    {
        Dictionary<string, TableDetail> materialized = new(StringComparer.OrdinalIgnoreCase);

        foreach (var cte in ctes)
        {
            Dictionary<string, TableDetail> inherited = new(StringComparer.OrdinalIgnoreCase);

            foreach (var table in _model.CteTables)
            {
                inherited[table.Key] = table.Value;
            }

            foreach (var table in materialized)
            {
                inherited[table.Key] = table.Value;
            }

            var cteSelect = new Select(cte.Select);
            cteSelect.UseEngine(Engine);
            cteSelect._model.SetCteTables(inherited);

            var cteResult = cteSelect.Perform(session);
            if (cteResult.IsError)
            {
                throw new Exception(cteResult.Messages.FirstOrDefault() ?? $"Failed to materialize CTE '{cte.Name.Name}'.");
            }

            List<Record> rows = [];
            long rowId = 1;
            foreach (var row in cteResult.Data)
            {
                var values = row.ToDictionary(k => k.Key, v => (dynamic)v.Value!);
                rows.Add(new Record(rowId++, values));
            }

            materialized[cte.Name.Name] = new TableDetail(cte.Name.Name, null, [.. cteResult.Fields], rows);
        }

        return materialized;
    }

    private List<string> AcquireReadLocks(string databaseName)
    {
        var tableNames = GetReferencedTableNames();

        foreach (string tableName in tableNames)
        {
            Locks.AcquireReadLock(databaseName, tableName);
        }

        return tableNames;
    }

    private void ReleaseReadLocks(string databaseName, List<string> tableNames)
    {
        for (int i = tableNames.Count - 1; i >= 0; i--)
        {
            Locks.ReleaseReadLock(databaseName, tableNames[i]);
        }
    }

    private List<string> GetReferencedTableNames()
    {
        if (_model.TableService?.TableDetails?.Count > 0)
        {
            return [.. _model.TableService.TableDetails.Values
                .Select(table => table.TableName)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(table => table, StringComparer.OrdinalIgnoreCase)];
        }

        return [.. new[] { _model.FromTable.TableName }
            .Where(table => !string.IsNullOrWhiteSpace(table))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(table => table, StringComparer.OrdinalIgnoreCase)];
    }

    /// <summary>
    /// Removes duplicate rows from the result set by comparing all column values.
    /// Uses <see cref="DictionaryComparer"/> for structural equality across row dictionaries.
    /// </summary>
    /// <param name="data">The unfiltered result rows that may contain duplicates.</param>
    /// <returns>A new list containing only distinct rows.</returns>
    private static List<Dictionary<string, dynamic>> ApplyDistinct(List<Dictionary<string, dynamic>> data)
    {
        return [.. data.Select(d => d.ToDictionary(k => k.Key, v => (object?)v.Value))
                   .Distinct(new DictionaryComparer())
                   .Select(d => d.ToDictionary(k => k.Key, v => (dynamic)v.Value!))];
    }

    /// <summary>
    /// Delegates to the model's <c>GroupByStatement</c> to partition the result rows into groups.
    /// If no GROUP BY clause is present, the entire result set is treated as a single group.
    /// </summary>
    /// <param name="tableData">The flat result rows prior to grouping.</param>
    /// <returns>A <see cref="GroupedTable"/> containing the partitioned row groups.</returns>
    private GroupedTable GroupResults(ListedTable tableData)
    {
        return _model.GroupByStatement.Evaluate(tableData);
    }

    /// <summary>
    /// Applies aggregate functions (e.g., <c>COUNT</c>, <c>SUM</c>, <c>AVG</c>) to the grouped table data
    /// and flattens the result back into a <see cref="ListedTable"/>.
    /// </summary>
    /// <param name="groupedTable">The grouped result table produced by <see cref="GroupResults"/>.</param>
    /// <returns>A <see cref="ListedTable"/> with aggregated values appended to each group's representative row.</returns>
    private ListedTable AggregateGroupedTable(GroupedTable groupedTable)
    {
        return _model.AggregateStatement.Perform(groupedTable);
    }

    /// <summary>
    /// Filters the result set using the HAVING clause expression, if one was specified.
    /// Each row is tested against the HAVING predicate; rows that do not satisfy the condition are removed.
    /// </summary>
    /// <param name="tableData">The aggregated result set to filter.</param>
    /// <returns>The filtered <see cref="ListedTable"/>, or the original data if no HAVING clause exists.</returns>
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
    /// Sorts the result set according to the ORDER BY clause, if one was specified.
    /// Multiple sort columns are applied in order, with each subsequent column acting as a tiebreaker.
    /// </summary>
    /// <param name="tableData">The result set to sort.</param>
    /// <returns>The sorted <see cref="ListedTable"/>, or the original data if no ORDER BY clause exists.</returns>
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

        return ordered == null ? tableData : [.. ordered.ToList()];
    }

    /// <summary>
    /// Applies a single ORDER BY column directive to the result set.
    /// If <paramref name="ordered"/> is <c>null</c>, establishes the primary sort;
    /// otherwise, appends a secondary (tiebreaker) sort via <c>ThenBy</c> / <c>ThenByDescending</c>.
    /// </summary>
    /// <param name="tableData">The initial (unsorted) table data — used only for the first sort column.</param>
    /// <param name="ordered">The existing ordered enumeration from previous sort columns, or <c>null</c> if this is the first.</param>
    /// <param name="orderCol">The ORDER BY directive specifying the column name and sort direction (<c>ASC</c> / <c>DESC</c>).</param>
    /// <returns>An <see cref="IOrderedEnumerable{T}"/> incorporating the new sort criterion.</returns>
    private IOrderedEnumerable<JoinedRow> ApplyOrderToColumn(ListedTable tableData, IOrderedEnumerable<JoinedRow>? ordered, OrderByColumnNode orderCol)
    {
        Func<JoinedRow, object?> keySelector = row => ResolveOrderByValue(row, orderCol.Column.Name);

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

    private object? ResolveOrderByValue(JoinedRow row, string orderByToken)
    {
        var aliasColumn = _model.GetSelectColumnByAlias(orderByToken);
        if (aliasColumn?.Expression != null)
        {
            return ResolveNodeValue(aliasColumn.Expression, row);
        }

        if (row.ContainsKey(GroupBy.HASH_VALUE) && row[GroupBy.HASH_VALUE].ContainsKey(orderByToken))
        {
            return row[GroupBy.HASH_VALUE][orderByToken];
        }

        return ResolveColumnValue(row, orderByToken);
    }

    /// <summary>
    /// Validates that a database is currently selected for the session and that
    /// all referenced columns exist in the catalog schema.
    /// </summary>
    /// <param name="session">The session identifier used to look up the active database.</param>
    /// <returns>The name of the active database.</returns>
    /// <exception cref="Exception">
    /// Thrown when no database is in use or when invalid columns are referenced
    /// outside of a JOIN context.
    /// </exception>
    private string ValidateDatabase(Guid session)
    {
        string databaseName = GetDatabaseName(session);

        bool hasMissingColumns = _model.Validate(databaseName);

        if (!_model.JoinStatement.ContainsJoin() && hasMissingColumns)
        {
            throw new Exception("Invalid columns specified'");
        }

        return databaseName;
    }

    /// <summary>
    /// Determines the initial row source for the query based on the clauses present:
    /// <list type="bullet">
    ///   <item><description>If a WHERE clause exists, evaluates it (with JOIN support).</description></item>
    ///   <item><description>If only a JOIN is present (no WHERE), evaluates the JOIN directly.</description></item>
    ///   <item><description>Otherwise, performs a full table scan on the FROM table.</description></item>
    /// </list>
    /// </summary>
    /// <returns>A <see cref="ListedTable"/> containing the initial matched rows.</returns>
    private ListedTable EvaluateStatements()
    {
        ListedTable result;

        if (_model.WhereStatement.IsEvaluatable())
        {
            var whereExpression = _model.WhereStatement.GetExpression();
            if (whereExpression != null && ContainsArithmeticExpression(whereExpression))
            {
                result = EvaluateWhereWithExpression(whereExpression);
            }
            else
            {
                result = _model.WhereStatement.EvaluateWithJoin(_model.TableService!, _model.JoinStatement);
            }
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

    private ListedTable EvaluateWhereWithExpression(ExpressionNode whereExpression)
    {
        ListedTable source = _model.JoinStatement.ContainsJoin()
            ? EvaluateJoin()
            : new ListedTable(_model.FromTable!.TableContentValues!
                .Select(row => new JoinedRow(_model.FromTable.TableName, row.ToRow()))
                .ToList());

        var filtered = source
            .Where(row => EvaluatePredicate(whereExpression, row))
            .ToList();

        return new ListedTable(filtered);
    }

    private static bool ContainsArithmeticExpression(ExpressionNode node)
    {
        if (node is BinaryExpressionNode binary)
        {
            if (binary.Operator is "+" or "-" or "*" or "/" or "ADD" or "SUB" or "MUL" or "DIV")
            {
                return true;
            }

            return ContainsArithmeticExpression(binary.Left) || ContainsArithmeticExpression(binary.Right);
        }

        if (node is AggregateExpressionNode aggregate && aggregate.Argument != null)
        {
            return ContainsArithmeticExpression(aggregate.Argument);
        }

        return false;
    }

    /// <summary>
    /// Converts the FROM table's content into a <see cref="HashedTable"/> and passes it through
    /// the configured JOIN strategy to produce the joined result set.
    /// Called when the query contains a JOIN clause but no WHERE clause.
    /// </summary>
    /// <returns>A <see cref="ListedTable"/> containing the joined rows.</returns>
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
    /// Constructs the output field list based on the columns specified in the SELECT clause.
    /// In a JOIN context, field names are prefixed with the table name or alias (e.g., <c>Users.Name</c>).
    /// If aggregation results are present (identified by <see cref="GroupBy.HASH_VALUE"/>),
    /// the aggregated column names are appended to the field list.
    /// </summary>
    /// <param name="filteredTable">The fully evaluated result set, used to inspect aggregation metadata.</param>
    /// <returns>A list of qualified field names representing the output schema.</returns>
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
        if (firstRow != null)
        {
            foreach (var expressionColumn in _model.GetComputedExpressionColumns())
            {
                fields.Add(expressionColumn.Alias ?? expressionColumn.RawExpression);
            }

            foreach (var windowColumn in _model.GetWindowFunctionColumns())
            {
                fields.Add(windowColumn.Alias ?? windowColumn.RawExpression);
            }
        }

        if (firstRow != null && firstRow.ContainsKey(GroupBy.HASH_VALUE))
        {
            foreach (var aggregateColumn in _model.GetAggregateColumns())
            {
                if (aggregateColumn.Alias != null)
                {
                    fields.Add(aggregateColumn.Alias);
                    continue;
                }

                if (aggregateColumn.Expression is AggregateExpressionNode aggregateExpression)
                {
                    string canonicalKey = AggregateExpressionFormatter.BuildHeader(aggregateExpression);
                    string outputName = firstRow[GroupBy.HASH_VALUE].ContainsKey(canonicalKey)
                        ? canonicalKey
                        : ResolveAggregateKey(aggregateExpression, firstRow);

                    fields.Add(outputName);
                }
            }
        }

        return fields;
    }

    /// <summary>
    /// Projects each result row into a dictionary keyed by field name, matching the output schema.
    /// </summary>
    /// <param name="filteredTable">The fully evaluated and filtered result set.</param>
    /// <param name="fieldsList">The ordered list of output field names.</param>
    /// <returns>A list of dictionaries, each representing one output row mapped by field name to its value.</returns>
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
    /// Extracts column values from a single <see cref="JoinedRow"/> according to the output field list.
    /// Handles column aliases (split on <c>" AS "</c>) and includes aggregation results when present.
    /// </summary>
    /// <param name="row">The joined row containing per-table column dictionaries.</param>
    /// <param name="fieldsList">The ordered list of output field names.</param>
    /// <returns>A dictionary mapping each field name to its value for this row.</returns>
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
            foreach (var expressionColumn in _model.GetComputedExpressionColumns())
            {
                string currentFieldName = fieldsList[fieldIndex++];
                if (expressionColumn.Expression != null)
                {
                    data[currentFieldName] = ResolveNodeValue(expressionColumn.Expression, row);
                }
            }

            foreach (var aggregateColumn in _model.GetAggregateColumns())
            {
                string currentFieldName = fieldsList[fieldIndex++];

                if (aggregateColumn.Expression is AggregateExpressionNode aggregateExpression)
                {
                    data[currentFieldName] = ResolveNodeValue(aggregateExpression, row);
                }
            }

            foreach (var windowColumn in _model.GetWindowFunctionColumns())
            {
                string currentFieldName = fieldsList[fieldIndex++];
                data[currentFieldName] = ResolveWindowValue(row, currentFieldName);
            }
        }
        else
        {
            foreach (var expressionColumn in _model.GetComputedExpressionColumns())
            {
                string currentFieldName = fieldsList[fieldIndex++];
                if (expressionColumn.Expression != null)
                {
                    data[currentFieldName] = ResolveNodeValue(expressionColumn.Expression, row);
                }
            }

            foreach (var windowColumn in _model.GetWindowFunctionColumns())
            {
                string currentFieldName = fieldsList[fieldIndex++];
                data[currentFieldName] = ResolveWindowValue(row, currentFieldName);
            }
        }

        return data;
    }

    private void ComputeWindowFunctionValues(ListedTable rows)
    {
        _windowValues.Clear();
        List<SelectColumnNode> windowColumns = _model.GetWindowFunctionColumns();
        if (windowColumns.Count == 0 || rows.Count == 0)
        {
            return;
        }

        foreach (var col in windowColumns)
        {
            if (col.Expression is not WindowFunctionExpressionNode windowExpr)
            {
                continue;
            }

            if (!windowExpr.FunctionName.Equals("RANK", StringComparison.OrdinalIgnoreCase))
            {
                throw new Exception($"Unsupported window function: {windowExpr.FunctionName}");
            }

            string outputName = col.Alias ?? col.RawExpression;

            var partitions = rows
                .GroupBy(row => BuildPartitionSignature(row, windowExpr.PartitionByColumns))
                .ToList();

            foreach (var partition in partitions)
            {
                List<JoinedRow> ordered = windowExpr.IsOrderAscending
                    ? [.. partition.OrderBy(r => ResolveWindowOrderValue(r, windowExpr.OrderByColumn), DynamicObjectComparer.Instance)]
                    : [.. partition.OrderByDescending(r => ResolveWindowOrderValue(r, windowExpr.OrderByColumn), DynamicObjectComparer.Instance)];

                object? previousOrderValue = null;
                long currentRank = 1;

                for (int i = 0; i < ordered.Count; i++)
                {
                    var row = ordered[i];
                    object? currentOrderValue = ResolveWindowOrderValue(row, windowExpr.OrderByColumn);

                    if (i == 0)
                    {
                        currentRank = 1;
                    }
                    else if (DynamicObjectComparer.Instance.Compare(previousOrderValue, currentOrderValue) != 0)
                    {
                        currentRank = i + 1;
                    }

                    if (!_windowValues.TryGetValue(row, out var rowValues))
                    {
                        rowValues = [];
                        _windowValues[row] = rowValues;
                    }

                    rowValues[outputName] = currentRank;
                    previousOrderValue = currentOrderValue;
                }
            }
        }
    }

    private string BuildPartitionSignature(JoinedRow row, List<ColumnRefNode> partitionColumns)
    {
        if (partitionColumns.Count == 0)
        {
            return "__ALL__";
        }

        var parts = partitionColumns
            .Select(col => ResolveWindowOrderValue(row, col))
            .Select(BuildWindowValueSignature);

        return string.Join("|", parts);
    }

    private static string BuildWindowValueSignature(object? value)
    {
        if (value == null)
        {
            return "NULL";
        }

        return value switch
        {
            string s => $"System.String:{s}",
            char c => $"System.Char:{c}",
            bool b => $"System.Boolean:{b}",
            DateOnly d => $"System.DateOnly:{d:O}",
            DateTime dt => $"System.DateTime:{dt:O}",
            IFormattable formattable => $"{value.GetType().FullName}:{formattable.ToString(null, System.Globalization.CultureInfo.InvariantCulture)}",
            _ => $"{value.GetType().FullName}:{value}"
        };
    }

    private object? ResolveWindowOrderValue(JoinedRow row, ColumnRefNode column)
    {
        string reference = string.IsNullOrWhiteSpace(column.TableOrAlias)
            ? column.Column
            : $"{column.TableOrAlias}.{column.Column}";

        return ResolveColumnValue(row, reference);
    }

    private object? ResolveWindowValue(JoinedRow row, string outputField)
    {
        if (_windowValues.TryGetValue(row, out var values) && values.TryGetValue(outputField, out var value))
        {
            return value;
        }

        return null;
    }

    /// <summary>
    /// Recursively evaluates a HAVING clause predicate against a specific row.
    /// Delegates to <see cref="EvaluateLiteralNode"/> for simple literals and
    /// <see cref="EvaluateBinaryNode"/> for binary expressions.
    /// </summary>
    /// <param name="node">The root expression node of the HAVING predicate (or a sub-node during recursion).</param>
    /// <param name="row">The row to test against the predicate.</param>
    /// <returns><c>true</c> if the row satisfies the condition; otherwise, <c>false</c>.</returns>
    /// <exception cref="Exception">Thrown when the node type is not a <see cref="LiteralNode"/> or <see cref="BinaryExpressionNode"/>.</exception>
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
    /// Evaluates a standalone literal node as a boolean.
    /// Returns <c>true</c> for boolean <c>true</c> or the SQL literal <c>TRUE</c> string; <c>false</c> otherwise.
    /// </summary>
    /// <param name="literalNode">The literal node to evaluate.</param>
    /// <returns>The boolean interpretation of the literal value.</returns>
    private bool EvaluateLiteralNode(LiteralNode literalNode)
    {
        if (literalNode.Value is bool b) return b;
        if (literalNode.Value is string s && s == SqlLiterals.TrueExpression) return true;
        return false;
    }

    /// <summary>
    /// Evaluates a binary expression node within a HAVING predicate.
    /// For logical operators (<c>AND</c>, <c>OR</c>), recursively evaluates the left and right sub-trees.
    /// For comparison operators, delegates to <see cref="EvaluateComparisonOperator"/>.
    /// </summary>
    /// <param name="binNode">The binary expression node containing the operator and operands.</param>
    /// <param name="row">The row to test against the condition.</param>
    /// <returns><c>true</c> if the row satisfies the binary condition; otherwise, <c>false</c>.</returns>
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
    /// Evaluates a comparison operator (<c>=</c>, <c>!=</c>, <c>&lt;</c>, <c>&gt;</c>, <c>&lt;=</c>, <c>&gt;=</c>)
    /// by resolving both operand values from the row and applying the operator.
    /// </summary>
    /// <param name="binNode">The binary expression containing the comparison operator and operands.</param>
    /// <param name="row">The row from which operand values are resolved.</param>
    /// <returns><c>true</c> if the comparison holds; otherwise, <c>false</c>.</returns>
    /// <exception cref="Exception">Thrown when the operator is not supported in a HAVING context.</exception>
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
    /// Compares two values for equality. Quoted strings are trimmed before comparison.
    /// Applies numeric tolerance for floating-point values.
    /// Returns <c>false</c> if either value is <c>null</c>.
    /// </summary>
    /// <param name="val1">The left-hand value.</param>
    /// <param name="val2">The right-hand value.</param>
    /// <returns><c>true</c> if the values are considered equal; otherwise, <c>false</c>.</returns>
    private static bool EvaluateEquality(object? val1, object? val2)
    {
        if (val1 == null || val2 == null) return false;
        return ExpressionValueComparer.AreEqual(val1, val2, trimQuotedStrings: true, useNumericTolerance: true);
    }

    /// <summary>
    /// Performs an ordered comparison between two values.
    /// Quoted strings are trimmed before comparison. Returns <c>null</c> if either value is <c>null</c>.
    /// </summary>
    /// <param name="leftVal">The left-hand value.</param>
    /// <param name="rightVal">The right-hand value.</param>
    /// <returns>
    /// A negative integer if <paramref name="leftVal"/> is less than <paramref name="rightVal"/>,
    /// zero if equal, a positive integer if greater, or <c>null</c> if either operand is <c>null</c>.
    /// </returns>
    private static int? CompareDynamics(object? leftVal, object? rightVal)
    {
        if (leftVal == null || rightVal == null) return null;
        return ExpressionValueComparer.Compare(leftVal, rightVal, trimQuotedStrings: true);
    }

    /// <summary>
    /// Resolves an expression node to its runtime value. Handles <see cref="LiteralNode"/>,
    /// <see cref="ResolvedColumnRefNode"/>, and <see cref="ColumnRefNode"/>.
    /// </summary>
    /// <param name="node">The expression node to resolve.</param>
    /// <param name="row">The current row from which column values are extracted.</param>
    /// <returns>The resolved value, or the literal value directly.</returns>
    /// <exception cref="Exception">Thrown when the node type is not supported in a HAVING context.</exception>
    private object? ResolveNodeValue(ExpressionNode node, JoinedRow row)
    {
        return ExpressionEvaluator.Evaluate(
            node,
            row,
            (colRef, r) =>
            {
                string reference = string.IsNullOrEmpty(colRef.TableOrAlias) ? colRef.Column : $"{colRef.TableOrAlias}.{colRef.Column}";
                return ResolveColumnValue(r, reference);
            },
            (aggNode, r) =>
            {
                // Aggregates are materialized into the grouped/aggregated row under the HASH_VALUE map.
                if (!r.ContainsKey(GroupBy.HASH_VALUE)) throw new Exception("Aggregate expression used outside grouped/aggregated context.");

                var aggMap = r[GroupBy.HASH_VALUE];

                string canonicalKey = AggregateExpressionFormatter.BuildHeader(aggNode);
                if (aggMap.ContainsKey(canonicalKey))
                {
                    return aggMap[canonicalKey];
                }

                string resolvedKey = ResolveAggregateKey(aggNode, r);
                return aggMap[resolvedKey];
            }
        );
    }

    private string ResolveAggregateKey(AggregateExpressionNode aggNode, JoinedRow row)
    {
        var aggMap = row[GroupBy.HASH_VALUE];

        // Try to match by function name and argument if available
        string funcName = aggNode.FunctionName.ToUpperInvariant();

        // If COUNT(*) style
        if (aggNode.IsStar)
        {
            // Find a key that starts with FUNCNAME(
            var key = aggMap.Keys.FirstOrDefault(k => k.StartsWith(funcName, StringComparison.OrdinalIgnoreCase));
            if (key != null) return key;
            throw new Exception($"Aggregate result '{funcName}(*)' not found in grouped row.");
        }

        // If argument is a column reference, try to build the header name
        if (aggNode.Argument is ColumnRefNode argCol)
        {
            string colRefStr = string.IsNullOrEmpty(argCol.TableOrAlias) ? argCol.Column : $"{argCol.TableOrAlias}.{argCol.Column}";
            // Try keys that contain the column reference
            var key = aggMap.Keys.FirstOrDefault(k => k.StartsWith(funcName, StringComparison.OrdinalIgnoreCase) && k.Contains(argCol.Column, StringComparison.OrdinalIgnoreCase));
            if (key != null) return key;
        }

        // Fallback: return first matching function
        var anyKey = aggMap.Keys.FirstOrDefault(k => k.StartsWith(funcName, StringComparison.OrdinalIgnoreCase));
        if (anyKey != null) return anyKey;

        throw new Exception($"Aggregate result for {funcName} not found in grouped row.");
    }

    /// <summary>
    /// Retrieves the value of a column from a <see cref="JoinedRow"/> by its reference string.
    /// Supports both unqualified column names (e.g., <c>"Name"</c>) and qualified references
    /// (e.g., <c>"Users.Name"</c>). For unqualified names, the column must exist in exactly one
    /// table to avoid ambiguity.
    /// </summary>
    /// <param name="row">The joined row containing per-table column dictionaries.</param>
    /// <param name="columnReference">The column reference, optionally prefixed with a table name or alias separated by <c>'.'</c>.</param>
    /// <returns>The column value from the matched table, or <c>null</c> if the value is null.</returns>
    /// <exception cref="Exception">Thrown when the column is not found or is ambiguous across multiple tables.</exception>
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