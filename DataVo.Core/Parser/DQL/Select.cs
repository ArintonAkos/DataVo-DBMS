using DataVo.Core.Logging;
using DataVo.Core.Models.DQL;
using DataVo.Core.Exceptions;
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
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.StorageEngine;
using DataVo.Core.Execution.Volcano;
using DataVo.Core.MVCC;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;

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
internal partial class Select(SelectStatement ast) : BaseDbAction
{
    private string? _activeDatabaseName;
    private TransactionSnapshot? _activeSnapshot;

    private readonly record struct LockedReadResources(
        Dictionary<string, List<long>> RowReadLocksByTable);

    private enum JoinPhysicalAlgorithm
    {
        Hash,
        NestedLoop
    }

    private enum JoinPlanSide
    {
        Left,
        Right
    }

    private sealed class JoinEdgePhysicalPlan
    {
        public JoinEdgePhysicalPlan(
            JoinPhysicalAlgorithm algorithm,
            JoinPlanSide buildSide,
            JoinPlanSide probeSide,
            int estimatedBuildRows,
            int estimatedProbeRows,
            int estimatedOutputRows,
            int estimatedCost,
            string reason)
        {
            Algorithm = algorithm;
            BuildSide = buildSide;
            ProbeSide = probeSide;
            EstimatedBuildRows = estimatedBuildRows;
            EstimatedProbeRows = estimatedProbeRows;
            EstimatedOutputRows = estimatedOutputRows;
            EstimatedCost = estimatedCost;
            Reason = reason;
        }

        public JoinPhysicalAlgorithm Algorithm { get; }
        public JoinPlanSide BuildSide { get; }
        public JoinPlanSide ProbeSide { get; }
        public int EstimatedBuildRows { get; }
        public int EstimatedProbeRows { get; }
        public int EstimatedOutputRows { get; }
        public int EstimatedCost { get; }
        public string Reason { get; }
    }

    /// <summary>
    /// The parsed model representation of the SELECT statement.
    /// </summary>
    private readonly SelectModel _model = SelectModel.FromAst(ast);
    private const double JoinCardinalityFeedbackAlpha = 0.35d;
    private static readonly object _joinCardinalityFeedbackSync = new();
    private static readonly ConcurrentDictionary<string, double> _joinCardinalityFeedback = new(StringComparer.OrdinalIgnoreCase);
    private static string? _joinCardinalityFeedbackLoadedPath;
    private static bool _joinCardinalityFeedbackLoaded;
    private readonly Dictionary<JoinedRow, Dictionary<string, object?>> _windowValues = [];
    private bool _volcanoLimitPushedDown;
    private bool _volcanoOffsetPushedDown;
    private bool _volcanoOrderPushedDown;
    private bool _volcanoDistinctPushedDown;
    private bool _volcanoGroupByPushedDown;
    private bool _volcanoAggregatePushedDown;
    private readonly Dictionary<string, int> _queryHybridRoutingCounters = new(StringComparer.OrdinalIgnoreCase);
    private int _queryVectorExpansionPasses;
    private HashSet<string> _volcanoAggregateGroupKeyColumns = [];
    private HashSet<string> _volcanoAggregateOutputColumns = [];

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
        long queryStartTick = Environment.TickCount64;
        ResetQueryHybridTelemetry();

        try
        {
            if (ast.Ctes.Count > 0)
            {
                _model.SetCteTables(MaterializeCtes(ast.Ctes, session));
            }

            string database = ValidateDatabase(session);
            _activeDatabaseName = database;

            TransactionContext? txContext = Transactions.GetContext(session);
            _activeSnapshot = txContext?.Snapshot;

            using IDisposable mvccScope = MvccExecutionScope.PushSnapshot(_activeSnapshot);

            LockedReadResources lockedResources = AcquireReadLocks(database);

            try
            {
                ListedTable result = EvaluateStatements();

                if (_volcanoAggregatePushedDown)
                {
                    result = ApplyHaving(result);
                    result = _volcanoOrderPushedDown ? result : ApplyOrderBy(result);
                }
                else
                {
                    GroupedTable groupedTable = GroupResults(result);

                    result = AggregateGroupedTable(groupedTable);
                    result = ApplyHaving(result);
                    result = _volcanoOrderPushedDown ? result : ApplyOrderBy(result);
                }
                ComputeWindowFunctionValues(result);

                Fields = CreateFieldsFromColumns(result);
                Data = CreateDataFromResult(result, Fields);

                if (_model.IsDistinct && !_volcanoDistinctPushedDown)
                {
                    Data = ApplyDistinct(Data);
                }

                if (!_volcanoOffsetPushedDown && _model.LimitSkip.HasValue && _model.LimitSkip.Value > 0)
                {
                    Data = Data.Skip(_model.LimitSkip.Value).ToList();
                }

                if (!_volcanoLimitPushedDown && _model.LimitTake.HasValue)
                {
                    Data = Data.Take(_model.LimitTake.Value).ToList();
                }

                Logger.Info($"Rows selected: {Data.Count}");
                Messages.Add($"Rows selected: {Data.Count}");

                long queryElapsedMs = Math.Max(0, Environment.TickCount64 - queryStartTick);
                ReportHybridRoutingTelemetry(queryElapsedMs, Data.Count);
            }
            finally
            {
                ReleaseReadLocks(database, lockedResources);
            }
        }
        catch (Exception ex)
        {
            AddError(ex);
            Logger.Error(ex.ToString());
        }
    }

    // CTE materialization extracted to Select.Cte.cs partial.

    private LockedReadResources AcquireReadLocks(string databaseName)
    {
        if (_activeSnapshot != null)
        {
            // MVCC snapshot reads are non-blocking; visibility is enforced by snapshot filtering.
            return new LockedReadResources(new Dictionary<string, List<long>>(StringComparer.OrdinalIgnoreCase));
        }

        var tableNames = GetReferencedTableNames();
        var rowReadLocksByTable = new Dictionary<string, List<long>>(StringComparer.OrdinalIgnoreCase);

        foreach (string tableName in tableNames)
        {
            IEnumerable<long> rowIds = GetKnownRowIdsForTable(tableName);
            List<long> rowLocks = Locks.AcquireRowReadLocks(databaseName, tableName, rowIds);
            rowReadLocksByTable[tableName] = rowLocks;
        }

        return new LockedReadResources(rowReadLocksByTable);
    }

    private void ReleaseReadLocks(string databaseName, LockedReadResources lockedResources)
    {
        List<string> tableNames = [.. lockedResources.RowReadLocksByTable.Keys
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)];

        for (int i = tableNames.Count - 1; i >= 0; i--)
        {
            string tableName = tableNames[i];

            if (lockedResources.RowReadLocksByTable.TryGetValue(tableName, out List<long>? rowLocks)
                && rowLocks.Count > 0)
            {
                Locks.ReleaseRowReadLocks(databaseName, tableName, rowLocks);
            }
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

    private IEnumerable<long> GetKnownRowIdsForTable(string tableName)
    {
        if (_model.TableService?.TableDetails == null)
        {
            return Enumerable.Empty<long>();
        }

        if (!_model.TableService.TableDetails.TryGetValue(tableName, out TableDetail? tableDetail)
            || tableDetail.TableContentValues == null)
        {
            tableDetail = _model.TableService.TableDetails.Values
                .FirstOrDefault(detail => string.Equals(detail.TableName, tableName, StringComparison.OrdinalIgnoreCase));
        }

        if (tableDetail?.TableContentValues == null)
        {
            return Enumerable.Empty<long>();
        }

        return tableDetail.TableContentValues.Select(record => record.RowId);
    }

    /// <summary>
    /// Removes duplicate rows from the result set by comparing all column values.
    /// Uses <see cref="DictionaryComparer"/> for structural equality across row dictionaries.
    /// </summary>
    /// <param name="data">The unfiltered result rows that may contain duplicates.</param>
    /// <returns>A new list containing only distinct rows.</returns>
    private static List<Dictionary<string, object?>> ApplyDistinct(List<Dictionary<string, object?>> data)
    {
        return [.. data.Select(d => d.ToDictionary(k => k.Key, v => (object?)v.Value))
                   .Distinct(new DictionaryComparer())
                   .Select(d => d.ToDictionary(k => k.Key, v => (object?)v.Value))];
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
            throw new BindingException("Invalid columns specified'");
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
        ListedTable? vectorPredicateFastPath = TryEvaluateVectorPredicateUsingVectorIndex();
        if (vectorPredicateFastPath != null)
        {
            return vectorPredicateFastPath;
        }

        ListedTable? nearestNeighborFastPath = TryEvaluateNearestNeighborUsingVectorIndex();
        if (nearestNeighborFastPath != null)
        {
            return nearestNeighborFastPath;
        }

        ExpressionNode? whereExpression = _model.WhereStatement.IsEvaluatable()
            ? _model.WhereStatement.GetExpression()
            : null;

        PhysicalPlanDecision plan = BuildPhysicalPlan(whereExpression);
        Logger.Info($"Planner: logical={plan.LogicalPlan}, physical={(plan.UseVolcano ? "Volcano" : "Legacy")}, cost={plan.EstimatedCost}, reason={plan.Reason}");

        return plan.LogicalPlan switch
        {
            LogicalPlanKind.VolcanoInnerJoin => EvaluateInnerJoinWithVolcano(whereExpression),
            LogicalPlanKind.VolcanoNoJoin => EvaluateNoJoinWithVolcano(whereExpression),
            LogicalPlanKind.LegacyWhereExpression when whereExpression != null => EvaluateWhereWithExpression(whereExpression),
            LogicalPlanKind.LegacyWhereJoin => _model.WhereStatement.EvaluateWithJoin(_model.TableService!, _model.JoinStatement),
            LogicalPlanKind.LegacyJoinOnly => EvaluateJoin(),
            LogicalPlanKind.LegacyNoJoinScan => BuildLegacyNoJoinScan(),
            _ => BuildLegacyNoJoinScan()
        };
    }

    // Planner and fast-path decision logic extracted to Select.Planner.cs and Select.FastPathDecisions.cs partials.

    private ListedTable BuildLegacyNoJoinScan()
    {
        var sourceRows = _model.FromTable!.TableContentValues!
            .Select((record, index) => new ExecutionRow(index + 1, ToExecutionValues(record.ToRow())))
            .ToList();

        List<ExecutionRow> rows = OperatorPipelineRunner.ExecuteToList(new TableScanOperator(sourceRows));

        var listResult = rows
            .Select(row => new JoinedRow(_model.FromTable.TableName, new Row(new Dictionary<string, object?>(row.Values))))
            .ToList();

        return new ListedTable(listResult);
    }


    private ListedTable? TryEvaluateNearestNeighborUsingVectorIndex()
    {
        if (!_model.LimitTake.HasValue || _model.LimitTake.Value <= 0)
        {
            return null;
        }

        if (_model.Database is null)
        {
            return null;
        }

        var orderBy = _model.GetOrderByExpression();
        if (orderBy == null || orderBy.Columns.Count == 0)
        {
            return null;
        }

        OrderByColumnNode firstOrderColumn = orderBy.Columns[0];
        if (!firstOrderColumn.IsAscending)
        {
            return null;
        }

        var orderedSelectColumn = _model.GetSelectColumnByAlias(firstOrderColumn.Column.Name);
        if (orderedSelectColumn?.Expression is not BinaryExpressionNode distanceExpression)
        {
            return null;
        }

        if (distanceExpression.Operator != Operators.VECTOR_DISTANCE_COSINE
            && distanceExpression.Operator != Operators.VECTOR_DISTANCE_L2)
        {
            return null;
        }

        if (!TryResolveVectorDistanceExpression(distanceExpression, out string tableName, out string columnName, out float[] queryVector))
        {
            return null;
        }

        tableName = ResolveRealTableName(tableName);

        string columnType = Catalog.GetTableColumnType(tableName, _model.Database, columnName);
        if (!columnType.Equals("VECTOR", StringComparison.OrdinalIgnoreCase)
            && !columnType.Equals("Vector", StringComparison.OrdinalIgnoreCase))
        {
            throw new BindingException($"Vector operator '{distanceExpression.Operator}' can only be used with VECTOR columns (found '{tableName}.{columnName}' of type '{columnType}').");
        }

        if (!TryResolveVectorIndex(tableName, columnName, _model.Database, out string indexName, out IndexFile indexMetadata))
        {
            return null;
        }

        int topK = _model.LimitTake.Value + (_model.LimitSkip ?? 0);
        if (topK <= 0)
        {
            IncrementHybridRoutingCounter("hybrid.orderby.reject.invalid_topk");
            return null;
        }

        ExpressionNode? materializedWhere = MaterializeWhereForFastPath();
        if (!ShouldUseVectorFastPath(topK, materializedWhere, out string routingReason))
        {
            IncrementHybridRoutingCounter($"hybrid.orderby.reject.{routingReason}");
            LogHybridRoutingDecision($"reject:{routingReason}", topK, topK, null);
            return null;
        }

        IncrementHybridRoutingCounter("hybrid.orderby.accept");

        ExpressionNode? seedPredicate = ResolveSeedPredicateForExpansion(materializedWhere);
        int totalRows = _model.FromTable?.TableContentValues?.Count ?? 0;
        if (totalRows <= 0)
        {
            IncrementHybridRoutingCounter("hybrid.orderby.reject.empty_input");
            return new ListedTable();
        }

        int initialTopK = ResolveHybridOrderByInitialTopK(totalRows, topK, seedPredicate, out string initialSizingMode);
        IncrementHybridRoutingCounter($"hybrid.orderby.initial_topk.{initialSizingMode}");
        LogHybridRoutingDecision("accept", topK, initialTopK, seedPredicate);

        List<long> rowIds = SearchVectorWithExpansionIfNeeded(
            queryVector,
            indexName,
            tableName,
            indexMetadata.IndexKind,
            totalRows,
            initialTopK,
            seedPredicate);
        RuntimeQueryDiagnosticsScope.RecordVectorSearch(indexName, topK, _queryVectorExpansionPasses);
        if (rowIds.Count == 0)
        {
            return new ListedTable();
        }

        Dictionary<long, StoredRow> rows = Context.GetTypedTableContents(rowIds, tableName, _model.Database);
        TableData seedRows = [];
        foreach (long rowId in rowIds)
        {
            if (!rows.TryGetValue(rowId, out StoredRow? rowValues))
            {
                continue;
            }

            seedRows[rowId] = new Record(rowId, MaterializeStoredRow(rowValues));
        }

        if (_model.JoinStatement.ContainsJoin())
        {
            if (!IsNearestJoinTwoPhaseEligible(materializedWhere, out var embeddingFilter))
            {
                if (materializedWhere == null || !ReferencesOnlyFromTable(materializedWhere, _model.FromTable!.TableName))
                {
                    ExpressionNode? partialEmbeddingFilter = ResolveSeedPredicateForExpansion(materializedWhere);
                    try
                    {
                        return EvaluateJoinFromSeed(seedRows, partialEmbeddingFilter);
                    }
                    catch
                    {
                        return null;
                    }
                }

                embeddingFilter = materializedWhere;
            }

            return EvaluateJoinFromSeed(seedRows, embeddingFilter);
        }

        if (materializedWhere != null && !IsAllTrueExpression(materializedWhere))
        {
            seedRows = ApplyPredicateToSeedRows(seedRows, materializedWhere);
        }

        return new ListedTable(seedRows.Values
            .Select(record => new JoinedRow(_model.FromTable!.TableName, record.ToRow()))
            .ToList());
    }

    private bool IsNearestJoinTwoPhaseEligible(ExpressionNode? whereExpression, out ExpressionNode? embeddingFilter)
    {
        embeddingFilter = null;

        if (_model.TableService == null)
        {
            return false;
        }

        // Try to extract WHERE predicates that reference only the embedding table
        embeddingFilter = ExpressionExtractor.TryExtractTableSpecificPredicates(
            whereExpression,
            _model.FromTable.TableName,
            _model.TableService);

        // Check if we have valid filters (either extracted predicates or no WHERE clause at all)
        bool hasValidFilter = embeddingFilter != null || IsAllTrueExpression(whereExpression);

        if (!hasValidFilter)
        {
            return false; // WHERE references joined tables or unsupported patterns
        }

        // Verify all joins are INNER (only safe type for this optimization)
        return _model.JoinStatement.Model.JoinConditions
            .All(condition => condition.JoinType.Equals(JoinTypes.INNER, StringComparison.OrdinalIgnoreCase));
    }


    private ListedTable? TryEvaluateVectorPredicateUsingVectorIndex()
    {
        if (_model.Database is null)
        {
            return null;
        }

        ExpressionNode? materializedWhere = MaterializeWhereForFastPath();
        if (materializedWhere == null)
        {
            return null;
        }

        if (!TryExtractVectorDistancePredicate(
                materializedWhere,
                out string tableName,
                out string columnName,
                out float[] queryVector,
            out string distanceOperator,
            out string comparisonOperator,
            out double threshold))
        {
            return null;
        }

        tableName = ResolveRealTableName(tableName);

        string columnType = Catalog.GetTableColumnType(tableName, _model.Database, columnName);
        if (!columnType.Equals("VECTOR", StringComparison.OrdinalIgnoreCase)
            && !columnType.Equals("Vector", StringComparison.OrdinalIgnoreCase))
        {
            throw new BindingException($"Vector distance predicate can only be used with VECTOR columns (found '{tableName}.{columnName}' of type '{columnType}').");
        }

        if (!TryResolveVectorIndex(tableName, columnName, _model.Database, out string indexName, out IndexFile indexMetadata))
        {
            return null;
        }

        int totalRows = _model.FromTable?.TableContentValues?.Count ?? 0;
        if (totalRows <= 0)
        {
            return new ListedTable();
        }

        double distanceSelectivity = EstimateVectorDistanceSelectivity(distanceOperator, comparisonOperator, threshold);
        int topK = ResolveVectorPredicateTopK(totalRows, distanceSelectivity);
        if (!ShouldUseVectorPredicateFastPath(totalRows, topK, distanceSelectivity, materializedWhere, out string decisionReason))
        {
            LogVectorPredicateFastPathDecision($"rejected: {decisionReason}", totalRows, topK, distanceSelectivity);
            return null;
        }

        LogVectorPredicateFastPathDecision("accepted", totalRows, topK, distanceSelectivity);

        List<long> rowIds = SearchVectorWithExpansionIfNeeded(
            queryVector,
            indexName,
            tableName,
            indexMetadata.IndexKind,
            totalRows,
            topK,
            materializedWhere);
        RuntimeQueryDiagnosticsScope.RecordVectorSearch(indexName, topK, _queryVectorExpansionPasses);

        if (rowIds.Count == 0)
        {
            return new ListedTable();
        }

        Dictionary<long, StoredRow> rows = Context.GetTypedTableContents(rowIds, tableName, _model.Database);
        TableData seedRows = [];
        foreach (long rowId in rowIds)
        {
            if (!rows.TryGetValue(rowId, out StoredRow? rowValues))
            {
                continue;
            }

            seedRows[rowId] = new Record(rowId, MaterializeStoredRow(rowValues));
        }

        if (_model.JoinStatement.ContainsJoin())
        {
            if (!IsNearestJoinTwoPhaseEligible(materializedWhere, out var embeddingFilter))
            {
                return null;
            }

            return EvaluateJoinFromSeed(seedRows, embeddingFilter);
        }

        TableData filtered = ApplyPredicateToSeedRows(seedRows, materializedWhere);
        return new ListedTable(filtered.Values
            .Select(record => new JoinedRow(_model.FromTable!.TableName, record.ToRow()))
            .ToList());
    }


    private List<long> SearchVectorWithExpansionIfNeeded(
        float[] queryVector,
        string indexName,
        string tableName,
        string indexKind,
        int totalRows,
        int initialTopK,
        ExpressionNode? whereExpression)
    {
        int topK = ClampInt(initialTopK, 1, totalRows);
        int expansionFactor = Math.Max(2, Engine.Config.VectorPredicateFastPathExpansionFactor);
        int maxPasses = Math.Max(0, Engine.Config.VectorPredicateFastPathMaxExpansionPasses);
        int requiredRows = (_model.LimitTake ?? 0) + (_model.LimitSkip ?? 0);
        bool canEstimatePostFilter = whereExpression != null && !IsAllTrueExpression(whereExpression);
        int lastPostFilterEstimate = -1;

        List<long> rowIds = [];
        for (int pass = 0; pass <= maxPasses; pass++)
        {
            rowIds = Indexes.SearchVector(queryVector, topK, indexName, tableName, _model.Database!, indexKind);
            LogVectorPredicateFastPathExpansion($"pass={pass}; requestedTopK={topK}; returned={rowIds.Count}");
            if (rowIds.Count == 0)
            {
                return rowIds;
            }

            if (requiredRows <= 0 || topK >= totalRows || !canEstimatePostFilter)
            {
                return rowIds;
            }

            int postFilterEstimate = EstimatePostFilterCandidateMatches(rowIds, tableName, whereExpression!);
            lastPostFilterEstimate = postFilterEstimate;
            LogVectorPredicateFastPathExpansion($"pass={pass}; postFilterEstimate={postFilterEstimate}; required={requiredRows}");
            if (postFilterEstimate >= requiredRows)
            {
                return rowIds;
            }

            int nextTopK = Math.Min(totalRows, topK * expansionFactor);
            if (nextTopK <= topK)
            {
                return rowIds;
            }

            topK = nextTopK;
            _queryVectorExpansionPasses++;
            LogVectorPredicateFastPathExpansion($"pass={pass}; expandingTopK={topK}");
        }

        if (requiredRows > 0)
        {
            LogVectorPredicateFastPathExpansion($"exhausted; finalCandidates={rowIds.Count}; finalPostFilterEstimate={Math.Max(0, lastPostFilterEstimate)}; required={requiredRows}");
        }

        return rowIds;
    }

    private ExpressionNode? ResolveSeedPredicateForExpansion(ExpressionNode? whereExpression)
    {
        if (whereExpression == null || IsAllTrueExpression(whereExpression))
        {
            return null;
        }

        if (!_model.JoinStatement.ContainsJoin())
        {
            return ReferencesOnlyFromTable(whereExpression, _model.FromTable.TableName)
                ? whereExpression
                : null;
        }

        if (_model.TableService == null)
        {
            return null;
        }

        return ExpressionExtractor.TryExtractTableSpecificPredicates(
            whereExpression,
            _model.FromTable.TableName,
            _model.TableService);
    }


    private void LogHybridRoutingDecision(string outcome, int requestedTopK, int initialTopK, ExpressionNode? seedPredicate)
    {
        if (!Engine.Config.EnableVectorPredicateFastPathTelemetry)
        {
            return;
        }

        double selectivity = seedPredicate == null ? 1d : ClampDouble(EstimatePredicateSelectivity(seedPredicate), 0.01d, 1d);
        Logger.Info($"Planner(hybrid-route): {outcome}; requestedTopK={requestedTopK}; initialTopK={initialTopK}; seedSelectivity={selectivity:F3}");
    }

    private static Dictionary<string, object?> MaterializeStoredRow(StoredRow row)
    {
        StoredRowDictionaryView view = row.AsDictionary();
        var result = new Dictionary<string, object?>(view.Count, StringComparer.OrdinalIgnoreCase);
        foreach ((string key, object? value) in view)
        {
            result[key] = value;
        }

        return result;
    }

    private int EstimatePostFilterCandidateMatches(List<long> candidateRowIds, string tableName, ExpressionNode whereExpression)
    {
        Dictionary<long, StoredRow> rows = Context.GetTypedTableContents(candidateRowIds, tableName, _model.Database!);
        int count = 0;

        foreach (long rowId in candidateRowIds)
        {
            if (!rows.TryGetValue(rowId, out StoredRow? rowValues))
            {
                continue;
            }

            var joinedRow = new JoinedRow(_model.FromTable.TableName, new Row(MaterializeStoredRow(rowValues)));
            if (EvaluatePredicate(whereExpression, joinedRow))
            {
                count++;
            }
        }

        return count;
    }

    private void LogVectorPredicateFastPathDecision(string outcome, int totalRows, int topK, double selectivity)
    {
        if (!Engine.Config.EnableVectorPredicateFastPathTelemetry)
        {
            return;
        }

        Logger.Info($"Planner(vector-fastpath): {outcome}; rows={totalRows}; topK={topK}; selectivity={selectivity:F3}");
    }

    private void LogVectorPredicateFastPathExpansion(string message)
    {
        if (!Engine.Config.EnableVectorPredicateFastPathTelemetry)
        {
            return;
        }

        Logger.Info($"Planner(vector-fastpath-expansion): {message}");
    }


    private bool TryExtractVectorDistancePredicate(
        ExpressionNode expression,
        out string tableName,
        out string columnName,
        out float[] queryVector,
        out string distanceOperator,
        out string comparisonOperator,
        out double threshold)
    {
        tableName = string.Empty;
        columnName = string.Empty;
        queryVector = [];
        distanceOperator = string.Empty;
        comparisonOperator = string.Empty;
        threshold = 0d;

        if (expression is not BinaryExpressionNode binary)
        {
            return false;
        }

        if (binary.Operator.Equals(Operators.AND, StringComparison.OrdinalIgnoreCase))
        {
            return TryExtractVectorDistancePredicate(binary.Left, out tableName, out columnName, out queryVector, out distanceOperator, out comparisonOperator, out threshold)
                || TryExtractVectorDistancePredicate(binary.Right, out tableName, out columnName, out queryVector, out distanceOperator, out comparisonOperator, out threshold);
        }

        if (!TryGetDistanceComparison(binary, out var distanceExpression, out comparisonOperator, out threshold))
        {
            return false;
        }

        distanceOperator = distanceExpression.Operator;

        return TryResolveVectorDistanceExpression(distanceExpression, out tableName, out columnName, out queryVector);
    }

    private static bool TryGetDistanceComparison(
        BinaryExpressionNode expression,
        out BinaryExpressionNode distanceExpression,
        out string comparisonOperator,
        out double threshold)
    {
        distanceExpression = null!;
        comparisonOperator = string.Empty;
        threshold = 0d;

        if (!IsDistanceComparisonOperator(expression.Operator))
        {
            return false;
        }

        if (TryResolveDistanceComparisonParts(expression.Left, expression.Right, out distanceExpression, out threshold, out bool reversed))
        {
            comparisonOperator = reversed
                ? InvertComparisonOperator(expression.Operator)
                : expression.Operator;
            return true;
        }

        return false;
    }

    private static bool TryResolveDistanceComparisonParts(
        ExpressionNode left,
        ExpressionNode right,
        out BinaryExpressionNode distanceExpression,
        out double threshold,
        out bool reversed)
    {
        distanceExpression = null!;
        threshold = 0d;
        reversed = false;

        if (left is BinaryExpressionNode leftDistance
            && IsVectorDistanceOperator(leftDistance.Operator)
            && TryResolveNumericLiteral(right, out threshold))
        {
            distanceExpression = leftDistance;
            return true;
        }

        if (right is BinaryExpressionNode rightDistance
            && IsVectorDistanceOperator(rightDistance.Operator)
            && TryResolveNumericLiteral(left, out threshold))
        {
            distanceExpression = rightDistance;
            reversed = true;
            return true;
        }

        return false;
    }

    private static bool IsDistanceComparisonOperator(string op)
    {
        return op == Operators.LESS_THAN
            || op == Operators.LESS_THAN_OR_EQUAL_TO
            || op == Operators.GREATER_THAN
            || op == Operators.GREATER_THAN_OR_EQUAL_TO;
    }

    private static bool IsVectorDistanceOperator(string op)
    {
        return op == Operators.VECTOR_DISTANCE_COSINE
            || op == Operators.VECTOR_DISTANCE_L2;
    }

    private static string InvertComparisonOperator(string op)
    {
        return op switch
        {
            Operators.LESS_THAN => Operators.GREATER_THAN,
            Operators.LESS_THAN_OR_EQUAL_TO => Operators.GREATER_THAN_OR_EQUAL_TO,
            Operators.GREATER_THAN => Operators.LESS_THAN,
            Operators.GREATER_THAN_OR_EQUAL_TO => Operators.LESS_THAN_OR_EQUAL_TO,
            _ => op
        };
    }

    private static bool TryResolveNumericLiteral(ExpressionNode expression, out double value)
    {
        value = 0d;

        if (expression is not LiteralNode literal || literal.Value is null)
        {
            return false;
        }

        return literal.Value switch
        {
            byte v => Assign(v, out value),
            sbyte v => Assign(v, out value),
            short v => Assign(v, out value),
            ushort v => Assign(v, out value),
            int v => Assign(v, out value),
            uint v => Assign(v, out value),
            long v => Assign(v, out value),
            ulong v => Assign(v, out value),
            float v => Assign(v, out value),
            double v => Assign(v, out value),
            decimal v => Assign((double)v, out value),
            string s => double.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out value)
                || double.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.CurrentCulture, out value),
            _ => false
        };

        static bool Assign(double input, out double output)
        {
            output = input;
            return true;
        }
    }

    private ExpressionNode? MaterializeWhereForFastPath()
    {
        ExpressionNode? whereExpression = _model.WhereStatement.GetExpression();
        if (whereExpression == null)
        {
            return null;
        }

        if (_model.Database is null || _model.TableService is null)
        {
            return whereExpression;
        }

        return SubqueryExpressionMaterializer.Materialize(
            whereExpression,
            _model.Database,
            DataVoEngine.Current(),
            _model.TableService);
    }


    private void IncrementHybridRoutingCounter(string bucket)
    {
        if (!Engine.Config.EnableHybridRoutingTelemetryCounters)
        {
            return;
        }

        _queryHybridRoutingCounters.TryGetValue(bucket, out int current);
        _queryHybridRoutingCounters[bucket] = current + 1;
    }

    private void ResetQueryHybridTelemetry()
    {
        _queryHybridRoutingCounters.Clear();
        _queryVectorExpansionPasses = 0;
    }

    private void ReportHybridRoutingTelemetry(long elapsedMilliseconds, int selectedRows)
    {
        if (!Engine.Config.EnableHybridRoutingTelemetryCounters)
        {
            return;
        }

        int usedBuckets = 0;
        foreach (int count in _queryHybridRoutingCounters.Values)
        {
            usedBuckets += count;
        }

        bool usedHybridRoute = usedBuckets > 0;

        if (Engine.Config.EnableHybridRoutingPerQueryTelemetry && usedHybridRoute)
        {
            string buckets = string.Join(", ", _queryHybridRoutingCounters
                .OrderByDescending(pair => pair.Value)
                .Select(pair => $"{pair.Key}={pair.Value}"));

            Logger.Info($"Planner(hybrid-route-query): elapsedMs={elapsedMilliseconds}; rows={selectedRows}; expansionPasses={_queryVectorExpansionPasses}; buckets={buckets}");
        }

        bool hasSnapshot = Engine.Config.RecordHybridRoutingQueryAndTryBuildSnapshot(
            _queryHybridRoutingCounters,
            _queryVectorExpansionPasses,
            out string? snapshotMessage);

        if (hasSnapshot && !string.IsNullOrWhiteSpace(snapshotMessage))
        {
            Logger.Info($"Planner(hybrid-route-snapshot): {snapshotMessage}");
        }
    }

    private bool TryResolveVectorIndex(string tableName, string columnName, string databaseName, out string indexName, out IndexFile indexMetadata)
    {
        indexName = string.Empty;
        indexMetadata = null!;

        if (_model.FromTable.IndexedColumns is not null
            && _model.FromTable.IndexedColumns.TryGetValue(columnName, out string? mappedName)
            && !string.IsNullOrWhiteSpace(mappedName))
        {
            IndexFile? mappedMetadata = Catalog
                .GetTableIndexes(tableName, databaseName)
                .FirstOrDefault(index => index.IndexFileName.Equals(mappedName, StringComparison.OrdinalIgnoreCase));

            if (mappedMetadata is not null && Indexes.SupportsVectorIndexType(mappedMetadata.IndexKind))
            {
                indexName = mappedName;
                indexMetadata = mappedMetadata;
                return true;
            }
        }

        IndexFile? matched = Catalog
            .GetTableIndexes(tableName, databaseName)
            .FirstOrDefault(index => Indexes.SupportsVectorIndexType(index.IndexKind)
                && index.AttributeNames.Any(attr => attr.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                && !string.IsNullOrWhiteSpace(index.IndexFileName));

        if (matched is null)
        {
            return false;
        }

        indexName = matched.IndexFileName;
        indexMetadata = matched;
        return true;
    }

    private string ResolveRealTableName(string tableName)
    {
        if (_model.TableService == null || string.IsNullOrWhiteSpace(tableName))
        {
            return tableName;
        }

        string resolved = _model.TableService.GetRealTableName(tableName);
        return string.IsNullOrWhiteSpace(resolved) ? tableName : resolved;
    }

    private static int EstimatePredicateComplexity(ExpressionNode node)
    {
        return node switch
        {
            BinaryExpressionNode binary when binary.Operator == Operators.AND || binary.Operator == Operators.OR
                => 1 + EstimatePredicateComplexity(binary.Left) + EstimatePredicateComplexity(binary.Right),
            BinaryExpressionNode => 1,
            ScalarFunctionExpressionNode scalar => 2 + EstimateScalarArgumentComplexity(scalar),
            _ => 1
        };
    }

    private static int EstimateScalarArgumentComplexity(ScalarFunctionExpressionNode scalar)
    {
        int complexity = 0;
        foreach (ExpressionNode argument in scalar.Arguments)
        {
            complexity += EstimatePredicateComplexity(argument);
        }

        return complexity;
    }

    private bool ReferencesOnlyFromTable(ExpressionNode expression, string fromTableName)
    {
        return expression switch
        {
            BinaryExpressionNode binary => ReferencesOnlyFromTable(binary.Left, fromTableName)
                && ReferencesOnlyFromTable(binary.Right, fromTableName),
            ScalarFunctionExpressionNode scalar => scalar.Arguments.All(arg => ReferencesOnlyFromTable(arg, fromTableName)),
            WindowFunctionExpressionNode window => ReferencesOnlyFromTable(window.OrderByColumn, fromTableName)
                && window.PartitionByColumns.All(col => ReferencesOnlyFromTable(col, fromTableName)),
            ResolvedColumnRefNode resolved => resolved.TableName.Equals(fromTableName, StringComparison.OrdinalIgnoreCase),
            ColumnRefNode colRef => IsFromTableReference(colRef, fromTableName),
            _ => true
        };
    }

    private bool IsFromTableReference(ColumnRefNode columnRef, string fromTableName)
    {
        if (string.IsNullOrWhiteSpace(columnRef.TableOrAlias))
        {
            return true;
        }

        if (_model.TableService == null)
        {
            return columnRef.TableOrAlias.Equals(fromTableName, StringComparison.OrdinalIgnoreCase);
        }

        string resolvedName = _model.TableService.GetRealTableName(columnRef.TableOrAlias);
        return resolvedName.Equals(fromTableName, StringComparison.OrdinalIgnoreCase)
            || columnRef.TableOrAlias.Equals(fromTableName, StringComparison.OrdinalIgnoreCase);
    }

    private bool ShouldAvoidSortPushdownForSpillRisk(ExpressionNode? whereExpression, bool hasJoin)
    {
        if (!Engine.Config.EnableVolcanoSpillGuardrails)
        {
            return false;
        }

        int threshold = Engine.Config.VolcanoSortSpillThresholdRows;
        if (threshold <= 0)
        {
            return false;
        }

        int estimatedRows = EstimateRowsForSpillRisk(whereExpression, hasJoin);
        return estimatedRows > threshold;
    }

    private bool ShouldAvoidAggregatePushdownForSpillRisk(ExpressionNode? whereExpression, bool hasJoin)
    {
        if (!Engine.Config.EnableVolcanoSpillGuardrails)
        {
            return false;
        }

        int threshold = Engine.Config.VolcanoAggregateSpillThresholdRows;
        if (threshold <= 0)
        {
            return false;
        }

        int estimatedRows = EstimateRowsForSpillRisk(whereExpression, hasJoin);
        return estimatedRows > threshold;
    }

    private int EstimateRowsForSpillRisk(ExpressionNode? whereExpression, bool hasJoin)
    {
        int baseRows = hasJoin
            ? EstimateJoinInputRowCount()
            : (_model.FromTable?.TableContentValues?.Count ?? 0);

        if (baseRows <= 0)
        {
            return 0;
        }

        double selectivity = EstimatePredicateSelectivity(whereExpression);
        return Math.Max(1, (int)Math.Ceiling(baseRows * selectivity));
    }

    private ListedTable EvaluateJoinFromSeed(TableData seedRows, ExpressionNode? embeddingFilter = null)
    {
        if (seedRows.Count == 0)
        {
            return new ListedTable();
        }

        // Phase 2: Apply embedding table predicates to filter seed rows
        TableData filteredRows = seedRows;
        if (embeddingFilter != null)
        {
            filteredRows = ApplyPredicateToSeedRows(seedRows, embeddingFilter);

            if (filteredRows.Count == 0)
            {
                return new ListedTable(); // No rows passed the filter
            }
        }

        // Phase 3: Original join execution on filtered seed set
        HashedTable groupedInitialTable = [];
        foreach (var row in filteredRows)
        {
            groupedInitialTable.Add(new JoinedRowId(row.Key), new JoinedRow(_model.FromTable.TableName, row.Value.ToRow()));
        }

        return _model.JoinStatement!
            .Evaluate(groupedInitialTable, _model.FromTable.TableName)
            .ToListedTable();
    }

    private TableData ApplyPredicateToSeedRows(TableData seedRows, ExpressionNode predicate)
    {
        TableData result = [];

        foreach (var seedRow in seedRows)
        {
            // Create a JoinedRow to evaluate the predicate
            var joinedRow = new JoinedRow(_model.FromTable.TableName, seedRow.Value.ToRow());

            // Evaluate the predicate against this row
            if (EvaluatePredicate(predicate, joinedRow))
            {
                result[seedRow.Key] = seedRow.Value;
            }
        }

        return result;
    }

    private bool TryResolveVectorDistanceExpression(BinaryExpressionNode expression, out string tableName, out string columnName, out float[] queryVector)
    {
        tableName = string.Empty;
        columnName = string.Empty;
        queryVector = [];

        if (TryResolveVectorColumnReference(expression.Left, out tableName, out columnName)
            && TryResolveVectorLiteral(expression.Right, out queryVector))
        {
            return true;
        }

        if (TryResolveVectorColumnReference(expression.Right, out tableName, out columnName)
            && TryResolveVectorLiteral(expression.Left, out queryVector))
        {
            return true;
        }

        return false;
    }

    private bool TryResolveVectorColumnReference(ExpressionNode expression, out string tableName, out string columnName)
    {
        tableName = string.Empty;
        columnName = string.Empty;

        if (expression is ResolvedColumnRefNode resolved)
        {
            tableName = resolved.TableName;
            columnName = resolved.Column;
            return true;
        }

        if (expression is ColumnRefNode colRef)
        {
            string reference = string.IsNullOrWhiteSpace(colRef.TableOrAlias)
                ? colRef.Column
                : $"{colRef.TableOrAlias}.{colRef.Column}";

            var resolvedTuple = _model.TableService!.ParseAndFindTableNameByColumn(reference);
            tableName = resolvedTuple.Item1;
            columnName = resolvedTuple.Item2;
            return true;
        }

        return false;
    }

    private static bool TryResolveVectorLiteral(ExpressionNode expression, out float[] vector)
    {
        vector = [];

        if (expression is LiteralNode literal)
        {
            return VectorParser.TryCoerceToVector(literal.Value, out vector);
        }

        if (expression is NullLiteralNode)
        {
            return false;
        }

        return false;
    }

    private ListedTable EvaluateWhereWithExpression(ExpressionNode whereExpression)
    {
        if (!_model.JoinStatement.ContainsJoin() && CanEvaluateWhereWithoutJoin(whereExpression))
        {
            string tableName = _model.FromTable!.TableName;
            string databaseName = _model.Database
                ?? throw new InvalidOperationException("SELECT requires an active database before evaluating the WHERE clause.");
            HashSet<long> rowIds = new Where(whereExpression, _model.FromTable).EvaluateWithoutJoin(tableName, databaseName);
            if (rowIds.Count == 0)
            {
                return new ListedTable();
            }

            Dictionary<long, StoredRow> rows = Context.GetTypedTableContents([.. rowIds], tableName, databaseName);
            return new ListedTable([.. rowIds
                .OrderBy(static id => id)
                .Where(rows.ContainsKey)
                .Select(id => new JoinedRow(tableName, new Row(MaterializeStoredRow(rows[id]))))]);
        }

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

    private static bool CanEvaluateWhereWithoutJoin(ExpressionNode expression)
    {
        if (expression is LiteralNode)
        {
            return true;
        }

        if (expression is not BinaryExpressionNode binaryExpression)
        {
            return false;
        }

        if (binaryExpression.Operator == Operators.AND || binaryExpression.Operator == Operators.OR)
        {
            return CanEvaluateWhereWithoutJoin(binaryExpression.Left) && CanEvaluateWhereWithoutJoin(binaryExpression.Right);
        }

        BinaryExpressionNode normalizedExpression = ExpressionNodeNormalizer.NormalizeComparisonNode(binaryExpression);
        return normalizedExpression.Left switch
        {
            ResolvedColumnRefNode when normalizedExpression.Right is LiteralNode => true,
            LiteralNode when normalizedExpression.Right is LiteralNode => true,
            ResolvedColumnRefNode when normalizedExpression.Right is ResolvedColumnRefNode => true,
            BinaryExpressionNode distanceExpression when normalizedExpression.Right is LiteralNode
                => distanceExpression.Operator == Operators.VECTOR_DISTANCE_COSINE
                    || distanceExpression.Operator == Operators.VECTOR_DISTANCE_L2,
            _ => false
        };
    }

    private ListedTable EvaluateNoJoinWithVolcano(ExpressionNode? whereExpression)
    {
        Logger.Info("Planner: using Volcano no-join pipeline.");

        var sourceRows = _model.FromTable!.TableContentValues!
            .Select((record, index) =>
            {
                var row = record.ToRow();
                var values = new Dictionary<string, object?>();
                foreach (string key in row.Keys)
                {
                    values[key] = row[key];
                }

                return new ExecutionRow(index + 1, values);
            })
            .ToList();

        IQueryOperator root = new TableScanOperator(sourceRows);
        if (whereExpression != null)
        {
            root = new FilterOperator(root, (TypedExecutionRow typedRow) =>
            {
                var joinedRow = BuildSingleTableJoinedRow(_model.FromTable.TableName, typedRow.Values);
                return EvaluatePredicate(whereExpression, joinedRow);
            });
        }

        if (TryBuildNoJoinGroupByPushdown(out var groupByColumns))
        {
            Logger.Info($"Planner: push down GROUP BY as distinct ({groupByColumns.Count} keys).");
            root = new DistinctOperator(root, (ExecutionRow row) => BuildDistinctKey(row, groupByColumns));
            _volcanoGroupByPushedDown = true;
        }

        if (TryBuildNoJoinAggregatePushdown(out var aggregateGroupByColumns, out var aggregateSpecs))
        {
            if (ShouldAvoidAggregatePushdownForSpillRisk(whereExpression, hasJoin: false))
            {
                Logger.Info("Planner: skip aggregate pushdown due to spill guardrail estimate.");
            }
            else
            {
                Logger.Info($"Planner: push down aggregate ({aggregateSpecs.Count} functions).");
                root = new HashAggregateOperator(root, aggregateGroupByColumns, aggregateSpecs, BuildAggregateExecutionOptions());
                _volcanoAggregatePushedDown = true;
                _volcanoAggregateGroupKeyColumns = [.. aggregateGroupByColumns];
                _volcanoAggregateOutputColumns = [.. aggregateSpecs.Select(spec => spec.OutputColumn)];
            }
        }

        if (TryBuildNoJoinProjectionPushdown(out var projectionColumns))
        {
            Logger.Info($"Planner: push down projection ({projectionColumns.Count} columns).");
            root = new ProjectOperator(root, (TypedExecutionRow typedRow) =>
            {
                var values = new Dictionary<string, object?>();
                foreach (string column in projectionColumns)
                {
                    if (typedRow.Values.TryGetValue(column, out var value))
                    {
                        values[column] = value;
                    }
                }

                return values;
            });

        }

        bool orderPushedDown = false;
        if (TryBuildNoJoinOrderPushdown(out var orderKeys))
        {
            if (ShouldAvoidSortPushdownForSpillRisk(whereExpression, hasJoin: false))
            {
                Logger.Info("Planner: skip ORDER BY pushdown due to spill guardrail estimate.");
            }
            else
            {
                Logger.Info($"Planner: push down ORDER BY ({orderKeys.Count} keys).");
                List<SortOperator.SortKeySpec> sortSpecs = [];
                foreach (var orderKey in orderKeys)
                {
                    string key = orderKey.Key;
                    bool ascending = orderKey.IsAscending;
                    sortSpecs.Add(new SortOperator.SortKeySpec(
                        (TypedExecutionRow row) => ResolveNoJoinOrderValue(row, key),
                        ascending));
                }

                root = new SortOperator(root, sortSpecs, BuildSortExecutionOptions());
                orderPushedDown = true;
                _volcanoOrderPushedDown = true;
            }
        }

        if (TryBuildNoJoinDistinctPushdown(out var distinctColumns))
        {
            Logger.Info($"Planner: push down DISTINCT ({distinctColumns.Count} keys).");
            root = new DistinctOperator(root, (TypedExecutionRow typedRow) => BuildDistinctKey(typedRow, distinctColumns));
            _volcanoDistinctPushedDown = true;
        }

        if (CanPushDownOffsetToVolcano(orderPushedDown))
        {
            root = new SkipOperator(root, _model.LimitSkip!.Value);
            _volcanoOffsetPushedDown = true;
        }

        if (CanPushDownLimitToVolcano(orderPushedDown))
        {
            root = new TakeOperator(root, _model.LimitTake!.Value);
            _volcanoLimitPushedDown = true;
        }

        List<ExecutionRow> filteredRows = OperatorPipelineRunner.ExecuteToList(root, GetVolcanoRunnerMaxRows());
        List<JoinedRow> listed;
        if (_volcanoAggregatePushedDown)
        {
            listed = filteredRows.Select(row =>
            {
                var baseValues = new Dictionary<string, object?>();
                foreach (string key in _volcanoAggregateGroupKeyColumns)
                {
                    if (row.Values.TryGetValue(key, out var value))
                    {
                        baseValues[key] = value;
                    }
                }

                var aggValues = new Dictionary<string, object?>();
                foreach (var entry in row.Values)
                {
                    if (!_volcanoAggregateGroupKeyColumns.Contains(entry.Key))
                    {
                        aggValues[entry.Key] = entry.Value;
                    }
                }

                var joined = new JoinedRow(_model.FromTable.TableName, new Row(baseValues));
                joined.Add(GroupBy.HASH_VALUE, new Row(aggValues));
                return joined;
            }).ToList();
        }
        else
        {
            listed = filteredRows
                .Select(row => new JoinedRow(_model.FromTable.TableName, new Row(new Dictionary<string, object?>(row.Values))))
                .ToList();
        }

        return new ListedTable(listed);
    }

    private ListedTable EvaluateInnerJoinWithVolcano(ExpressionNode? whereExpression)
    {
        Logger.Info("Planner: using Volcano join pipeline.");

        string fromTableName = _model.FromTable.TableName;
        Dictionary<string, ExpressionNode> tableFilters = BuildJoinTableFilterPushdowns(whereExpression);
        if (tableFilters.Count > 0)
        {
            Logger.Info($"Planner: push down JOIN table filters ({tableFilters.Count} tables).");
        }

        HashSet<string> joinedTables = [fromTableName];
        List<string> joinOrder = [fromTableName];
        List<(string FeedbackKey, CountingPassthroughOperator Counter, int EstimatedOutputRows)> joinCounters = [];
        List<DataVo.Core.Models.Statement.JoinModel.JoinCondition> remaining = [.. _model.JoinStatement.Model.JoinConditions];

        var initialRows = _model.FromTable.TableContentValues!
            .Select((record, index) => new ExecutionRow(index + 1, ToExecutionValues(record.ToRow())))
            .ToList();

        IQueryOperator root = new TableScanOperator(initialRows);
        if (tableFilters.TryGetValue(fromTableName, out var fromFilter))
        {
            root = BuildTableFilterOperator(root, fromTableName, fromFilter);
        }

        while (remaining.Count > 0)
        {
            int pickIndex = PickNextVolcanoJoinConditionIndex(
                remaining,
                joinedTables,
                tableFilters,
                out bool leftSideAlreadyJoined);

            if (pickIndex < 0)
            {
                throw new EvaluationException("Unable to build Volcano join pipeline for disconnected INNER JOIN graph.");
            }

            var picked = remaining[pickIndex];
            remaining.RemoveAt(pickIndex);

            string existingTable = leftSideAlreadyJoined ? picked.LeftColumn.TableName : picked.RightColumn.TableName;
            string existingColumn = leftSideAlreadyJoined ? picked.LeftColumn.ColumnName : picked.RightColumn.ColumnName;
            string newTable = leftSideAlreadyJoined ? picked.RightColumn.TableName : picked.LeftColumn.TableName;
            string newColumn = leftSideAlreadyJoined ? picked.RightColumn.ColumnName : picked.LeftColumn.ColumnName;

            string streamJoinKey = joinedTables.Count == 1 && existingTable.Equals(fromTableName, StringComparison.OrdinalIgnoreCase)
                ? existingColumn
                : $"{existingTable}.{existingColumn}";

            var newTableDetail = _model.TableService!.GetTableDetailByAliasOrName(newTable);
            var newRows = newTableDetail.TableContentValues!
                .Select((record, index) => new ExecutionRow(index + 1, ToExecutionValues(record.ToRow())))
                .ToList();

            IQueryOperator rightInput = new TableScanOperator(newRows);
            if (tableFilters.TryGetValue(newTable, out var tableFilter))
            {
                rightInput = BuildTableFilterOperator(rightInput, newTable, tableFilter);
            }

            int estimatedLeftRows = EstimateJoinedStreamRows(joinedTables, tableFilters);
            int estimatedRightRows = ResolveJoinTableRowCount(newTable, tableFilters);
            JoinEdgePhysicalPlan joinPlan = BuildJoinEdgePhysicalPlan(
                buildSideTable: newTable,
                estimatedLeftRows,
                estimatedRightRows,
                leftJoinTable: existingTable,
                leftJoinColumn: existingColumn,
                rightJoinTable: newTable,
                rightJoinColumn: newColumn,
                tableFilters);

            string feedbackKey = BuildJoinFeedbackKey(existingTable, existingColumn, newTable, newColumn);
            root = BuildVolcanoInnerJoinOperator(
                root,
                rightInput,
                streamJoinKey,
                newColumn,
                existingTable,
                newTable,
                joinPlan);

            var countedJoin = new CountingPassthroughOperator(root);
            root = countedJoin;
            joinCounters.Add((feedbackKey, countedJoin, joinPlan.EstimatedOutputRows));

            Logger.Info($"Planner: physical join edge plan {existingTable}->{newTable}: alg={joinPlan.Algorithm}, build={joinPlan.BuildSide}({joinPlan.EstimatedBuildRows}), probe={joinPlan.ProbeSide}({joinPlan.EstimatedProbeRows}), cost={joinPlan.EstimatedCost}, reason={joinPlan.Reason}");

            Logger.Info($"Planner: appended INNER JOIN edge {existingTable}.{existingColumn} = {newTable}.{newColumn}");

            joinedTables.Add(newTable);
            joinOrder.Add(newTable);
        }

        if (!IsAllTrueExpression(whereExpression))
        {
            root = new FilterOperator(root, (TypedExecutionRow typedRow) =>
            {
                var joinedRow = ToJoinedRowFromJoinExecution(typedRow, joinOrder);
                return EvaluatePredicate(whereExpression!, joinedRow);
            });
        }

        if (TryBuildJoinProjectionPushdown(out var projectionColumns))
        {
            Logger.Info($"Planner: push down JOIN projection ({projectionColumns.Count} columns).");
            root = new ProjectOperator(root, (TypedExecutionRow typedRow) =>
            {
                var values = new Dictionary<string, object?>();
                foreach (string column in projectionColumns)
                {
                    if (typedRow.Values.TryGetValue(column, out var value))
                    {
                        values[column] = value;
                    }
                }

                return values;
            });

        }

        if (TryBuildJoinDistinctPushdown(out var distinctColumns))
        {
            Logger.Info($"Planner: push down JOIN DISTINCT ({distinctColumns.Count} keys).");
            root = new DistinctOperator(root, (TypedExecutionRow typedRow) => BuildDistinctKey(typedRow, distinctColumns));
            _volcanoDistinctPushedDown = true;
        }

        bool orderPushedDown = false;
        if (TryBuildJoinOrderPushdown(out var orderKeys))
        {
            if (ShouldAvoidSortPushdownForSpillRisk(whereExpression, hasJoin: true))
            {
                Logger.Info("Planner: skip JOIN ORDER BY pushdown due to spill guardrail estimate.");
            }
            else
            {
                Logger.Info($"Planner: push down JOIN ORDER BY ({orderKeys.Count} keys).");
                List<SortOperator.SortKeySpec> sortSpecs = [];
                foreach (var orderKey in orderKeys)
                {
                    string key = orderKey.Key;
                    bool ascending = orderKey.IsAscending;
                    sortSpecs.Add(new SortOperator.SortKeySpec(
                        (TypedExecutionRow row) => ResolveJoinOrderValue(row, key),
                        ascending));
                }

                root = new SortOperator(root, sortSpecs, BuildSortExecutionOptions());
                orderPushedDown = true;
                _volcanoOrderPushedDown = true;
            }
        }

        if (CanPushDownOffsetToVolcano(orderPushedDown))
        {
            root = new SkipOperator(root, _model.LimitSkip!.Value);
            _volcanoOffsetPushedDown = true;
        }

        if (CanPushDownLimitToVolcano(orderPushedDown))
        {
            root = new TakeOperator(root, _model.LimitTake!.Value);
            _volcanoLimitPushedDown = true;
        }

        List<ExecutionRow> joinedRows = OperatorPipelineRunner.ExecuteToList(root, GetVolcanoRunnerMaxRows());
        UpdateJoinCardinalityFeedback(joinCounters);

        List<JoinedRow> listed = joinedRows
            .Select(row => ToJoinedRowFromJoinExecution(row, joinOrder))
            .ToList();

        return new ListedTable(listed);
    }

    private Dictionary<string, ExpressionNode> BuildJoinTableFilterPushdowns(ExpressionNode? whereExpression)
    {
        Dictionary<string, ExpressionNode> extracted = new(StringComparer.OrdinalIgnoreCase);

        if (_model.TableService == null || whereExpression == null || IsAllTrueExpression(whereExpression))
        {
            return extracted;
        }

        HashSet<string> candidateTables = [_model.FromTable.TableName];
        foreach (var detail in _model.JoinStatement.Model.JoinTableDetails.Values)
        {
            candidateTables.Add(detail.TableName);
        }

        foreach (string table in candidateTables)
        {
            ExpressionNode? tablePredicate = ExpressionExtractor.TryExtractTableSpecificPredicates(whereExpression, table, _model.TableService);
            if (tablePredicate != null && !IsAllTrueExpression(tablePredicate))
            {
                extracted[table] = tablePredicate;
            }
        }

        return extracted;
    }

    private IQueryOperator BuildTableFilterOperator(IQueryOperator input, string tableName, ExpressionNode filterExpression)
    {
        return new FilterOperator(input, (TypedExecutionRow typedRow) =>
        {
            var joinedRow = BuildSingleTableJoinedRow(tableName, typedRow.Values);
            return EvaluatePredicate(filterExpression, joinedRow);
        });
    }

    private SortOperator.SortExecutionOptions BuildSortExecutionOptions()
    {
        return new SortOperator.SortExecutionOptions
        {
            EnableExternalSpill = Engine.Config.EnableVolcanoExternalSortSpill,
            SpillThresholdRows = Engine.Config.VolcanoExternalSortThresholdRows,
            SpillRunSizeRows = Engine.Config.VolcanoExternalSortRunSizeRows,
            SpillDirectory = Engine.Config.VolcanoExternalSortTempDirectory
        };
    }

    private HashAggregateOperator.AggregateExecutionOptions BuildAggregateExecutionOptions()
    {
        return new HashAggregateOperator.AggregateExecutionOptions
        {
            EnableExternalSpill = Engine.Config.EnableVolcanoExternalAggregateSpill,
            SpillThresholdRows = Engine.Config.VolcanoExternalAggregateThresholdRows,
            PartitionCount = Engine.Config.VolcanoExternalAggregatePartitionCount,
            SpillDirectory = Engine.Config.VolcanoExternalAggregateTempDirectory,
            EnableAdaptivePartitioning = Engine.Config.VolcanoExternalAggregateAdaptivePartitioning,
            TargetRowsPerPartition = Engine.Config.VolcanoExternalAggregateTargetRowsPerPartition,
            MaxPartitionCount = Engine.Config.VolcanoExternalAggregateMaxPartitionCount
        };
    }

    private JoinEdgePhysicalPlan BuildJoinEdgePhysicalPlan(
        string buildSideTable,
        int estimatedLeftRows,
        int estimatedRightRows,
        string leftJoinTable,
        string leftJoinColumn,
        string rightJoinTable,
        string rightJoinColumn,
        Dictionary<string, ExpressionNode> tableFilters)
    {
        JoinPlanSide buildSide;
        JoinPlanSide probeSide;
        int buildRows;
        int probeRows;
        string buildTable;
        string buildColumn;
        string probeTable;
        string probeColumn;

        if (estimatedLeftRows <= estimatedRightRows)
        {
            buildSide = JoinPlanSide.Left;
            probeSide = JoinPlanSide.Right;
            buildRows = Math.Max(1, estimatedLeftRows);
            probeRows = Math.Max(1, estimatedRightRows);
            buildTable = leftJoinTable;
            buildColumn = leftJoinColumn;
            probeTable = rightJoinTable;
            probeColumn = rightJoinColumn;
        }
        else
        {
            buildSide = JoinPlanSide.Right;
            probeSide = JoinPlanSide.Left;
            buildRows = Math.Max(1, estimatedRightRows);
            probeRows = Math.Max(1, estimatedLeftRows);
            buildTable = rightJoinTable;
            buildColumn = rightJoinColumn;
            probeTable = leftJoinTable;
            probeColumn = leftJoinColumn;
        }

        int buildDistinct = EstimateJoinKeyDistinct(buildTable, buildColumn, tableFilters, buildRows);
        int probeDistinct = EstimateJoinKeyDistinct(probeTable, probeColumn, tableFilters, probeRows);
        int estimatedOutputRows = EstimateJoinEdgeOutputRows(
            estimatedLeftRows,
            estimatedRightRows,
            leftJoinTable,
            leftJoinColumn,
            rightJoinTable,
            rightJoinColumn,
            tableFilters,
            out int estimatedOutputRowsHeuristic,
            out bool hasLearnedOutput,
            out long learnedOutputRows);

        int nestedLoopThreshold = Math.Max(1, Engine.Config.VolcanoNestedLoopJoinThresholdRows);
        int probeThreshold = nestedLoopThreshold * 8;
        int outputThreshold = nestedLoopThreshold * 8;

        int hashCost = buildRows + probeRows + (estimatedOutputRows / 2);
        int nestedLoopCost = buildRows * probeRows;

        bool nestedLoopEligible = buildRows <= nestedLoopThreshold
            && probeRows <= probeThreshold
            && estimatedOutputRows <= outputThreshold
            && nestedLoopCost <= hashCost * 2;

        JoinPhysicalAlgorithm algorithm = nestedLoopEligible
            ? JoinPhysicalAlgorithm.NestedLoop
            : JoinPhysicalAlgorithm.Hash;

        int estimatedCost = algorithm == JoinPhysicalAlgorithm.NestedLoop
            ? nestedLoopCost
            : hashCost;

        string feedbackReason = hasLearnedOutput
            ? $", learnedOut={learnedOutputRows}"
            : ", learnedOut=<none>";

        return new JoinEdgePhysicalPlan(
            algorithm,
            buildSide,
            probeSide,
            buildRows,
            probeRows,
            estimatedOutputRows,
            estimatedCost,
            $"chosen using left={estimatedLeftRows}, right={estimatedRightRows}, ndv(build/probe)=({buildDistinct}/{probeDistinct}), estOut={estimatedOutputRows} (heuristic={estimatedOutputRowsHeuristic}{feedbackReason}), thresholds(build/probe/out)=({nestedLoopThreshold}/{probeThreshold}/{outputThreshold}), edge build table '{buildSideTable}'");
    }

    private int EstimateJoinEdgeOutputRows(
        int estimatedLeftRows,
        int estimatedRightRows,
        string leftJoinTable,
        string leftJoinColumn,
        string rightJoinTable,
        string rightJoinColumn,
        Dictionary<string, ExpressionNode> tableFilters,
        out int estimatedOutputRowsHeuristic,
        out bool hasLearnedOutput,
        out long learnedOutputRows)
    {
        int leftDistinct = EstimateJoinKeyDistinct(leftJoinTable, leftJoinColumn, tableFilters, Math.Max(1, estimatedLeftRows));
        int rightDistinct = EstimateJoinKeyDistinct(rightJoinTable, rightJoinColumn, tableFilters, Math.Max(1, estimatedRightRows));
        int maxDistinct = Math.Max(1, Math.Max(leftDistinct, rightDistinct));

        estimatedOutputRowsHeuristic = Math.Max(1, (int)Math.Ceiling((double)(Math.Max(1, estimatedLeftRows) * Math.Max(1, estimatedRightRows)) / maxDistinct));
        string feedbackKey = BuildJoinFeedbackKey(leftJoinTable, leftJoinColumn, rightJoinTable, rightJoinColumn);

        hasLearnedOutput = TryGetJoinCardinalityFeedback(feedbackKey, out learnedOutputRows);
        if (hasLearnedOutput)
        {
            return Math.Max(1, (int)Math.Round((estimatedOutputRowsHeuristic * 0.4d) + (learnedOutputRows * 0.6d)));
        }

        return estimatedOutputRowsHeuristic;
    }

    private bool TryGetJoinCardinalityFeedback(string edgeKey, out long learnedRows)
    {
        if (!Engine.Config.EnableVolcanoJoinCardinalityFeedback)
        {
            learnedRows = 0;
            return false;
        }

        EnsureJoinCardinalityFeedbackLoaded();

        if (_joinCardinalityFeedback.TryGetValue(edgeKey, out var value))
        {
            learnedRows = Math.Max(1L, (long)Math.Round(value));
            return true;
        }

        learnedRows = 0;
        return false;
    }

    private static void RecordJoinCardinalityFeedback(string edgeKey, long observedRows)
    {
        double sanitizedObserved = Math.Max(1L, observedRows);
        _joinCardinalityFeedback.AddOrUpdate(
            edgeKey,
            sanitizedObserved,
            (_, existing) => (existing * (1d - JoinCardinalityFeedbackAlpha)) + (sanitizedObserved * JoinCardinalityFeedbackAlpha));
    }

    private void UpdateJoinCardinalityFeedback(
        List<(string FeedbackKey, CountingPassthroughOperator Counter, int EstimatedOutputRows)> joinCounters)
    {
        if (!Engine.Config.EnableVolcanoJoinCardinalityFeedback)
        {
            return;
        }

        EnsureJoinCardinalityFeedbackLoaded();

        foreach (var (feedbackKey, counter, estimatedOutputRows) in joinCounters)
        {
            long observed = Math.Max(1, counter.EmittedRows);
            RecordJoinCardinalityFeedback(feedbackKey, observed);
            Logger.Info($"Planner: join feedback learned for {feedbackKey}: observed={observed}, estimated={estimatedOutputRows}");
        }

        TrimJoinCardinalityFeedbackEntries();
        PersistJoinCardinalityFeedbackIfEnabled();
    }

    private void EnsureJoinCardinalityFeedbackLoaded()
    {
        if (!Engine.Config.EnableVolcanoJoinCardinalityFeedbackPersistence)
        {
            return;
        }

        string? path = Engine.Config.VolcanoJoinCardinalityFeedbackPersistenceFile;
        if (string.IsNullOrWhiteSpace(path))
        {
            return;
        }

        lock (_joinCardinalityFeedbackSync)
        {
            bool samePath = string.Equals(_joinCardinalityFeedbackLoadedPath, path, StringComparison.OrdinalIgnoreCase);
            if (_joinCardinalityFeedbackLoaded && samePath)
            {
                return;
            }

            _joinCardinalityFeedback.Clear();

            try
            {
                if (File.Exists(path))
                {
                    string json = File.ReadAllText(path);
                    Dictionary<string, double>? loaded = JsonSerializer.Deserialize<Dictionary<string, double>>(json);
                    if (loaded != null)
                    {
                        foreach (var entry in loaded)
                        {
                            if (!string.IsNullOrWhiteSpace(entry.Key) && entry.Value > 0)
                            {
                                _joinCardinalityFeedback[entry.Key] = entry.Value;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Info($"Planner: unable to load join feedback persistence: {ex.Message}");
            }

            _joinCardinalityFeedbackLoadedPath = path;
            _joinCardinalityFeedbackLoaded = true;
        }
    }

    private void PersistJoinCardinalityFeedbackIfEnabled()
    {
        if (!Engine.Config.EnableVolcanoJoinCardinalityFeedbackPersistence)
        {
            return;
        }

        string? path = Engine.Config.VolcanoJoinCardinalityFeedbackPersistenceFile;
        if (string.IsNullOrWhiteSpace(path))
        {
            return;
        }

        try
        {
            string directory = Path.GetDirectoryName(path) ?? string.Empty;
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var snapshot = _joinCardinalityFeedback.ToDictionary(k => k.Key, v => v.Value, StringComparer.OrdinalIgnoreCase);
            string json = JsonSerializer.Serialize(snapshot);
            File.WriteAllText(path, json);
        }
        catch (Exception ex)
        {
            Logger.Info($"Planner: unable to persist join feedback: {ex.Message}");
        }
    }

    private void TrimJoinCardinalityFeedbackEntries()
    {
        int maxEntries = Math.Max(16, Engine.Config.VolcanoJoinCardinalityFeedbackMaxEntries);
        if (_joinCardinalityFeedback.Count <= maxEntries)
        {
            return;
        }

        var keep = _joinCardinalityFeedback
            .OrderByDescending(entry => entry.Value)
            .Take(maxEntries)
            .ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.OrdinalIgnoreCase);

        _joinCardinalityFeedback.Clear();
        foreach (var entry in keep)
        {
            _joinCardinalityFeedback[entry.Key] = entry.Value;
        }
    }

    private string BuildJoinFeedbackKey(string leftTable, string leftColumn, string rightTable, string rightColumn)
    {
        string normalizedLeft = NormalizeTableIdentifier(leftTable);
        string normalizedRight = NormalizeTableIdentifier(rightTable);
        string leftRef = $"{normalizedLeft}.{leftColumn}";
        string rightRef = $"{normalizedRight}.{rightColumn}";

        if (string.Compare(leftRef, rightRef, StringComparison.OrdinalIgnoreCase) <= 0)
        {
            return $"{leftRef}={rightRef}";
        }

        return $"{rightRef}={leftRef}";
    }

    private string NormalizeTableIdentifier(string tableOrAlias)
    {
        if (_model.TableService == null)
        {
            return tableOrAlias;
        }

        try
        {
            return _model.TableService.GetTableDetailByAliasOrName(tableOrAlias).TableName;
        }
        catch
        {
            return tableOrAlias;
        }
    }

    private int EstimateJoinKeyDistinct(
        string tableOrAlias,
        string joinColumn,
        Dictionary<string, ExpressionNode> tableFilters,
        int estimatedRows)
    {
        if (_model.TableService == null)
        {
            return Math.Max(1, estimatedRows / 2);
        }

        var detail = _model.TableService.GetTableDetailByAliasOrName(tableOrAlias);
        var rows = detail.TableContentValues;
        if (rows == null || rows.Count == 0)
        {
            return 1;
        }

        int sampleSize = Math.Min(2048, rows.Count);
        var distinct = new HashSet<string>(StringComparer.Ordinal);

        for (int i = 0; i < sampleSize; i++)
        {
            Row row = rows[i].ToRow();
            object? value = row.ContainsKey(joinColumn) ? row[joinColumn] : null;
            string typePart = value?.GetType().Name ?? "<null>";
            string valuePart = value?.ToString() ?? "<null>";
            distinct.Add($"{typePart}:{valuePart}");
        }

        if (distinct.Count == 0)
        {
            return 1;
        }

        double scale = (double)rows.Count / sampleSize;
        int estimatedDistinct = (int)Math.Ceiling(distinct.Count * Math.Max(1d, scale * 0.8d));

        if (tableFilters.TryGetValue(tableOrAlias, out var filter))
        {
            double selectivity = EstimatePredicateSelectivity(filter);
            estimatedDistinct = Math.Max(1, (int)Math.Ceiling(estimatedDistinct * selectivity));
        }

        return Math.Min(Math.Max(1, estimatedDistinct), Math.Max(1, estimatedRows));
    }

    private int EstimateJoinedStreamRows(HashSet<string> joinedTables, Dictionary<string, ExpressionNode> tableFilters)
    {
        if (_model.TableService == null)
        {
            return _model.FromTable?.TableContentValues?.Count ?? 0;
        }

        int total = 0;
        foreach (string joined in joinedTables)
        {
            total += ResolveJoinTableRowCount(joined, tableFilters);
        }

        return total;
    }

    private IQueryOperator BuildVolcanoInnerJoinOperator(
        IQueryOperator left,
        IQueryOperator right,
        string leftJoinColumn,
        string rightJoinColumn,
        string leftTableName,
        string rightTableName,
        JoinEdgePhysicalPlan joinPlan)
    {
        IQueryOperator probeInput = left;
        IQueryOperator buildInput = right;
        string probeJoinColumn = leftJoinColumn;
        string buildJoinColumn = rightJoinColumn;
        string probeTableName = leftTableName;
        string buildTableName = rightTableName;

        if (joinPlan.BuildSide == JoinPlanSide.Left)
        {
            probeInput = right;
            buildInput = left;
            probeJoinColumn = rightJoinColumn;
            buildJoinColumn = leftJoinColumn;
            probeTableName = rightTableName;
            buildTableName = leftTableName;
        }

        if (joinPlan.Algorithm == JoinPhysicalAlgorithm.NestedLoop)
        {
            return new NestedLoopJoinOperator(probeInput, buildInput, probeJoinColumn, buildJoinColumn, probeTableName, buildTableName);
        }

        return new InnerJoinOperator(probeInput, buildInput, probeJoinColumn, buildJoinColumn, probeTableName, buildTableName);
    }

    private int PickNextVolcanoJoinConditionIndex(
        List<DataVo.Core.Models.Statement.JoinModel.JoinCondition> remaining,
        HashSet<string> joinedTables,
        Dictionary<string, ExpressionNode> tableFilters,
        out bool leftSideAlreadyJoined)
    {
        leftSideAlreadyJoined = false;
        int selectedIndex = -1;
        int bestEstimatedOutputRows = int.MaxValue;
        int bestCandidateRowCount = int.MaxValue;
        int estimatedJoinedRows = EstimateJoinedStreamRows(joinedTables, tableFilters);

        for (int i = 0; i < remaining.Count; i++)
        {
            bool leftIn = joinedTables.Contains(remaining[i].LeftColumn.TableName);
            bool rightIn = joinedTables.Contains(remaining[i].RightColumn.TableName);

            if (!(leftIn ^ rightIn))
            {
                continue;
            }

            string candidateTable = leftIn
                ? remaining[i].RightColumn.TableName
                : remaining[i].LeftColumn.TableName;

            int candidateRowCount = ResolveJoinTableRowCount(candidateTable, tableFilters);

            string existingTable = leftIn ? remaining[i].LeftColumn.TableName : remaining[i].RightColumn.TableName;
            string existingColumn = leftIn ? remaining[i].LeftColumn.ColumnName : remaining[i].RightColumn.ColumnName;
            string newTable = leftIn ? remaining[i].RightColumn.TableName : remaining[i].LeftColumn.TableName;
            string newColumn = leftIn ? remaining[i].RightColumn.ColumnName : remaining[i].LeftColumn.ColumnName;

            int edgeOutputRows = EstimateJoinEdgeOutputRows(
                estimatedJoinedRows,
                candidateRowCount,
                existingTable,
                existingColumn,
                newTable,
                newColumn,
                tableFilters,
                out _,
                out _,
                out _);

            if (edgeOutputRows < bestEstimatedOutputRows
                || (edgeOutputRows == bestEstimatedOutputRows && candidateRowCount < bestCandidateRowCount))
            {
                bestEstimatedOutputRows = edgeOutputRows;
                bestCandidateRowCount = candidateRowCount;
                selectedIndex = i;
                leftSideAlreadyJoined = leftIn;
            }
        }

        return selectedIndex;
    }

    private int ResolveJoinTableRowCount(string tableOrAlias, Dictionary<string, ExpressionNode> tableFilters)
    {
        if (_model.TableService == null)
        {
            return int.MaxValue;
        }

        var detail = _model.TableService.GetTableDetailByAliasOrName(tableOrAlias);
        int baseRowCount = detail?.TableContentValues?.Count ?? int.MaxValue;
        if (baseRowCount == int.MaxValue)
        {
            return baseRowCount;
        }

        if (!tableFilters.TryGetValue(tableOrAlias, out var localFilter))
        {
            return baseRowCount;
        }

        double selectivity = EstimatePredicateSelectivity(localFilter);
        return Math.Max(1, (int)Math.Ceiling(baseRowCount * selectivity));
    }

    private static Dictionary<string, object?> ToExecutionValues(Row row)
    {
        var values = new Dictionary<string, object?>();
        foreach (string key in row.Keys)
        {
            values[key] = row[key];
        }

        return values;
    }

    private static JoinedRow ToJoinedRowFromJoinExecution(ExecutionRow row, IReadOnlyList<string> tableNames)
    {
        Dictionary<string, Dictionary<string, object?>> buckets = [];
        foreach (string tableName in tableNames)
        {
            buckets[tableName] = [];
        }

        foreach (var entry in row.Values)
        {
            bool matched = false;
            foreach (string tableName in tableNames)
            {
                string prefix = $"{tableName}.";
                if (entry.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                {
                    buckets[tableName][entry.Key[prefix.Length..]] = entry.Value;
                    matched = true;
                    break;
                }
            }

            if (!matched && tableNames.Count == 1)
            {
                buckets[tableNames[0]][entry.Key] = entry.Value;
            }
        }

        var joined = new JoinedRow();
        foreach (string tableName in tableNames)
        {
            joined.Add(tableName, new Row(buckets[tableName]));
        }

        return joined;
    }

    private static JoinedRow ToJoinedRowFromJoinExecution(TypedExecutionRow row, IReadOnlyList<string> tableNames)
    {
        return ToJoinedRowFromJoinExecution(ExecutionRow.FromTyped(row), tableNames);
    }

    private static JoinedRow BuildSingleTableJoinedRow(string tableName, IReadOnlyDictionary<string, object?> values)
    {
        var rowValues = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in values)
        {
            rowValues[entry.Key] = entry.Value;
        }

        return new JoinedRow(tableName, new Row(rowValues));
    }

    private bool CanPushDownLimitToVolcano(bool orderPushedDown)
    {
        if (!_model.LimitTake.HasValue || _model.LimitTake.Value <= 0)
        {
            return false;
        }

        if (_model.IsDistinct && !_volcanoDistinctPushedDown)
        {
            return false;
        }

        if (_model.GroupByStatement.ContainsGroupBy() && !_volcanoGroupByPushedDown)
        {
            return false;
        }

        if (_model.GetHavingExpression() != null)
        {
            return false;
        }

        var orderBy = _model.GetOrderByExpression();
        if (orderBy == null || orderBy.Columns.Count == 0)
        {
            return true;
        }

        return orderPushedDown;
    }

    private int? GetVolcanoRunnerMaxRows()
        => _volcanoLimitPushedDown && _model.LimitTake.HasValue
            ? _model.LimitTake.Value
            : null;

    private bool CanPushDownOffsetToVolcano(bool orderPushedDown)
    {
        if (!_model.LimitSkip.HasValue || _model.LimitSkip.Value <= 0)
        {
            return false;
        }

        if (_model.IsDistinct && !_volcanoDistinctPushedDown)
        {
            return false;
        }

        if (_model.GroupByStatement.ContainsGroupBy() && !_volcanoGroupByPushedDown)
        {
            return false;
        }

        if (_model.GetHavingExpression() != null)
        {
            return false;
        }

        var orderBy = _model.GetOrderByExpression();
        if (orderBy == null || orderBy.Columns.Count == 0)
        {
            return true;
        }

        return orderPushedDown;
    }

    private bool TryBuildNoJoinOrderPushdown(out List<(string Key, bool IsAscending)> orderKeys)
    {
        if (_volcanoAggregatePushedDown)
        {
            return TryBuildNoJoinAggregateOrderPushdown(out orderKeys);
        }

        orderKeys = [];

        var orderBy = _model.GetOrderByExpression();
        if (orderBy == null || orderBy.Columns.Count == 0)
        {
            return false;
        }

        if (_model.JoinStatement.ContainsJoin())
        {
            return false;
        }

        foreach (var orderColumn in orderBy.Columns)
        {
            if (_model.GetSelectColumnByAlias(orderColumn.Column.Name)?.Expression != null)
            {
                return false;
            }

            string token = orderColumn.Column.Name;
            if (token.Contains('.'))
            {
                string[] parts = token.Split('.');
                if (parts.Length != 2)
                {
                    return false;
                }

                if (!parts[0].Equals(_model.FromTable.TableName, StringComparison.OrdinalIgnoreCase)
                    && (_model.FromTable.TableAlias == null || !parts[0].Equals(_model.FromTable.TableAlias, StringComparison.OrdinalIgnoreCase)))
                {
                    return false;
                }

                orderKeys.Add((parts[1], orderColumn.IsAscending));
                continue;
            }

            orderKeys.Add((token, orderColumn.IsAscending));
        }

        return orderKeys.Count > 0;
    }

    private bool TryBuildNoJoinAggregateOrderPushdown(out List<(string Key, bool IsAscending)> orderKeys)
    {
        orderKeys = [];

        if (_model.JoinStatement.ContainsJoin())
        {
            return false;
        }

        var orderBy = _model.GetOrderByExpression();
        if (orderBy == null || orderBy.Columns.Count == 0)
        {
            return false;
        }

        string fromTable = _model.FromTable.TableName;
        string? fromAlias = _model.FromTable.TableAlias;
        var groupBySet = new HashSet<string>(_volcanoAggregateGroupKeyColumns, StringComparer.OrdinalIgnoreCase);
        Dictionary<string, string> aggregateAliasMap = BuildAggregateAliasToOutputKeyMap();

        foreach (var orderColumn in orderBy.Columns)
        {
            string token = orderColumn.Column.Name;

            if (token.Contains('.'))
            {
                string[] parts = token.Split('.');
                if (parts.Length != 2)
                {
                    return false;
                }

                if (!parts[0].Equals(fromTable, StringComparison.OrdinalIgnoreCase)
                    && (fromAlias == null || !parts[0].Equals(fromAlias, StringComparison.OrdinalIgnoreCase)))
                {
                    return false;
                }

                if (!groupBySet.Contains(parts[1]))
                {
                    return false;
                }

                orderKeys.Add((parts[1], orderColumn.IsAscending));
                continue;
            }

            if (aggregateAliasMap.TryGetValue(token, out string? aggregateOutputKey)
                && !string.IsNullOrWhiteSpace(aggregateOutputKey))
            {
                orderKeys.Add((aggregateOutputKey, orderColumn.IsAscending));
                continue;
            }

            if (_volcanoAggregateOutputColumns.Contains(token))
            {
                orderKeys.Add((token, orderColumn.IsAscending));
                continue;
            }

            if (groupBySet.Contains(token))
            {
                orderKeys.Add((token, orderColumn.IsAscending));
                continue;
            }

            if (_model.GetSelectColumnByAlias(token)?.Expression is AggregateExpressionNode aggregateExpression)
            {
                string outputKey = AggregateExpressionFormatter.BuildHeader(aggregateExpression);
                if (_volcanoAggregateOutputColumns.Contains(outputKey))
                {
                    orderKeys.Add((outputKey, orderColumn.IsAscending));
                    continue;
                }
            }

            return false;
        }

        return orderKeys.Count > 0;
    }

    private Dictionary<string, string> BuildAggregateAliasToOutputKeyMap()
    {
        Dictionary<string, string> aliasMap = new(StringComparer.OrdinalIgnoreCase);

        foreach (var aggregateColumn in _model.GetAggregateColumns())
        {
            if (aggregateColumn.Expression is not AggregateExpressionNode aggregateExpression)
            {
                continue;
            }

            if (string.IsNullOrWhiteSpace(aggregateColumn.Alias))
            {
                continue;
            }

            aliasMap[aggregateColumn.Alias] = AggregateExpressionFormatter.BuildHeader(aggregateExpression);
        }

        return aliasMap;
    }

    private bool TryBuildNoJoinProjectionPushdown(out HashSet<string> projectionColumns)
    {
        projectionColumns = [];

        if (_model.JoinStatement.ContainsJoin())
        {
            return false;
        }

        if (_model.GetComputedExpressionColumns().Count > 0
            || _model.GetWindowFunctionColumns().Count > 0
            || _model.GetAggregateColumns().Count > 0)
        {
            return false;
        }

        foreach (string selected in _model.GetSelectedColumns())
        {
            string baseName = selected.Contains(" AS ", StringComparison.OrdinalIgnoreCase)
                ? selected.Split(" AS ", StringSplitOptions.None)[0]
                : selected;

            string[] parts = baseName.Split('.');
            if (parts.Length != 2)
            {
                return false;
            }

            projectionColumns.Add(parts[1]);
        }

        // Include ORDER BY keys so sorting can still run correctly after projection.
        if (TryBuildNoJoinOrderPushdown(out var orderKeys))
        {
            foreach (var key in orderKeys)
            {
                projectionColumns.Add(key.Key);
            }
        }

        return projectionColumns.Count > 0;
    }

    private bool TryBuildNoJoinGroupByPushdown(out List<string> groupByColumns)
    {
        groupByColumns = [];

        if (_model.JoinStatement.ContainsJoin())
        {
            return false;
        }

        if (!_model.GroupByStatement.ContainsGroupBy())
        {
            return false;
        }

        if (_model.AggregateStatement.ContainsAggregate())
        {
            return false;
        }

        if (_model.GetHavingExpression() != null)
        {
            return false;
        }

        if (_model.GetComputedExpressionColumns().Count > 0
            || _model.GetWindowFunctionColumns().Count > 0
            || _model.GetAggregateColumns().Count > 0)
        {
            return false;
        }

        string fromTable = _model.FromTable.TableName;
        foreach (var groupedColumn in _model.GroupByStatement.Model.Columns)
        {
            if (!groupedColumn.TableName.Equals(fromTable, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            groupByColumns.Add(groupedColumn.ColumnName);
        }

        if (groupByColumns.Count == 0)
        {
            return false;
        }

        var groupByKeySet = new HashSet<string>(groupByColumns, StringComparer.OrdinalIgnoreCase);
        foreach (string selected in _model.GetSelectedColumns())
        {
            string baseName = selected.Contains(" AS ", StringComparison.OrdinalIgnoreCase)
                ? selected.Split(" AS ", StringSplitOptions.None)[0]
                : selected;

            string[] parts = baseName.Split('.');
            if (parts.Length != 2)
            {
                return false;
            }

            if (!parts[0].Equals(fromTable, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (!groupByKeySet.Contains(parts[1]))
            {
                return false;
            }
        }

        if (TryBuildNoJoinOrderPushdown(out var orderKeys)
            && orderKeys.Any(key => !groupByKeySet.Contains(key.Key)))
        {
            return false;
        }

        return true;
    }

    private bool TryBuildNoJoinAggregatePushdown(
        out List<string> groupByColumns,
        out List<HashAggregateOperator.AggregateSpec> aggregateSpecs)
    {
        groupByColumns = [];
        aggregateSpecs = [];

        if (_model.JoinStatement.ContainsJoin())
        {
            return false;
        }

        if (_model.GetComputedExpressionColumns().Count > 0
            || _model.GetWindowFunctionColumns().Count > 0)
        {
            return false;
        }

        var aggregateColumns = _model.GetAggregateColumns();
        if (aggregateColumns.Count == 0)
        {
            return false;
        }

        string fromTable = _model.FromTable.TableName;
        foreach (var groupedColumn in _model.GroupByStatement.Model.Columns)
        {
            if (!groupedColumn.TableName.Equals(fromTable, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            groupByColumns.Add(groupedColumn.ColumnName);
        }

        var groupByKeySet = new HashSet<string>(groupByColumns, StringComparer.OrdinalIgnoreCase);
        foreach (string selected in _model.GetSelectedColumns())
        {
            string baseName = selected.Contains(" AS ", StringComparison.OrdinalIgnoreCase)
                ? selected.Split(" AS ", StringSplitOptions.None)[0]
                : selected;

            string[] parts = baseName.Split('.');
            if (parts.Length != 2)
            {
                return false;
            }

            if (!parts[0].Equals(fromTable, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (!groupByKeySet.Contains(parts[1]))
            {
                return false;
            }
        }

        var seenOutputs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var aggregateColumn in aggregateColumns)
        {
            if (aggregateColumn.Expression is not AggregateExpressionNode agg)
            {
                return false;
            }

            if (!TryBuildAggregateSpec(agg, fromTable, out var spec))
            {
                return false;
            }

            if (!seenOutputs.Add(spec.OutputColumn))
            {
                return false;
            }

            aggregateSpecs.Add(spec);
        }

        return aggregateSpecs.Count > 0;
    }

    private bool TryBuildAggregateSpec(AggregateExpressionNode agg, string fromTable, out HashAggregateOperator.AggregateSpec spec)
    {
        spec = null!;

        HashAggregateOperator.AggregateFunction function = agg.FunctionName.ToUpperInvariant() switch
        {
            "COUNT" => HashAggregateOperator.AggregateFunction.Count,
            "SUM" => HashAggregateOperator.AggregateFunction.Sum,
            "AVG" => HashAggregateOperator.AggregateFunction.Avg,
            "MIN" => HashAggregateOperator.AggregateFunction.Min,
            "MAX" => HashAggregateOperator.AggregateFunction.Max,
            _ => (HashAggregateOperator.AggregateFunction)(-1)
        };

        if ((int)function < 0)
        {
            return false;
        }

        string outputKey = AggregateExpressionFormatter.BuildHeader(agg);

        if (agg.IsStar)
        {
            if (function != HashAggregateOperator.AggregateFunction.Count)
            {
                return false;
            }

            spec = new HashAggregateOperator.AggregateSpec(outputKey, function, (Func<ExecutionRow, object?>?)null);
            return true;
        }

        if (agg.Argument == null)
        {
            return false;
        }

        if (!IsAggregateArgumentPushdownSupported(agg.Argument, fromTable))
        {
            return false;
        }

        ExpressionNode argument = agg.Argument;
        spec = new HashAggregateOperator.AggregateSpec(outputKey, function, (TypedExecutionRow row) =>
            EvaluateAggregateArgumentForPushdown(argument, row, fromTable));
        return true;
    }

    private bool IsAggregateArgumentPushdownSupported(ExpressionNode argument, string fromTable)
    {
        return argument switch
        {
            LiteralNode or NullLiteralNode => true,
            ResolvedColumnRefNode resolved => resolved.TableName.Equals(fromTable, StringComparison.OrdinalIgnoreCase),
            ColumnRefNode colRef => IsColumnReferenceOnTable(colRef, fromTable),
            BinaryExpressionNode binary => IsAggregateArgumentPushdownSupported(binary.Left, fromTable)
                && IsAggregateArgumentPushdownSupported(binary.Right, fromTable),
            ScalarFunctionExpressionNode scalar => scalar.Arguments.All(arg => IsAggregateArgumentPushdownSupported(arg, fromTable)),
            _ => false,
        };
    }

    private bool IsColumnReferenceOnTable(ColumnRefNode colRef, string fromTable)
    {
        string reference = string.IsNullOrWhiteSpace(colRef.TableOrAlias)
            ? colRef.Column
            : $"{colRef.TableOrAlias}.{colRef.Column}";

        var parsed = _model.TableService!.ParseAndFindTableNameByColumn(reference);
        return parsed.Item1.Equals(fromTable, StringComparison.OrdinalIgnoreCase);
    }

    private object? EvaluateAggregateArgumentForPushdown(ExpressionNode argument, TypedExecutionRow row, string fromTable)
    {
        var joinedRow = BuildSingleTableJoinedRow(fromTable, row.Values);

        return ExpressionEvaluator.Evaluate(
            argument,
            joinedRow,
            (colRef, r) =>
            {
                string reference = string.IsNullOrWhiteSpace(colRef.TableOrAlias)
                    ? colRef.Column
                    : $"{colRef.TableOrAlias}.{colRef.Column}";
                return ResolveColumnValue(r, reference);
            },
            (_, _) => throw new EvaluationException("Nested aggregate expression is not supported in aggregate pushdown argument."));
    }

    private bool TryBuildNoJoinDistinctPushdown(out List<string> distinctColumns)
    {
        distinctColumns = [];

        if (!_model.IsDistinct)
        {
            return false;
        }

        if (_model.JoinStatement.ContainsJoin())
        {
            return false;
        }

        if (_model.GroupByStatement.ContainsGroupBy() || _model.GetHavingExpression() != null)
        {
            return false;
        }

        if (_model.GetComputedExpressionColumns().Count > 0
            || _model.GetWindowFunctionColumns().Count > 0
            || _model.GetAggregateColumns().Count > 0)
        {
            return false;
        }

        foreach (string selected in _model.GetSelectedColumns())
        {
            string baseName = selected.Contains(" AS ", StringComparison.OrdinalIgnoreCase)
                ? selected.Split(" AS ", StringSplitOptions.None)[0]
                : selected;

            string[] parts = baseName.Split('.');
            if (parts.Length != 2)
            {
                return false;
            }

            distinctColumns.Add(parts[1]);
        }

        var orderBy = _model.GetOrderByExpression();
        if (orderBy != null && orderBy.Columns.Count > 0)
        {
            if (!TryBuildNoJoinOrderPushdown(out var orderKeys))
            {
                return false;
            }

            if (!HasEquivalentKeySet(distinctColumns, orderKeys.Select(k => k.Key)))
            {
                return false;
            }
        }

        return distinctColumns.Count > 0;
    }

    private static string BuildDistinctKey(ExecutionRow row, IReadOnlyList<string> columns)
    {
        if (columns.Count == 0)
        {
            return string.Empty;
        }

        List<string> parts = [];
        foreach (string column in columns)
        {
            object? value = row.Values.TryGetValue(column, out var found) ? found : null;
            string typePart = value?.GetType().Name ?? "<null>";
            string valuePart = value?.ToString() ?? "<null>";
            parts.Add($"{column}:{typePart}:{valuePart}");
        }

        return string.Join("|", parts);
    }

    private static string BuildDistinctKey(TypedExecutionRow row, IReadOnlyList<string> columns)
    {
        if (columns.Count == 0)
        {
            return string.Empty;
        }

        List<string> parts = [];
        foreach (string column in columns)
        {
            object? value = row.Values.TryGetValue(column, out var found) ? found : null;
            string typePart = value?.GetType().Name ?? "<null>";
            string valuePart = value?.ToString() ?? "<null>";
            parts.Add($"{column}:{typePart}:{valuePart}");
        }

        return string.Join("|", parts);
    }

    private bool TryBuildJoinOrderPushdown(out List<(string Key, bool IsAscending)> orderKeys)
    {
        orderKeys = [];

        var orderBy = _model.GetOrderByExpression();
        if (orderBy == null || orderBy.Columns.Count == 0)
        {
            return false;
        }

        if (!_model.JoinStatement.ContainsJoin())
        {
            return false;
        }

        if (_model.TableService == null)
        {
            return false;
        }

        foreach (var orderColumn in orderBy.Columns)
        {
            if (_model.GetSelectColumnByAlias(orderColumn.Column.Name)?.Expression != null)
            {
                return false;
            }

            try
            {
                string token = orderColumn.Column.Name;
                if (!token.Contains('.'))
                {
                    var resolved = _model.TableService.ParseAndFindTableNameByColumn(token);
                    orderKeys.Add(($"{resolved.Item1}.{resolved.Item2}", orderColumn.IsAscending));
                    continue;
                }

                string[] parts = token.Split('.');
                if (parts.Length != 2)
                {
                    return false;
                }

                var table = _model.TableService.GetTableDetailByAliasOrName(parts[0]);
                orderKeys.Add(($"{table.TableName}.{parts[1]}", orderColumn.IsAscending));
            }
            catch
            {
                return false;
            }
        }

        return orderKeys.Count > 0;
    }

    private bool TryBuildJoinDistinctPushdown(out List<string> distinctColumns)
    {
        distinctColumns = [];

        if (!_model.IsDistinct)
        {
            return false;
        }

        if (!_model.JoinStatement.ContainsJoin())
        {
            return false;
        }

        if (_model.GroupByStatement.ContainsGroupBy() || _model.GetHavingExpression() != null)
        {
            return false;
        }

        if (_model.GetComputedExpressionColumns().Count > 0
            || _model.GetWindowFunctionColumns().Count > 0
            || _model.GetAggregateColumns().Count > 0)
        {
            return false;
        }

        if (_model.TableService == null)
        {
            return false;
        }

        foreach (string selected in _model.GetSelectedColumns())
        {
            string baseName = selected.Contains(" AS ", StringComparison.OrdinalIgnoreCase)
                ? selected.Split(" AS ", StringSplitOptions.None)[0]
                : selected;

            string[] parts = baseName.Split('.');
            if (parts.Length != 2)
            {
                return false;
            }

            try
            {
                var table = _model.TableService.GetTableDetailByAliasOrName(parts[0]);
                distinctColumns.Add($"{table.TableName}.{parts[1]}");
            }
            catch
            {
                return false;
            }
        }

        var orderBy = _model.GetOrderByExpression();
        if (orderBy != null && orderBy.Columns.Count > 0)
        {
            if (!TryBuildJoinOrderPushdown(out var orderKeys))
            {
                return false;
            }

            if (!HasEquivalentKeySet(distinctColumns, orderKeys.Select(k => k.Key)))
            {
                return false;
            }
        }

        return distinctColumns.Count > 0;
    }

    private bool TryBuildJoinProjectionPushdown(out HashSet<string> projectionColumns)
    {
        projectionColumns = [];

        if (!_model.JoinStatement.ContainsJoin())
        {
            return false;
        }

        if (_model.GetComputedExpressionColumns().Count > 0
            || _model.GetWindowFunctionColumns().Count > 0
            || _model.GetAggregateColumns().Count > 0)
        {
            return false;
        }

        if (_model.TableService == null)
        {
            return false;
        }

        foreach (string selected in _model.GetSelectedColumns())
        {
            string baseName = selected.Contains(" AS ", StringComparison.OrdinalIgnoreCase)
                ? selected.Split(" AS ", StringSplitOptions.None)[0]
                : selected;

            string[] parts = baseName.Split('.');
            if (parts.Length != 2)
            {
                return false;
            }

            try
            {
                var table = _model.TableService.GetTableDetailByAliasOrName(parts[0]);
                projectionColumns.Add($"{table.TableName}.{parts[1]}");
            }
            catch
            {
                return false;
            }
        }

        // Keep ORDER BY keys available for join sort pushdown.
        if (TryBuildJoinOrderPushdown(out var orderKeys))
        {
            foreach (var key in orderKeys)
            {
                projectionColumns.Add(key.Key);
            }
        }

        return projectionColumns.Count > 0;
    }

    private static bool HasEquivalentKeySet(IEnumerable<string> left, IEnumerable<string> right)
    {
        var leftSet = new HashSet<string>(left, StringComparer.OrdinalIgnoreCase);
        var rightSet = new HashSet<string>(right, StringComparer.OrdinalIgnoreCase);
        return leftSet.SetEquals(rightSet);
    }

    private static object? ResolveNoJoinOrderValue(ExecutionRow row, string orderByColumn)
    {
        return row.Values.TryGetValue(orderByColumn, out var value)
            ? value
            : null;
    }

    private static object? ResolveNoJoinOrderValue(TypedExecutionRow row, string orderByColumn)
    {
        return row.Values.TryGetValue(orderByColumn, out var value)
            ? value
            : null;
    }

    private static object? ResolveJoinOrderValue(ExecutionRow row, string orderByKey)
    {
        return row.Values.TryGetValue(orderByKey, out var value)
            ? value
            : null;
    }

    private static object? ResolveJoinOrderValue(TypedExecutionRow row, string orderByKey)
    {
        return row.Values.TryGetValue(orderByKey, out var value)
            ? value
            : null;
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

    // Window/HAVING/value-resolution methods moved to Select.WindowHaving.cs partial.
}
