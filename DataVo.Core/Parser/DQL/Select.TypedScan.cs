using System.Globalization;
using DataVo.Core.Constants;
using DataVo.Core.Enums;
using DataVo.Core.Execution.Volcano;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Parser.Statements.Mechanism;
using DataVo.Core.Parser.Types;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// The typed single-table path: a SELECT over one stored table whose WHERE is a conjunction of
/// <c>column op literal</c> comparisons is answered by streaming rows straight from storage. Only the
/// columns the query reads are decoded, a range on a clustered integer key becomes a key-range scan,
/// aggregates accumulate typed values, and ORDER BY ... LIMIT keeps only the top rows. Only the rows
/// that reach the result are turned into dictionaries. Anything else takes the general path, which
/// stays the reference for semantics: comparisons here use the same comparers it uses.
/// </summary>
internal partial class Select
{
    private enum TypedPredicateKind { Numeric, Integer, Text, ExactText, General, IsNull, IsNotNull, Like, Or }

    private sealed class TypedPredicate(int ordinal, string op, object? literal, TypedPredicateKind kind, double number, long integer = 0)
    {
        /// <summary>The column tested; -1 for an <see cref="TypedPredicateKind.Or"/>, whose branches name their own.</summary>
        public int Ordinal { get; } = ordinal;
        public string Operator { get; } = op;
        public object? Literal { get; } = literal;
        public TypedPredicateKind Kind { get; } = kind;
        public double Number { get; } = number;

        /// <summary>The exact value of an <see cref="TypedPredicateKind.Integer"/> predicate; a double cannot hold every BIGINT.</summary>
        public long Integer { get; } = integer;

        /// <summary>The compiled pattern of a <see cref="TypedPredicateKind.Like"/> predicate (<c>LIKE</c> or <c>NOT LIKE</c>).</summary>
        public SqlLikePattern? Pattern { get; private init; }

        /// <summary>
        /// The UTF-8 bytes of the literal of a <see cref="TypedPredicateKind.Text"/> or
        /// <see cref="TypedPredicateKind.ExactText"/> predicate, to compare stored text without decoding it;
        /// null for other kinds and for a literal that is not valid Unicode.
        /// </summary>
        public byte[]? LiteralUtf8 { get; } = kind is TypedPredicateKind.Text or TypedPredicateKind.ExactText && literal is string text
            ? Utf8Text.TryEncode(text)
            : null;

        /// <summary>The branches of an <see cref="TypedPredicateKind.Or"/>: it holds when every predicate of some branch holds.</summary>
        public TypedPredicate[][]? Branches { get; private init; }

        public static TypedPredicate Like(int ordinal, string op, object literal, string pattern) =>
            new(ordinal, op, literal, TypedPredicateKind.Like, 0) { Pattern = new SqlLikePattern(pattern) };

        public static TypedPredicate Or(TypedPredicate[][] branches) =>
            new(-1, Operators.OR, null, TypedPredicateKind.Or, 0) { Branches = branches };
    }

    private sealed class TypedAggregate(HashAggregateOperator.AggregateSpec spec, int ordinal)
    {
        public HashAggregateOperator.AggregateSpec Spec { get; } = spec;

        /// <summary>Column read by the aggregate, or -1 for <c>COUNT(*)</c>.</summary>
        public int Ordinal { get; } = ordinal;
    }

    private sealed class TypedScanState
    {
        public required IReadOnlyList<Column> Columns { get; init; }
        public required List<TypedPredicate> Predicates { get; init; }
        public required bool[] FilterMask { get; init; }
        public required bool[] RestMask { get; init; }

        /// <summary>Every column read from a typed row (<see cref="FilterMask"/> and <see cref="RestMask"/>): the in-memory engine decodes no others.</summary>
        public bool[] TypedReadMask => _typedReadMask ??= [.. FilterMask.Select((filter, i) => filter || RestMask[i])];

        private bool[]? _typedReadMask;

        /// <summary>The text filter columns compared as stored bytes when rows arrive serialized; null when there is none.</summary>
        public RawTextColumns? RawText { get; init; }

        /// <summary>Columns the rest of the query reads from a matched row; the others are never decoded or copied.</summary>
        public bool[]? OutputMask { get; init; }

        /// <summary>When set, matched rows are written as final output rows: field name and column per output position.</summary>
        public (string Field, int Ordinal)[]? DirectOutput { get; init; }

        /// <summary>Stop once this many rows matched (LIMIT without ORDER BY); -1 for no limit.</summary>
        public int StopAfter { get; init; } = -1;
        public required CellValue[] Cells { get; init; }
        public required int ClusteredOrdinal { get; init; }
        public required bool ClusteredIsGuid { get; init; }
        public required string TableName { get; init; }

        // Plain rows.
        public List<JoinedRow>? Rows { get; init; }
        public List<(int Ordinal, bool Ascending)>? OrderKeys { get; init; }
        public int TopLimit { get; init; } = -1;
        public List<(CellValue[] Keys, long Sequence, object Row)>? Top { get; init; }

        /// <summary>The sort-key cells of the row being offered to <see cref="Top"/>; copied only when the row is kept.</summary>
        public CellValue[]? TopCandidate { get; init; }
        public List<Dictionary<string, object?>>? OutputRows { get; init; }
        public long Sequence;

        /// <summary>Rows the scan visited (read from storage), matched or not; reported by EXPLAIN.</summary>
        public long Visited;

        // Aggregates.
        public List<TypedAggregate>? Aggregates { get; init; }
        public int[]? GroupOrdinals { get; init; }
        public List<string>? GroupColumnNames { get; init; }
        public GroupLookup? GroupKeys { get; init; }
        public TypedGroupStates? GroupStates { get; init; }

        /// <summary>The one text GROUP BY column, located in serialized rows instead of decoded; null when there is none.</summary>
        public GroupTextColumn? GroupText { get; init; }
        public long Matched;
    }

    /// <summary>Returns the matched rows, or null when the query needs the general path.</summary>
    private ListedTable? TryEvaluateTypedSingleTable()
    {
        if (_model.JoinStatement.ContainsJoin() || _model.FromTable is null)
        {
            return null; // a join has its own fast path (TryEvaluateTypedJoin)
        }

        if (SharedTypedRefusal() is { } refusal)
        {
            return DeclineTypedScan(refusal);
        }

        string tableName = _model.FromTable.TableName;
        string databaseName = _model.Database!;
        DataVoEngine engine = DataVoEngine.Current();
        IReadOnlyList<Column> columns = engine.Catalog.GetTableColumns(tableName, databaseName);
        if (columns.Count == 0)
        {
            return DeclineTypedScan("the table has no catalog columns");
        }

        var predicates = new List<TypedPredicate>();
        ExpressionNode? where = _model.WhereStatement.IsEvaluatable() ? _model.WhereStatement.GetExpression() : null;
        if (where is not null && !TryCollectTypedPredicates(where, tableName, columns, predicates))
        {
            return DeclineTypedScan(_predicateRefusal ?? "WHERE shape is not supported by the typed scan");
        }

        // An equality on the primary key or an indexed column reads only the rows the index names. When the
        // index key cannot be built exactly (a composite index, a literal of another type), the general
        // path decides, as before.
        (string IndexName, string ColumnName, string Key)? indexLookup = null;
        List<string> primaryKeys = engine.Catalog.GetTablePrimaryKeys(tableName, databaseName);
        Dictionary<string, string> indexedColumns = _model.FromTable.IndexedColumns ?? [];
        foreach (TypedPredicate predicate in predicates)
        {
            if (predicate.Operator != Operators.EQUALS)
            {
                continue;
            }

            string column = columns[predicate.Ordinal].Name;
            bool isKey = primaryKeys.Count == 1 && primaryKeys[0].Equals(column, StringComparison.OrdinalIgnoreCase);
            string? indexName = isKey ? $"_PK_{tableName}" : indexedColumns.TryGetValue(column, out string? file) ? file : null;
            if (indexName is null)
            {
                if (primaryKeys.Contains(column, StringComparer.OrdinalIgnoreCase))
                {
                    return DeclineTypedScan($"equality on {column}, one column of a composite primary key"); // the general path's key lookup
                }

                continue;
            }

            if (!SqlIndexLookupKey.TryBuildAuthoritative(_model.FromTable, column, indexName, predicate.Literal, out string key, isBoundValue: predicate.Kind == TypedPredicateKind.ExactText))
            {
                return DeclineTypedScan($"the key of index {indexName} cannot be built exactly from the literal compared with {column}");
            }

            if (indexLookup is null || isKey)
            {
                indexLookup = (indexName, column, key);
            }

            if (isKey)
            {
                break;
            }
        }

        ClusteredKeyLayout? clustered = engine.StorageContext.ResolveClusteredKey(databaseName, tableName);
        int clusteredOrdinal = clustered?.Ordinal ?? -1;
        bool clusteredIsGuid = clustered is not null
            && string.Equals(columns[clusteredOrdinal].Type, "GUID", StringComparison.OrdinalIgnoreCase);
        (RowKey? lower, RowKey? upper) = clustered is not null && !clusteredIsGuid
            ? KeyRangeOf(predicates, clusteredOrdinal)
            : (null, null);
        if (lower is { } l && upper is { } u && l >= u)
        {
            _trace?.Choose(ExplainOperators.PrimaryKeyRangeScan, fastPath: true, "empty clustered key range", rowsScanned: 0);
            return new ListedTable([]); // an empty range: nothing can match
        }

        var filterMask = new bool[columns.Count];
        MarkFilterColumns(predicates, filterMask);

        TypedScanState? state = _model.GetComputedAggregateColumns().Count > 0
            ? BuildAggregateScan(tableName, columns, predicates, filterMask, clusteredOrdinal, clusteredIsGuid)
            : _model.GroupByStatement.Model.Columns.Count > 0
                ? null
                : BuildRowScan(tableName, columns, predicates, filterMask, clusteredOrdinal, clusteredIsGuid);
        if (state is null)
        {
            return DeclineTypedScan(_model.GetComputedAggregateColumns().Count == 0
                ? "GROUP BY without aggregates"
                : "an aggregate or grouping shape the typed scan cannot fold");
        }

        if (_activeSnapshot is not null)
        {
            if (indexLookup is { } snapshotLookup)
            {
                List<RowKey> rowIds = MVCC.MvccCoordinator.FilterUsingIndex(
                    engine,
                    databaseName,
                    tableName,
                    snapshotLookup.IndexName,
                    snapshotLookup.ColumnName,
                    snapshotLookup.Key);
                rowIds.Sort();
                if (!engine.StorageContext.VisitVisibleRowsByKey(tableName, databaseName, rowIds, ref state, RowVisitorOf(state)))
                {
                    return DeclineTypedScan("the storage engine cannot read snapshot rows by key");
                }

                TraceIndexLookup(snapshotLookup.IndexName, rowIds.Count, "inside a transaction snapshot");
                return state.Aggregates is not null ? FinishAggregates(state) : FinishRows(state);
            }

            // A range on the InMemory engine's integer primary key walks only the committed rows in it, and the
            // snapshot's overlay supplies the rows that walk cannot see.
            if (clustered is null
                && TryPrimaryKeyRangeRows(engine, tableName, databaseName, columns, primaryKeys, predicates) is { } snapshotRange)
            {
                if (!engine.StorageContext.VisitVisibleRowsInKeyRange(tableName, databaseName, snapshotRange, ref state, RowVisitorOf(state), state.TypedReadMask))
                {
                    return DeclineTypedScan("the storage engine cannot read a snapshot key range");
                }

                _trace?.Choose(ExplainOperators.PrimaryKeyRangeScan, fastPath: true, "integer primary key range inside a transaction snapshot", state.Visited);
                return state.Aggregates is not null ? FinishAggregates(state) : FinishRows(state);
            }

            // Inside a transaction a non-indexed read streams the snapshot.
            return ScanSnapshotRows(engine, tableName, databaseName, lower, upper, state);
        }

        if (state.Aggregates is not null)
        {
            ListedTable? grouped = TryGroupCountsFromIndex(engine, tableName, databaseName, predicates, state, out IndexGroupOutcome outcome);
            if (outcome != IndexGroupOutcome.NotPlanned)
            {
                if (grouped is null)
                {
                    return DeclineTypedScan("the index stopped answering the grouped count part way");
                }

                _trace?.Choose(ExplainOperators.IndexGroupCount, fastPath: true, "COUNT per group from index key counts", rowsScanned: 0);
                return grouped; // null when the index stopped answering part way: the general path decides
            }
        }

        List<RowKey>? keyRangeRows = indexLookup is null && clustered is null
            ? TryPrimaryKeyRangeRows(engine, tableName, databaseName, columns, primaryKeys, predicates)
            : null;
        if (indexLookup is { } lookup)
        {
            var rowIds = engine.IndexManager.FilterUsingIndex(lookup.Key, lookup.IndexName, tableName, databaseName).ToList();
            rowIds.Sort();
            LockRowsForRead(databaseName, tableName, rowIds);
            if (!engine.StorageContext.VisitRowsByKey(tableName, databaseName, rowIds, ref state, RowVisitorOf(state)))
            {
                return DeclineTypedScan("the storage engine cannot read rows by key");
            }

            TraceIndexLookup(lookup.IndexName, rowIds.Count, detail: null);
        }
        else if (keyRangeRows is not null)
        {
            // A range on the InMemory engine's integer primary key: only the rows in it are read.
            LockRowsForRead(databaseName, tableName, keyRangeRows);
            if (!engine.StorageContext.VisitRowsByKey(tableName, databaseName, keyRangeRows, ref state, RowVisitorOf(state)))
            {
                return DeclineTypedScan("the storage engine cannot read rows by key");
            }

            _trace?.Choose(ExplainOperators.PrimaryKeyRangeScan, fastPath: true, "integer primary key range from the key lane", keyRangeRows.Count);
        }
        else if (TryAggregateFromColumnarIndex(engine, databaseName, tableName, state))
        {
            // Answered from the table's columnar index; no row was read.
            _trace?.Choose(ExplainOperators.ColumnarSimdAggregate, fastPath: true, "aggregates folded over the table's columnar index", rowsScanned: 0);
        }
        else if (!engine.StorageContext.ScanTableRows(tableName, databaseName, lower, upper, ref state, RowVisitorOf(state), state.TypedReadMask))
        {
            return DeclineTypedScan("the storage engine cannot stream typed rows");
        }
        else if (lower is not null || upper is not null)
        {
            _trace?.Choose(ExplainOperators.PrimaryKeyRangeScan, fastPath: true, "clustered primary key range", state.Visited);
        }
        else
        {
            _trace?.Choose(ExplainOperators.StreamingTableScan, fastPath: true, ScanDetail(state), state.Visited);
        }

        if (indexLookup is null && keyRangeRows is null && lower is null && upper is null)
        {
            RuntimeQueryDiagnosticsScope.RecordTableScan(tableName, (int)Math.Min(state.Sequence, int.MaxValue));
        }

        return state.Aggregates is not null ? FinishAggregates(state) : FinishRows(state);
    }

    /// <summary>
    /// Why neither typed fast path (scan or join) can take this statement whatever its WHERE, or null. Shared by
    /// both paths so EXPLAIN reports the same reason the dispatcher acted on.
    /// </summary>
    private string? SharedTypedRefusal()
    {
        if (ast.Ctes.Count > 0)
        {
            return "a common table expression (WITH)";
        }

        if (_model.FromTable is { } from && (from.IsMaterialized || _model.CteTables.ContainsKey(from.TableName)))
        {
            return "a derived table or CTE source";
        }

        if (_model.IsDistinct)
        {
            return "SELECT DISTINCT";
        }

        if (_model.Database is null)
        {
            return "no active database";
        }

        if (_model.GetComputedExpressionColumns().Count > 0)
        {
            return "a computed projection";
        }

        return _model.GetWindowFunctionColumns().Count > 0 ? "a window function" : null;
    }

    /// <summary>Records why the typed single-table scan declined; returns null so callers fall through.</summary>
    private ListedTable? DeclineTypedScan(string reason)
    {
        _trace?.Decline("TypedScan", reason);
        return null;
    }

    /// <summary>Records why a WHERE could not be collected as typed predicates; returns false.</summary>
    private bool RefusePredicate(string reason)
    {
        _predicateRefusal = reason;
        return false;
    }

    /// <summary>The first reason <see cref="TryCollectSidePredicates"/> refused the current WHERE.</summary>
    private string? _predicateRefusal;

    /// <summary>Records an index point lookup (primary key or secondary) on the trace.</summary>
    private void TraceIndexLookup(string indexName, int rows, string? detail)
    {
        if (_trace is null)
        {
            return;
        }

        bool primaryKey = indexName.StartsWith("_PK_", StringComparison.OrdinalIgnoreCase);
        _trace.Choose(primaryKey ? ExplainOperators.PrimaryKeyPointLookup : ExplainOperators.CompiledIndexLookup, fastPath: true, detail, rows);
        _trace.UseIndex(indexName);
    }

    /// <summary>What a typed streaming scan did besides reading rows.</summary>
    private static string ScanDetail(TypedScanState state) =>
        state.Aggregates is not null ? "aggregates folded while streaming"
        : state.Top is not null ? "bounded top-N while streaming"
        : "rows decoded only in the columns the query reads";

    /// <summary>A short name of an expression node for EXPLAIN, e.g. "function call".</summary>
    private static string DescribeNode(ExpressionNode node)
    {
        string name = node.GetType().Name;
        name = name.EndsWith("Node", StringComparison.Ordinal) ? name[..^4] : name;
        name = name.EndsWith("Expression", StringComparison.Ordinal) && name.Length > 10 ? name[..^10] : name;
        var words = new System.Text.StringBuilder();
        foreach (char c in name)
        {
            if (char.IsUpper(c) && words.Length > 0)
            {
                words.Append(' ');
            }

            words.Append(char.ToLowerInvariant(c));
        }

        return words.Length == 0 ? "expression" : words.ToString() + " expression";
    }

    /// <summary>
    /// Answers the typed scan from the transaction's snapshot: committed rows it sees, retained images and its own
    /// writes, each decoded only in the columns the query reads. Null when the snapshot cannot be streamed here.
    /// </summary>
    private ListedTable? ScanSnapshotRows(DataVoEngine engine, string tableName, string databaseName, RowKey? lower, RowKey? upper, TypedScanState state)
    {
        if (!engine.StorageContext.ScanVisibleRows(tableName, databaseName, lower, upper, ref state, RowVisitorOf(state), state.TypedReadMask))
        {
            return DeclineTypedScan("the storage engine cannot stream snapshot rows");
        }

        _trace?.Choose(ExplainOperators.MvccSnapshotScan, fastPath: true, "typed scan of the transaction snapshot", state.Visited);

        if (lower is null && upper is null)
        {
            RuntimeQueryDiagnosticsScope.RecordTableScan(tableName, (int)Math.Min(state.Sequence, int.MaxValue));
        }

        return state.Aggregates is not null ? FinishAggregates(state) : FinishRows(state);
    }

    /// <summary>
    /// The rows a range on a table's single-column integer primary key selects, read from the InMemory engine's
    /// primary-key lane in key order and returned in row order (the order a scan visits them in); null when there
    /// is no such range, the lane cannot answer, or the range holds more than a quarter of the table, which a scan
    /// reads faster. The WHERE is still tested on every row read.
    /// </summary>
    private static List<RowKey>? TryPrimaryKeyRangeRows(DataVoEngine engine, string tableName, string databaseName, IReadOnlyList<Column> columns, List<string> primaryKeys, List<TypedPredicate> predicates)
    {
        int keyOrdinal = primaryKeys.Count == 1 ? OrdinalOf(columns, primaryKeys[0]) : -1;
        if (keyOrdinal < 0
            || !(columns[keyOrdinal].Type.Equals("INT", StringComparison.OrdinalIgnoreCase) || columns[keyOrdinal].Type.Equals("BIGINT", StringComparison.OrdinalIgnoreCase))
            || !engine.StorageContext.TryEstimateRowCount(tableName, databaseName, out long tableRows))
        {
            return null;
        }

        (RowKey? lower, RowKey? upper) = KeyRangeOf(predicates, keyOrdinal);
        if (lower is null && upper is null)
        {
            return null;
        }

        if (lower is { } from && upper is { } to && from >= to)
        {
            return []; // an empty range
        }

        return engine.IndexManager.TryPrimaryKeyRangeRows(tableName, databaseName, lower?.AsInt64(), upper?.AsInt64(), tableRows);
    }

    private bool TryCollectTypedPredicates(ExpressionNode node, string tableName, IReadOnlyList<Column> columns, List<TypedPredicate> predicates) =>
        TryCollectSidePredicates(
            node,
            reference => ColumnOfTable(reference, tableName) is { } name ? (0, name) : null,
            [columns],
            [predicates]);

    /// <summary>
    /// Collects an AND of <c>column op literal</c> comparisons, <c>LIKE</c> tests and ORs of those, assigning
    /// each to the table side <paramref name="resolve"/> names. False when any part has another shape.
    /// </summary>
    private bool TryCollectSidePredicates(
        ExpressionNode node,
        Func<ExpressionNode?, (int Side, string Column)?> resolve,
        IReadOnlyList<Column>[] columnsBySide,
        List<TypedPredicate>[] predicatesBySide)
    {
        if (node is LiteralNode constant && node is not NullLiteralNode)
        {
            // No WHERE is bound as an always-true literal. Anything else constant takes the general path.
            bool alwaysTrue = constant.Value is true || (constant.Value is string text && text == SqlLiterals.TrueExpression);
            return alwaysTrue || RefusePredicate("a constant WHERE condition");
        }

        if (node is not BinaryExpressionNode binary)
        {
            return RefusePredicate(ContainsSubqueryExpression(node)
                ? "a subquery in WHERE"
                : $"a {DescribeNode(node)} in WHERE");
        }

        if (binary.Operator == Operators.AND)
        {
            return TryCollectSidePredicates(binary.Left, resolve, columnsBySide, predicatesBySide)
                && TryCollectSidePredicates(binary.Right, resolve, columnsBySide, predicatesBySide);
        }

        if (binary.Operator == Operators.OR)
        {
            return TryCollectDisjunction(binary, resolve, columnsBySide, predicatesBySide);
        }

        if (binary.Operator == Operators.LIKE || binary.Operator == Operators.NOT_LIKE)
        {
            return TryCollectLike(binary, resolve, columnsBySide, predicatesBySide)
                || RefusePredicate("LIKE whose pattern is not a literal");
        }

        BinaryExpressionNode comparison = ExpressionNodeNormalizer.NormalizeComparisonNode(binary);
        if (resolve(comparison.Left) is not { } target || comparison.Right is not LiteralNode literal)
        {
            return RefusePredicate(ContainsSubqueryExpression(binary)
                ? "a subquery in WHERE"
                : $"a comparison that is not column-versus-literal ({binary.Operator})");
        }

        IReadOnlyList<Column> columns = columnsBySide[target.Side];
        List<TypedPredicate> predicates = predicatesBySide[target.Side];
        int ordinal = OrdinalOf(columns, target.Column);
        if (ordinal < 0)
        {
            return RefusePredicate($"an unknown column {target.Column}");
        }

        string op = comparison.Operator;
        if (op == Operators.IS_NULL || op == Operators.IS_NOT_NULL)
        {
            predicates.Add(new TypedPredicate(ordinal, op, null, op == Operators.IS_NULL ? TypedPredicateKind.IsNull : TypedPredicateKind.IsNotNull, 0));
            return true;
        }

        if (op != Operators.EQUALS && op != Operators.NOT_EQUALS && op != Operators.LESS_THAN && op != Operators.LESS_THAN_OR_EQUAL_TO
            && op != Operators.GREATER_THAN && op != Operators.GREATER_THAN_OR_EQUAL_TO)
        {
            return RefusePredicate($"the operator {op}");
        }

        if (literal is NullLiteralNode || literal.Value is null)
        {
            return RefusePredicate("a comparison with a NULL literal");
        }

        if (columns[ordinal].Type.Equals("BIGINT", StringComparison.OrdinalIgnoreCase) && literal.Value is int or long)
        {
            predicates.Add(new TypedPredicate(ordinal, op, literal.Value, TypedPredicateKind.Integer, 0, Convert.ToInt64(literal.Value, System.Globalization.CultureInfo.InvariantCulture)));
            return true;
        }

        bool numericColumn = columns[ordinal].Type.Equals("INT", StringComparison.OrdinalIgnoreCase)
            || columns[ordinal].Type.Equals("BIGINT", StringComparison.OrdinalIgnoreCase)
            || columns[ordinal].Type.Equals("FLOAT", StringComparison.OrdinalIgnoreCase)
            || columns[ordinal].Type.Equals("DOUBLE", StringComparison.OrdinalIgnoreCase);
        if (numericColumn && TryLiteralAsNumber(literal.Value, out double number))
        {
            predicates.Add(new TypedPredicate(ordinal, op, literal.Value, TypedPredicateKind.Numeric, number));
            return true;
        }

        // Text column against a non-numeric text literal, = or <>: the general comparer then reduces to an
        // ordinal comparison of the unquoted strings (its numeric fallback needs both sides to be numbers).
        if ((op == Operators.EQUALS || op == Operators.NOT_EQUALS)
            && literal.Value is string quoted
            && IsTextColumn(columns[ordinal]))
        {
            string unquoted = literal is BoundValueLiteralNode ? quoted : UnquoteSingleQuoted(quoted);
            if (!double.TryParse(unquoted, NumberStyles.Any, CultureInfo.InvariantCulture, out _))
            {
                predicates.Add(new TypedPredicate(ordinal, op, unquoted, literal is BoundValueLiteralNode ? TypedPredicateKind.ExactText : TypedPredicateKind.Text, 0));
                return true;
            }
        }

        predicates.Add(new TypedPredicate(ordinal, op, literal.Value, TypedPredicateKind.General, 0));
        return true;
    }

    /// <summary>
    /// Collects an OR whose branches all read the same table side as one predicate that holds when some
    /// branch's conjunction holds. The WHERE has no NOT left (the parser pushes NOT into the comparisons), and
    /// keeps a row only when it is true, so treating an unknown branch (one that compared a NULL) as false
    /// gives the same rows as SQL's three-valued OR.
    /// </summary>
    private bool TryCollectDisjunction(
        BinaryExpressionNode node,
        Func<ExpressionNode?, (int Side, string Column)?> resolve,
        IReadOnlyList<Column>[] columnsBySide,
        List<TypedPredicate>[] predicatesBySide)
    {
        var branchNodes = new List<ExpressionNode>();
        FlattenOr(node, branchNodes);

        int side = -1;
        var branches = new TypedPredicate[branchNodes.Count][];
        for (int i = 0; i < branchNodes.Count; i++)
        {
            var bySide = new List<TypedPredicate>[columnsBySide.Length];
            for (int s = 0; s < bySide.Length; s++)
            {
                bySide[s] = [];
            }

            if (!TryCollectSidePredicates(branchNodes[i], resolve, columnsBySide, bySide))
            {
                return false;
            }

            int branchSide = -1;
            for (int s = 0; s < bySide.Length; s++)
            {
                if (bySide[s].Count == 0)
                {
                    continue;
                }

                if (branchSide >= 0)
                {
                    return RefusePredicate("OR across joined tables"); // one branch reads two tables
                }

                branchSide = s;
            }

            // A constant branch, or branches over different tables, take the general path.
            if (branchSide < 0 || (side >= 0 && side != branchSide))
            {
                return RefusePredicate(branchSide < 0 ? "a constant OR branch" : "OR across joined tables");
            }

            side = branchSide;
            branches[i] = [.. bySide[side]];
        }

        predicatesBySide[side].Add(TypedPredicate.Or(branches));
        return true;

        static void FlattenOr(ExpressionNode node, List<ExpressionNode> branches)
        {
            if (node is BinaryExpressionNode { Operator: Operators.OR } or)
            {
                FlattenOr(or.Left!, branches);
                FlattenOr(or.Right!, branches);
            }
            else
            {
                branches.Add(node);
            }
        }
    }

    /// <summary>
    /// Collects <c>column LIKE literal</c> or <c>column NOT LIKE literal</c>. A text column is matched against
    /// the compiled pattern; another column type keeps the general comparer's text conversion.
    /// </summary>
    private static bool TryCollectLike(
        BinaryExpressionNode node,
        Func<ExpressionNode?, (int Side, string Column)?> resolve,
        IReadOnlyList<Column>[] columnsBySide,
        List<TypedPredicate>[] predicatesBySide)
    {
        // The pattern is the right operand; the comparison normaliser's operand swap does not apply to LIKE.
        if (resolve(node.Left) is not { } target
            || node.Right is not LiteralNode literal
            || literal is NullLiteralNode
            || literal.Value is null)
        {
            return false;
        }

        IReadOnlyList<Column> columns = columnsBySide[target.Side];
        int ordinal = OrdinalOf(columns, target.Column);
        if (ordinal < 0)
        {
            return false;
        }

        // The general path unquotes both sides, a bound value included.
        predicatesBySide[target.Side].Add(IsTextColumn(columns[ordinal]) && literal.Value is string pattern
            ? TypedPredicate.Like(ordinal, node.Operator, pattern, UnquoteSingleQuoted(pattern))
            : new TypedPredicate(ordinal, node.Operator, literal.Value, TypedPredicateKind.General, 0));
        return true;
    }

    /// <summary>Marks every column <paramref name="predicates"/> read, inside ORs too.</summary>
    private static void MarkFilterColumns(IEnumerable<TypedPredicate> predicates, bool[] mask)
    {
        foreach (TypedPredicate predicate in predicates)
        {
            if (predicate.Branches is { } branches)
            {
                foreach (TypedPredicate[] branch in branches)
                {
                    MarkFilterColumns(branch, mask);
                }
            }
            else
            {
                mask[predicate.Ordinal] = true;
            }
        }
    }

    /// <summary>Whether a row's loaded <paramref name="cells"/> meet every predicate.</summary>
    private static bool MatchesAll(IReadOnlyList<TypedPredicate> predicates, CellValue[] cells, IReadOnlyList<Column> columns)
    {
        for (int i = 0; i < predicates.Count; i++)
        {
            if (!Matches(predicates[i], cells, columns))
            {
                return false;
            }
        }

        return true;
    }

    private static bool Matches(TypedPredicate predicate, CellValue[] cells, IReadOnlyList<Column> columns)
    {
        if (predicate.Branches is not { } branches)
        {
            return Matches(predicate, cells[predicate.Ordinal], columns[predicate.Ordinal]);
        }

        foreach (TypedPredicate[] branch in branches)
        {
            if (MatchesAll(branch, cells, columns))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The column name when <paramref name="node"/> is a reference to the FROM table, else null.</summary>
    private string? ColumnOfTable(ExpressionNode? node, string tableName) => node switch
    {
        ResolvedColumnRefNode resolved when resolved.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase) => resolved.Column,
        ColumnRefNode reference when reference.TableOrAlias is null
            || reference.TableOrAlias.Equals(tableName, StringComparison.OrdinalIgnoreCase)
            || reference.TableOrAlias.Equals(_model.FromTable.TableAlias, StringComparison.OrdinalIgnoreCase) => reference.Column,
        _ => null,
    };

    private static bool IsTextColumn(Column column) =>
        column.Type.Equals("VARCHAR", StringComparison.OrdinalIgnoreCase)
        || column.Type.Equals("CHAR", StringComparison.OrdinalIgnoreCase)
        || column.Type.Equals("TEXT", StringComparison.OrdinalIgnoreCase)
        || column.Type.Equals("NVARCHAR", StringComparison.OrdinalIgnoreCase);

    private static string UnquoteSingleQuoted(string value) =>
        value.Length >= 2 && value[0] == '\'' && value[^1] == '\'' ? value[1..^1] : value;

    private static ReadOnlySpan<char> UnquoteSingleQuoted(ReadOnlySpan<char> value) =>
        value.Length >= 2 && value[0] == '\'' && value[^1] == '\'' ? value[1..^1] : value;

    private static int OrdinalOf(IReadOnlyList<Column> columns, string name)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>A number literal (not a quoted string): the general comparers would also compare these numerically.</summary>
    private static bool TryLiteralAsNumber(object value, out double number)
    {
        switch (value)
        {
            case int i: number = i; return true;
            case long l: number = l; return true;
            case double d: number = d; return true;
            case float f: number = f; return true;
            case decimal m: number = (double)m; return true;
            default: number = 0; return false;
        }
    }

    /// <summary>2^53: every integer up to this size has its own double.</summary>
    private const double MaxExactInteger = 9007199254740992d;

    /// <summary>The key range implied by integer comparisons on the clustered key; a superset of the matches.</summary>
    private static (RowKey? Lower, RowKey? Upper) KeyRangeOf(List<TypedPredicate> predicates, int keyOrdinal)
    {
        long? low = null;
        long? high = null; // exclusive
        foreach (TypedPredicate predicate in predicates)
        {
            if (predicate.Ordinal != keyOrdinal || predicate.Kind is not (TypedPredicateKind.Numeric or TypedPredicateKind.Integer))
            {
                continue;
            }

            long floor;
            long ceiling;
            if (predicate.Kind == TypedPredicateKind.Integer)
            {
                floor = ceiling = predicate.Integer;
            }
            else
            {
                double n = predicate.Number;
                if (double.IsNaN(n) || Math.Abs(n) > MaxExactInteger)
                {
                    continue; // beyond this a double no longer names one integer, so it cannot bound the range
                }

                floor = (long)Math.Floor(n);
                ceiling = (long)Math.Ceiling(n);
            }

            if (floor == long.MaxValue && predicate.Operator is Operators.GREATER_THAN or Operators.LESS_THAN_OR_EQUAL_TO)
            {
                continue; // floor + 1 would wrap; the predicate itself still filters
            }

            switch (predicate.Operator)
            {
                case Operators.GREATER_THAN_OR_EQUAL_TO: Raise(ref low, ceiling); break;
                case Operators.GREATER_THAN: Raise(ref low, floor + 1); break;
                case Operators.LESS_THAN: Lower(ref high, ceiling); break;
                case Operators.LESS_THAN_OR_EQUAL_TO: Lower(ref high, floor + 1); break;
            }
        }

        return (low is { } a ? a : null, high is { } b ? b : null);

        static void Raise(ref long? bound, long value) => bound = bound is { } current ? Math.Max(current, value) : value;
        static void Lower(ref long? bound, long value) => bound = bound is { } current ? Math.Min(current, value) : value;
    }

    private TypedScanState? BuildRowScan(string tableName, IReadOnlyList<Column> columns, List<TypedPredicate> predicates, bool[] filterMask, int clusteredOrdinal, bool clusteredIsGuid)
    {
        List<(int Ordinal, bool Ascending)>? orderKeys = null;
        int topLimit = -1;
        OrderByNode? orderBy = _model.GetOrderByExpression();
        if (orderBy is { Columns.Count: > 0 })
        {
            // Only a top-N over plain columns is kept in the scan; a full ORDER BY sorts afterwards as before.
            // OFFSET without LIMIT asks for every row (LIMIT is int.MaxValue), which is a full sort, not a top-N.
            if (!_model.LimitTake.HasValue || _model.RowsNeededForLimit == int.MaxValue)
            {
                orderKeys = null;
            }
            else
            {
                orderKeys = [];
                foreach (OrderByColumnNode orderColumn in orderBy.Columns)
                {
                    string token = orderColumn.Column.Name;
                    if (_model.GetSelectColumnByAlias(token)?.Expression is { } aliased && aliased is not ColumnRefNode && aliased is not ResolvedColumnRefNode)
                    {
                        return null;
                    }

                    string name = token.Contains('.') ? token[(token.LastIndexOf('.') + 1)..] : token;
                    int ordinal = OrdinalOf(columns, name);
                    if (ordinal < 0)
                    {
                        return null;
                    }

                    orderKeys.Add((ordinal, orderColumn.IsAscending));
                }

                topLimit = _model.RowsNeededForLimit;
            }
        }

        bool[] outputMask = OutputColumnsOf(tableName, columns, orderKeys);
        bool fullSortLater = orderBy is { Columns.Count: > 0 } && topLimit < 0;
        (string Field, int Ordinal)[]? direct = fullSortLater ? null : DirectOutputOf(columns);
        int stopAfter = direct is not null && orderKeys is null && _model.LimitTake.HasValue
            ? _model.RowsNeededForLimit
            : -1;
        var restMask = new bool[columns.Count];
        for (int i = 0; i < restMask.Length; i++)
        {
            restMask[i] = outputMask[i] && !filterMask[i];
        }

        RawTextColumns? rawText = TryPlanRawText(columns, predicates, filterMask);
        DecodeRawTextAfterMatch(rawText, outputMask, restMask);
        return new TypedScanState
        {
            Columns = columns,
            Predicates = predicates,
            FilterMask = filterMask,
            RestMask = restMask,
            RawText = rawText,
            Cells = new CellValue[columns.Count],
            ClusteredOrdinal = clusteredOrdinal,
            ClusteredIsGuid = clusteredIsGuid,
            TableName = tableName,
            OutputMask = outputMask,
            DirectOutput = direct,
            StopAfter = stopAfter,
            OutputRows = topLimit < 0 && direct is not null ? [] : null,
            Rows = topLimit < 0 && direct is null ? [] : null,
            OrderKeys = topLimit < 0 ? null : orderKeys,
            TopLimit = topLimit,
            Top = topLimit < 0 ? null : [],
            TopCandidate = topLimit < 0 ? null : new CellValue[orderKeys!.Count],
        };
    }

    /// <summary>
    /// The columns a plain row result needs: those in the SELECT list and the ORDER BY keys. A SELECT *, or
    /// any entry this cannot read as a plain column of the table, keeps every column.
    /// </summary>
    private bool[] OutputColumnsOf(string tableName, IReadOnlyList<Column> columns, List<(int Ordinal, bool Ascending)>? orderKeys)
    {
        var mask = new bool[columns.Count];
        bool all = _model.TableColumnsInUse is null;
        if (!all)
        {
            foreach (string selected in _model.GetSelectedColumns())
            {
                int aliasAt = selected.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
                string reference = (aliasAt >= 0 ? selected[..aliasAt] : selected).Trim();
                int dot = reference.LastIndexOf('.');
                string table = dot > 0 ? reference[..dot] : tableName;
                int ordinal = OrdinalOf(columns, dot > 0 ? reference[(dot + 1)..] : reference);
                if (ordinal < 0 || !(table.Equals(tableName, StringComparison.OrdinalIgnoreCase)
                    || table.Equals(_model.FromTable.TableAlias, StringComparison.OrdinalIgnoreCase)))
                {
                    all = true;
                    break;
                }

                mask[ordinal] = true;
            }
        }

        if (!all && _model.GetOrderByExpression() is { Columns.Count: > 0 } orderBy)
        {
            foreach (OrderByColumnNode orderColumn in orderBy.Columns)
            {
                string token = orderColumn.Column.Name;
                int ordinal = OrdinalOf(columns, token.Contains('.') ? token[(token.LastIndexOf('.') + 1)..] : token);
                if (ordinal >= 0)
                {
                    mask[ordinal] = true;
                }
                else if (_model.GetSelectColumnByAlias(token) is null)
                {
                    all = true; // an ORDER BY the general sort resolves some other way
                }
            }
        }

        if (all)
        {
            Array.Fill(mask, true);
        }

        if (orderKeys is not null)
        {
            foreach ((int ordinal, _) in orderKeys)
            {
                mask[ordinal] = true;
            }
        }

        return mask;
    }

    private TypedScanState? BuildAggregateScan(string tableName, IReadOnlyList<Column> columns, List<TypedPredicate> predicates, bool[] filterMask, int clusteredOrdinal, bool clusteredIsGuid)
    {
        if (!TryBuildNoJoinAggregatePushdown(out List<string> groupByColumns, out List<HashAggregateOperator.AggregateSpec> specs))
        {
            return null;
        }

        List<SelectColumnNode> aggregateColumns = _model.GetComputedAggregateColumns();
        if (aggregateColumns.Count != specs.Count)
        {
            return null;
        }

        var aggregates = new List<TypedAggregate>(specs.Count);
        var needed = new bool[columns.Count];
        for (int i = 0; i < specs.Count; i++)
        {
            var node = (AggregateExpressionNode)aggregateColumns[i].Expression!;
            int ordinal = -1;
            if (!node.IsStar)
            {
                string? name = ColumnOfTable(node.Argument, tableName);
                ordinal = name is null ? -1 : OrdinalOf(columns, name);
                if (ordinal < 0)
                {
                    return null;
                }

                needed[ordinal] = true;
            }

            aggregates.Add(new TypedAggregate(specs[i], ordinal));
        }

        var groupOrdinals = new int[groupByColumns.Count];
        for (int i = 0; i < groupOrdinals.Length; i++)
        {
            groupOrdinals[i] = OrdinalOf(columns, groupByColumns[i]);
            if (groupOrdinals[i] < 0)
            {
                return null;
            }

            needed[groupOrdinals[i]] = true;
        }

        _volcanoAggregateGroupKeyColumns = [.. groupByColumns];
        _volcanoAggregateOutputColumns = [.. specs.Select(spec => spec.OutputColumn)];

        // needed: the columns the aggregates and group keys read; those the filter already loaded are not loaded twice.
        bool[] restMask = needed.Select((need, i) => need && !filterMask[i]).ToArray();
        RawTextColumns? rawText = TryPlanRawText(columns, predicates, filterMask);
        DecodeRawTextAfterMatch(rawText, needed, restMask);
        GroupTextColumn? groupText = TryPlanGroupText(columns, aggregates, groupOrdinals, restMask, clusteredOrdinal);
        return new TypedScanState
        {
            Columns = columns,
            Predicates = predicates,
            FilterMask = filterMask,
            RestMask = restMask,
            RawText = rawText,
            Cells = new CellValue[columns.Count],
            ClusteredOrdinal = clusteredOrdinal,
            ClusteredIsGuid = clusteredIsGuid,
            TableName = tableName,
            Aggregates = aggregates,
            GroupOrdinals = groupOrdinals,
            GroupColumnNames = groupByColumns,
            GroupKeys = new GroupLookup(textKeys: groupText is not null),
            GroupStates = new TypedGroupStates(aggregates),
            GroupText = groupText,
        };
    }

    // Compiled fully optimized on first use, so its speed does not depend on which queries ran before it.
    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
    private static bool VisitTypedRow(ref TypedScanState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        state.Visited++;
        CellValue[] cells = state.Cells;
        if (state.RawText is { } rawText && typedCells.IsEmpty)
        {
            // Serialized rows: text conditions are tested on the stored bytes, without a string per row.
            LoadFilterColumnsRaw(state, rawText, rowId, rowBytes);
            if (!MatchesAllRaw(state, rawText, state.Predicates, rowBytes))
            {
                return true;
            }
        }
        else
        {
            Load(state, rowId, rowBytes, typedCells, state.FilterMask);
            if (!MatchesAll(state.Predicates, cells, state.Columns))
            {
                return true;
            }
        }

        Load(state, rowId, rowBytes, typedCells, state.RestMask);
        state.Sequence++;
        if (state.Aggregates is not null)
        {
            Accumulate(state, FindTypedGroup(state));
            return true;
        }

        if (state.Top is not null)
        {
            OfferTop(state);
            return true;
        }

        if (state.OutputRows is { } output)
        {
            if (state.StopAfter == 0)
            {
                return false; // LIMIT 0: nothing is returned, so no row is built
            }

            output.Add(BuildDirectRow(state));
            return state.StopAfter < 0 || output.Count < state.StopAfter;
        }

        state.Rows!.Add(BuildJoinedRow(state));
        return true;
    }

    /// <summary>
    /// <see cref="VisitTypedRow"/> for a scan that groups by one text column (<see cref="TypedScanState.GroupText"/>):
    /// a matched serialized row finds its group by the stored bytes of the key instead of decoding it. A separate
    /// visitor, chosen once per scan, so other scans pay no per-row test for it.
    /// </summary>
    private static bool VisitTypedRowGroupedByText(ref TypedScanState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        if (!typedCells.IsEmpty)
        {
            return VisitTypedRow(ref state, rowId, rowBytes, typedCells);
        }

        state.Visited++;

        if (state.RawText is { } rawText)
        {
            LoadFilterColumnsRaw(state, rawText, rowId, rowBytes);
            if (!MatchesAllRaw(state, rawText, state.Predicates, rowBytes))
            {
                return true;
            }
        }
        else
        {
            Load(state, rowId, rowBytes, typedCells, state.FilterMask);
            if (!MatchesAll(state.Predicates, state.Cells, state.Columns))
            {
                return true;
            }
        }

        state.Sequence++;
        Accumulate(state, FindLocatedTextGroup(state, state.GroupText!, rowId, rowBytes));
        return true;
    }

    /// <summary>The row visitor of a typed scan.</summary>
    private static StorageContext.RawRowVisitor<TypedScanState> RowVisitorOf(TypedScanState state) =>
        state.GroupText is null ? VisitTypedRow : VisitTypedRowGroupedByText;

    private static object BuildOutputRow(TypedScanState state) =>
        state.DirectOutput is null ? BuildJoinedRow(state) : BuildDirectRow(state);

    /// <summary>One output row, keyed and ordered exactly as the general projection would build it.</summary>
    private static Dictionary<string, object?> BuildDirectRow(TypedScanState state)
    {
        (string Field, int Ordinal)[] output = state.DirectOutput!;
        var row = new Dictionary<string, object?>(output.Length);
        foreach ((string field, int ordinal) in output)
        {
            row[field] = StorageContext.ConvertCellForLegacyDictionary(state.Columns[ordinal], state.Cells[ordinal]);
        }

        return row;
    }

    private static void Load(TypedScanState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells, bool[] mask)
    {
        CellValue[] cells = state.Cells;
        if (!typedCells.IsEmpty)
        {
            for (int i = 0; i < mask.Length; i++)
            {
                if (mask[i])
                {
                    cells[i] = typedCells[i];
                }
            }
        }
        else
        {
            RowSerializer.DecodeCellsByOrdinal(rowBytes, state.Columns, mask, cells);
        }

        RestoreClusteredKey(state, rowId, mask);
    }

    /// <summary>Clustered rows store the key cell as NULL (the row key holds it): puts the key back when <paramref name="mask"/> reads it.</summary>
    private static void RestoreClusteredKey(TypedScanState state, RowKey rowId, bool[] mask)
    {
        CellValue[] cells = state.Cells;
        int key = state.ClusteredOrdinal;
        if (key >= 0 && mask[key] && cells[key].IsNull)
        {
            cells[key] = state.ClusteredIsGuid ? CellValue.From(rowId.AsGuid()) : KeyCell(state.Columns[key], rowId);
        }
    }

    private static CellValue KeyCell(Column column, RowKey rowId)
    {
        long value = rowId.AsInt64();
        return column.Type.Equals("INT", StringComparison.OrdinalIgnoreCase) ? CellValue.From(checked((int)value)) : CellValue.From(value);
    }

    private static bool Matches(TypedPredicate predicate, CellValue cell, Column column)
    {
        switch (predicate.Kind)
        {
            case TypedPredicateKind.IsNull:
                return cell.IsNull;
            case TypedPredicateKind.IsNotNull:
                return !cell.IsNull;
        }

        if (cell.IsNull)
        {
            return false;
        }

        if (predicate.Kind == TypedPredicateKind.Like && cell.Type == CellType.String)
        {
            bool like = predicate.Pattern!.IsMatch(UnquoteSingleQuoted(cell.AsString().AsSpan()));
            return predicate.Operator == Operators.LIKE ? like : !like;
        }

        if (predicate.Kind is TypedPredicateKind.Text or TypedPredicateKind.ExactText && cell.Type == CellType.String)
        {
            string stored = predicate.Kind == TypedPredicateKind.Text ? UnquoteSingleQuoted(cell.AsString()!) : cell.AsString()!;
            bool equal = string.Equals(stored, (string)predicate.Literal!, StringComparison.Ordinal);
            return predicate.Operator == Operators.EQUALS ? equal : !equal;
        }

        if (predicate.Kind == TypedPredicateKind.Integer)
        {
            long exact = cell.Type == CellType.Int32 ? cell.AsInt32() : cell.AsInt64();
            return Satisfies(predicate.Operator, exact.CompareTo(predicate.Integer));
        }

        if (predicate.Kind == TypedPredicateKind.Numeric)
        {
            double value = cell.Type switch
            {
                CellType.Int32 => cell.AsInt32(),
                CellType.Int64 => cell.AsInt64(),
                CellType.Double => StorageContext.FloatingValue(column, cell),
                _ => double.NaN,
            };

            if (!double.IsNaN(value))
            {
                int order = value.CompareTo(predicate.Number);
                return Satisfies(predicate.Operator, order);
            }
        }

        object? boxed = StorageContext.ConvertCellForLegacyDictionary(column, cell);
        return predicate.Operator switch
        {
            Operators.EQUALS => ExpressionValueComparer.AreEqual(boxed, predicate.Literal, trimQuotedStrings: true),
            Operators.NOT_EQUALS => ExpressionValueComparer.SqlNotEquals(boxed, predicate.Literal, trimQuotedStrings: true),
            Operators.LIKE => ExpressionValueComparer.MatchesLike(boxed, predicate.Literal, trimQuotedStrings: true),
            Operators.NOT_LIKE => ExpressionValueComparer.MatchesNotLike(boxed, predicate.Literal, trimQuotedStrings: true),
            _ => Satisfies(predicate.Operator, ExpressionValueComparer.Compare(boxed, predicate.Literal, trimQuotedStrings: true)),
        };
    }

    private static bool Satisfies(string op, int order) => op switch
    {
        Operators.EQUALS => order == 0,
        Operators.NOT_EQUALS => order != 0,
        Operators.LESS_THAN => order < 0,
        Operators.LESS_THAN_OR_EQUAL_TO => order <= 0,
        Operators.GREATER_THAN => order > 0,
        Operators.GREATER_THAN_OR_EQUAL_TO => order >= 0,
        _ => false,
    };

    private static JoinedRow BuildJoinedRow(TypedScanState state)
    {
        bool[]? output = state.OutputMask;
        var values = new Dictionary<string, object?>(state.Columns.Count);
        for (int i = 0; i < state.Columns.Count; i++)
        {
            if (output is null || output[i])
            {
                values[state.Columns[i].Name] = StorageContext.ConvertCellForLegacyDictionary(state.Columns[i], state.Cells[i]);
            }
        }

        return new JoinedRow(state.TableName, new Row(values));
    }

    /// <summary>
    /// Keeps the best <c>TopLimit</c> rows in a sorted list. Ties keep scan order, as the general path's
    /// stable sort over the same rows would. A row is compared with the kept rows as typed cells, so the
    /// rows that do not make the list (nearly all of a large scan) allocate nothing.
    /// </summary>
    private static void OfferTop(TypedScanState state)
    {
        List<(CellValue[] Keys, long Sequence, object Row)> top = state.Top!;
        if (state.TopLimit <= 0)
        {
            return; // LIMIT 0 keeps no row
        }

        CellValue[] keys = state.TopCandidate!;
        for (int i = 0; i < keys.Length; i++)
        {
            keys[i] = state.Cells[state.OrderKeys![i].Ordinal];
        }

        if (top.Count >= state.TopLimit && CompareTop(state, keys, state.Sequence, top[^1].Keys, top[^1].Sequence) >= 0)
        {
            return;
        }

        int low = 0;
        int high = top.Count;
        while (low < high)
        {
            int mid = (low + high) >>> 1;
            if (CompareTop(state, top[mid].Keys, top[mid].Sequence, keys, state.Sequence) <= 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        top.Insert(low, ((CellValue[])keys.Clone(), state.Sequence, BuildOutputRow(state)));
        if (top.Count > state.TopLimit)
        {
            top.RemoveAt(top.Count - 1);
        }
    }

    private static int CompareTop(TypedScanState state, CellValue[] left, long leftSequence, CellValue[] right, long rightSequence)
    {
        List<(int Ordinal, bool Ascending)> orderKeys = state.OrderKeys!;
        for (int i = 0; i < left.Length; i++)
        {
            int order = CompareCells(state.Columns[orderKeys[i].Ordinal], left[i], right[i]);
            if (order != 0)
            {
                return orderKeys[i].Ascending ? order : -order;
            }
        }

        return leftSequence.CompareTo(rightSequence);
    }

    private static void Accumulate(TypedScanState state, int group)
    {
        state.Matched++;
        TypedGroupStates groups = state.GroupStates!;
        List<TypedAggregate> aggregates = state.Aggregates!;
        for (int i = 0; i < aggregates.Count; i++)
        {
            TypedAggregate aggregate = aggregates[i];
            if (aggregate.Ordinal < 0)
            {
                groups.CountOf(group, i)++;
                continue;
            }

            CellValue cell = state.Cells[aggregate.Ordinal];
            if (cell.IsNull)
            {
                continue;
            }

            switch (aggregate.Spec.Function)
            {
                case HashAggregateOperator.AggregateFunction.Count:
                    groups.CountOf(group, i)++;
                    break;
                case HashAggregateOperator.AggregateFunction.Sum:
                    switch (cell.Type)
                    {
                        case CellType.Int32: groups.TotalOf(group, i).Add((long)cell.AsInt32()); break;
                        case CellType.Int64: groups.TotalOf(group, i).Add(cell.AsInt64()); break;
                        case CellType.Double: groups.TotalOf(group, i).Add(StorageContext.FloatingValue(state.Columns[aggregate.Ordinal], cell)); break;
                        default: groups.TotalOf(group, i).Add(StorageContext.ConvertCellForLegacyDictionary(state.Columns[aggregate.Ordinal], cell)!); break;
                    }

                    break;
                case HashAggregateOperator.AggregateFunction.Avg:
                    groups.SumOf(group, i) += cell.Type switch
                    {
                        CellType.Int32 => cell.AsInt32(),
                        CellType.Int64 => cell.AsInt64(),
                        CellType.Double => StorageContext.FloatingValue(state.Columns[aggregate.Ordinal], cell),
                        _ => Convert.ToDouble(StorageContext.ConvertCellForLegacyDictionary(state.Columns[aggregate.Ordinal], cell), CultureInfo.InvariantCulture),
                    };
                    groups.CountOf(group, i)++;
                    break;
                default:
                    AccumulateExtreme(state, groups, group, i, aggregate, cell);
                    break;
            }
        }
    }

    /// <summary>
    /// Keeps <paramref name="cell"/> as the MIN or MAX of a group when it beats the kept value. A number is
    /// compared with the kept value in place and boxed only when it replaces it.
    /// </summary>
    private static void AccumulateExtreme(TypedScanState state, TypedGroupStates groups, int group, int i, TypedAggregate aggregate, CellValue cell)
    {
        Column column = state.Columns[aggregate.Ordinal];
        bool min = aggregate.Spec.Function == HashAggregateOperator.AggregateFunction.Min;
        if (groups.HasExtremeOf(group, i) && TryCompareCellToBoxed(column, cell, groups.ExtremeOf(group, i), out int typedOrder))
        {
            if (min ? typedOrder >= 0 : typedOrder <= 0)
            {
                return;
            }

            groups.ExtremeOf(group, i) = StorageContext.ConvertCellForLegacyDictionary(column, cell);
            return;
        }

        object? value = StorageContext.ConvertCellForLegacyDictionary(column, cell);
        int order = groups.HasExtremeOf(group, i) ? DynamicObjectComparer.Instance.Compare(value, groups.ExtremeOf(group, i)) : 0;
        if (!groups.HasExtremeOf(group, i) || (min ? order < 0 : order > 0))
        {
            groups.ExtremeOf(group, i) = value;
            groups.HasExtremeOf(group, i) = true;
        }
    }

    private ListedTable FinishRows(TypedScanState state)
    {
        if (state.DirectOutput is not null)
        {
            List<Dictionary<string, object?>> rows = state.Top is not null
                ? state.Top.Select(entry => (Dictionary<string, object?>)entry.Row).ToList()
                : state.OutputRows!;
            _typedOutput = (_typedOutputFields!, rows);
            return new ListedTable();
        }

        return state.Top is not null
            ? new ListedTable(state.Top.Select(entry => (JoinedRow)entry.Row).ToList())
            : new ListedTable(state.Rows!);
    }

    /// <summary>Output already built by the typed path (fields and rows in final order, before OFFSET/LIMIT).</summary>
    private (List<string> Fields, List<Dictionary<string, object?>> Rows)? _typedOutput;
    private List<string>? _typedOutputFields;

    /// <summary>
    /// The output columns when the typed path can write final rows itself: explicit plain columns of the
    /// table, and no HAVING, so nothing after the scan reshapes a row. Null when the general projection runs.
    /// </summary>
    private (string Field, int Ordinal)[]? DirectOutputOf(IReadOnlyList<Column> columns)
    {
        if (_model.GetHavingExpression() is not null)
        {
            return null;
        }

        List<string> selected = _model.GetSelectedColumns();
        var ordinals = new int[selected.Count];
        for (int i = 0; i < selected.Count; i++)
        {
            string entry = selected[i];
            int aliasAt = entry.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
            string reference = (aliasAt >= 0 ? entry[..aliasAt] : entry).Trim();
            int dot = reference.LastIndexOf('.');
            ordinals[i] = OrdinalOf(columns, dot > 0 ? reference[(dot + 1)..] : reference);
            if (ordinals[i] < 0)
            {
                return null;
            }
        }

        // The general projection's own field names and SELECT-list order.
        List<string> fields = CreateFieldsFromColumns(new ListedTable());
        if (_projectionBuildOrder.Count != ordinals.Length)
        {
            return null;
        }

        var output = new (string Field, int Ordinal)[ordinals.Length];
        for (int i = 0; i < ordinals.Length; i++)
        {
            int buildIndex = _projectionPermutation is { } permutation ? permutation[i] : i;
            output[i] = (_projectionBuildOrder[buildIndex], ordinals[buildIndex]);
        }

        _typedOutputFields = fields;
        return output;
    }
}
