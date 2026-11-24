namespace DataVo.Core.Runtime.Diagnostics;

/// <summary>The physical operators a traced <c>SELECT</c> can report through <c>EXPLAIN</c>.</summary>
internal static class ExplainOperators
{
    /// <summary>Equality on a single-column primary key, answered from the key index.</summary>
    public const string PrimaryKeyPointLookup = "PrimaryKeyPointLookup";

    /// <summary>Equality on a secondary index, answered from the index, rows decoded typed.</summary>
    public const string CompiledIndexLookup = "CompiledIndexLookup";

    /// <summary>A range on an integer or clustered primary key: only the rows in the range are read.</summary>
    public const string PrimaryKeyRangeScan = "PrimaryKeyRangeScan";

    /// <summary>Aggregates folded over dense columns with SIMD kernels; no row is decoded.</summary>
    public const string ColumnarSimdAggregate = "ColumnarSimdAggregate";

    /// <summary><c>COUNT(*) ... GROUP BY</c> answered from an index's key counts.</summary>
    public const string IndexGroupCount = "IndexGroupCount";

    /// <summary>A typed streaming scan of the table: rows decoded only in the columns the query reads.</summary>
    public const string StreamingTableScan = "StreamingTableScan";

    /// <summary>A typed streaming scan of a transaction snapshot (committed rows, retained images, own writes).</summary>
    public const string MvccSnapshotScan = "MvccSnapshotScan";

    /// <summary>A typed hash join on integer equality keys.</summary>
    public const string IntegerHashJoin = "IntegerHashJoin";

    /// <summary>Approximate nearest-neighbour search through an HNSW vector index.</summary>
    public const string VectorHnswScan = "VectorHnswScan";

    /// <summary>The general evaluator: rows materialized as dictionaries and evaluated by the Volcano or legacy pipeline.</summary>
    public const string GeneralDictionaryEvaluator = "GeneralDictionaryEvaluator";

    /// <summary>A <c>SELECT</c> of constants with no table.</summary>
    public const string ConstantProjection = "ConstantProjection";
}

/// <summary>
/// Records how one <c>SELECT</c> actually executed: the operator that produced its rows, whether that was a
/// zero-allocation fast path, every fast path that declined it and why, and the rows and columnar segments read.
/// <c>EXPLAIN</c> runs the statement under a trace; ordinary statements carry none and pay one null check per site.
/// </summary>
/// <remarks>
/// The trace flows with the async context, so storage code reached by the statement (the LSM columnar scan) can
/// count what it skipped. The outermost <c>SELECT</c> claims the trace; subqueries and CTEs it materializes do not
/// overwrite its operator.
/// </remarks>
internal sealed class QueryExecutionTrace
{
    private static readonly AsyncLocal<QueryExecutionTrace?> s_current = new();

    private readonly List<string> _declines = [];
    private readonly List<string> _indexes = [];
    private object? _owner;
    private long _segmentsRead;
    private long _segmentsSkipped;

    /// <summary>The trace of the statement running on this flow, or null.</summary>
    public static QueryExecutionTrace? Current => s_current.Value;

    /// <summary>The operator that produced the rows (one of <see cref="ExplainOperators"/>).</summary>
    public string Operator { get; private set; } = ExplainOperators.GeneralDictionaryEvaluator;

    /// <summary>Whether <see cref="Operator"/> is a typed, zero-allocation fast path.</summary>
    public bool FastPath { get; private set; }

    /// <summary>Operator-specific detail (plan variant, join steps, vector routing).</summary>
    public string? Detail { get; private set; }

    /// <summary>Why the statement fell back to the general evaluator; null when a fast path ran.</summary>
    public string? FallbackReason => FastPath || _declines.Count == 0 ? null : _declines[0];

    /// <summary>Every fast path that declined the statement, in the order they were tried, as "path: reason".</summary>
    public IReadOnlyList<string> Declines => _declines;

    /// <summary>The indexes the operator read.</summary>
    public IReadOnlyList<string> Indexes => _indexes;

    /// <summary>The planner's row estimate for the FROM table, or -1 when unknown.</summary>
    public long EstimatedRows { get; set; } = -1;

    /// <summary>Rows the operator read from storage, or -1 when it did not count them.</summary>
    public long RowsScanned { get; private set; } = -1;

    /// <summary>Columnar segments (chunk groups) read.</summary>
    public long SegmentsRead => Interlocked.Read(ref _segmentsRead);

    /// <summary>Columnar segments skipped because their zone maps ruled the condition out.</summary>
    public long SegmentsSkipped => Interlocked.Read(ref _segmentsSkipped);

    /// <summary>Starts tracing the statements run on this flow until the returned scope is disposed.</summary>
    public static IDisposable Begin(out QueryExecutionTrace trace)
    {
        var created = new QueryExecutionTrace();
        QueryExecutionTrace? previous = s_current.Value;
        s_current.Value = created;
        trace = created;
        return new Scope(previous);
    }

    /// <summary>The current trace when <paramref name="statement"/> is the first statement to claim it; otherwise null.</summary>
    public static QueryExecutionTrace? Claim(object statement)
    {
        QueryExecutionTrace? trace = s_current.Value;
        if (trace is null)
        {
            return null;
        }

        if (trace._owner is null)
        {
            trace._owner = statement;
        }

        return ReferenceEquals(trace._owner, statement) ? trace : null;
    }

    /// <summary>Records the operator that produced the rows.</summary>
    public void Choose(string op, bool fastPath, string? detail = null, long rowsScanned = -1)
    {
        Operator = op;
        FastPath = fastPath;
        Detail = detail;
        if (rowsScanned >= 0)
        {
            RowsScanned = rowsScanned;
        }
    }

    /// <summary>Records that <paramref name="path"/> declined the statement because of <paramref name="reason"/>.</summary>
    public void Decline(string path, string reason) => _declines.Add($"{path}: {reason}");

    /// <summary>Records an index the operator read.</summary>
    public void UseIndex(string indexName)
    {
        if (!_indexes.Contains(indexName, StringComparer.OrdinalIgnoreCase))
        {
            _indexes.Add(indexName);
        }
    }

    /// <summary>Sets the rows the operator read.</summary>
    public void SetRowsScanned(long rows) => RowsScanned = rows;

    /// <summary>Counts one columnar segment read (<paramref name="skipped"/> false) or skipped by its zone maps.</summary>
    public void CountSegment(bool skipped)
    {
        if (skipped)
        {
            Interlocked.Increment(ref _segmentsSkipped);
        }
        else
        {
            Interlocked.Increment(ref _segmentsRead);
        }
    }

    private sealed class Scope(QueryExecutionTrace? previous) : IDisposable
    {
        public void Dispose() => s_current.Value = previous;
    }
}
