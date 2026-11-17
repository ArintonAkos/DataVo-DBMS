using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// The hashed (build) side of the typed join: which columns of each joined table are read once a row is joined,
/// and the hash tables that hold them.
/// </summary>
/// <remarks>
/// A hashed table keeps only the columns a joined row reads after the join: those in the SELECT list, GROUP BY,
/// aggregates and ORDER BY, and those a LEFT-joined table's conditions test. When that is nothing but the join key,
/// as in <c>LEFT JOIN c ... WHERE c.Id IS NULL</c> or <c>COUNT(c.Id)</c>, the table keeps only its keys: no row is
/// decoded beyond its key and none is copied. Keys that are unique and close together (integer primary keys) are
/// kept as one bit per key of their range (<see cref="DenseInt64Set"/>), so a probe is a bit test instead of a
/// hash lookup; other keys are hashed with the number of rows that have each.
/// </remarks>
internal partial class Select
{
    /// <summary>
    /// The rows of one joined table by join key. A key-only table keeps its keys (a dense set, or row counts
    /// per key) and hands out one shared row holding the key, which is rewritten at every match.
    /// </summary>
    private sealed class JoinHashTable
    {
        private readonly bool _keyOnly;
        private Dictionary<long, int>? _counts;
        private DenseInt64Set? _keySet;
        private long[]? _pendingKeys;
        private int _pendingCount;
        private readonly Dictionary<long, (CellValue[] First, List<CellValue[]>? More)>? _rows;
        private readonly CellValue[]? _keyRow;
        private readonly int _keyOrdinal;
        private readonly bool _bigIntKey;
        private HashSet<long>? _absent;

        public JoinHashTable(bool keyOnly, int columns, int keyOrdinal, bool bigIntKey, int capacity)
        {
            _keyOrdinal = keyOrdinal;
            _bigIntKey = bigIntKey;
            _keyOnly = keyOnly;
            if (keyOnly)
            {
                // Keys are gathered in a pooled buffer and turned into a set once all are known (see Seal).
                _pendingKeys = System.Buffers.ArrayPool<long>.Shared.Rent(Math.Max(capacity, 256));
                _keyRow = new CellValue[columns];
            }
            else
            {
                _rows = new Dictionary<long, (CellValue[] First, List<CellValue[]>? More)>(capacity);
            }
        }

        /// <summary>
        /// When set, the table is filled on demand: a key not seen yet is read through this (null for no row)
        /// and remembered, so only the rows the join asks for are ever read.
        /// </summary>
        public Func<long, CellValue[]?>? Loader { get; set; }

        /// <summary>The number of distinct keys.</summary>
        public int KeyCount => !_keyOnly ? _rows!.Count : Sealed()._keySet?.Count ?? _counts!.Count;

        /// <summary>The distinct keys, each once.</summary>
        public IEnumerable<long> Keys => !_keyOnly ? _rows!.Keys : Sealed()._keySet?.Keys() ?? _counts!.Keys;

        /// <summary>Notes one more row with <paramref name="key"/> in a key-only table.</summary>
        public void AddKey(long key)
        {
            long[] pending = _pendingKeys!;
            if (_pendingCount == pending.Length)
            {
                long[] larger = System.Buffers.ArrayPool<long>.Shared.Rent(pending.Length * 2);
                pending.AsSpan().CopyTo(larger);
                System.Buffers.ArrayPool<long>.Shared.Return(pending);
                _pendingKeys = pending = larger;
            }

            pending[_pendingCount++] = key;
        }

        /// <summary>
        /// Ends the filling of a key-only table: the gathered keys become a dense set when they are unique and
        /// close together, and counts per key in a hash table otherwise. Calling it again does nothing.
        /// </summary>
        public void Seal()
        {
            if (_pendingKeys is not { } pending)
            {
                return;
            }

            ReadOnlySpan<long> keys = pending.AsSpan(0, _pendingCount);
            _keySet = DenseInt64Set.TryCreate(keys);
            if (_keySet is null)
            {
                _counts = new Dictionary<long, int>(keys.Length);
                foreach (long key in keys)
                {
                    _counts.TryGetValue(key, out int count);
                    _counts[key] = count + 1;
                }
            }

            _pendingKeys = null;
            System.Buffers.ArrayPool<long>.Shared.Return(pending);
        }

        private JoinHashTable Sealed()
        {
            Seal();
            return this;
        }

        /// <summary>Adds a row (owned by the table from now on) under <paramref name="key"/>.</summary>
        public void AddRow(long key, CellValue[] row)
        {
            if (!_rows!.TryGetValue(key, out (CellValue[] First, List<CellValue[]>? More) bucket))
            {
                _rows[key] = (row, null);
                return;
            }

            if (bucket.More is null)
            {
                _rows[key] = (bucket.First, [row]);
                return;
            }

            bucket.More.Add(row);
        }

        /// <summary>The rows with <paramref name="key"/>; false when there are none.</summary>
        public bool TryMatch(long key, out JoinMatches matches)
        {
            if (_keyOnly)
            {
                Seal();
                int count = 0;
                if (_keySet is { } keySet ? keySet.Contains(key) && (count = 1) == 1 : _counts!.TryGetValue(key, out count))
                {
                    _keyRow![_keyOrdinal] = _bigIntKey ? CellValue.From(key) : CellValue.From((int)key);
                    matches = new JoinMatches(_keyRow, null, count);
                    return true;
                }
            }
            else if (_rows!.TryGetValue(key, out (CellValue[] First, List<CellValue[]>? More) bucket))
            {
                matches = new JoinMatches(bucket.First, bucket.More, 1 + (bucket.More?.Count ?? 0));
                return true;
            }
            else if (Loader is not null && TryLoad(key, out CellValue[]? loaded))
            {
                matches = new JoinMatches(loaded, null, 1);
                return true;
            }

            matches = default;
            return false;
        }

        /// <summary>Reads the row of a key asked for the first time; a key without a row is not read again.</summary>
        private bool TryLoad(long key, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out CellValue[]? row)
        {
            row = null;
            if (_absent is not null && _absent.Contains(key))
            {
                return false;
            }

            row = Loader!(key);
            if (row is null)
            {
                (_absent ??= []).Add(key);
                return false;
            }

            _rows![key] = (row, null);
            return true;
        }
    }

    /// <summary>The rows of a hashed table that match one key; a key-only table repeats its shared row.</summary>
    private readonly struct JoinMatches(CellValue[] first, List<CellValue[]>? more, int count)
    {
        public int Count { get; } = count;

        public CellValue[] this[int index] => index == 0 || more is null ? first : more[index - 1];
    }

    /// <summary>The scan of one joined table into its hash table.</summary>
    private sealed class JoinBuildState
    {
        public JoinBuildState(JoinSide side, JoinStep step, bool filter, int capacity)
        {
            Side = side;
            Step = step;
            Filter = filter;
            Cells = new CellValue[side.Columns.Count];

            // Decoded: the kept columns, the key, and the conditions tested while hashing.
            DecodeMask = (bool[])side.KeepMask!.Clone();
            DecodeMask[step.KeyOrdinal] = true;
            if (filter)
            {
                MarkFilterColumns(side.Predicates, DecodeMask);
            }

            Load = PlanJoinBuildLoad(side, step.KeyOrdinal, filter, side.KeepMask!);
            bool bigIntKey = side.Columns[step.KeyOrdinal].Type.Equals("BIGINT", StringComparison.OrdinalIgnoreCase);
            step.Table = new JoinHashTable(side.KeyOnly, side.Columns.Count, step.KeyOrdinal, bigIntKey, capacity);
        }

        public JoinSide Side { get; }
        public JoinStep Step { get; }
        public bool Filter { get; }
        public CellValue[] Cells { get; }
        public bool[] DecodeMask { get; }

        /// <summary>How serialized rows are read: filter first, kept columns only for rows that pass.</summary>
        public JoinBuildLoad Load { get; }
    }

    /// <summary>Scans every joined table into its hash table; false when a table cannot be scanned here.</summary>
    private static bool TryBuildJoinTables(DataVoEngine engine, JoinSide[] sides, JoinStep[] steps, string databaseName)
    {
        foreach (JoinStep step in steps)
        {
            // An INNER-joined table may be filtered while it is hashed; a LEFT-joined one is tested after its step.
            JoinSide side = sides[step.Side];
            bool filter = !step.Outer && side.Predicates.Count > 0;
            var buildState = new JoinBuildState(side, step, filter: !step.Outer, filter ? 0 : HashCapacity(engine, side, databaseName));
            if (!engine.StorageContext.ScanTableRows(side.Detail.TableName, databaseName, null, null, ref buildState, VisitJoinBuildRow, buildState.DecodeMask))
            {
                return false;
            }

            step.Table.Seal();
        }

        return true;
    }

    /// <summary>The expected row count of an unfiltered table, to size its hash table once; 0 when unknown.</summary>
    private static int HashCapacity(DataVoEngine engine, JoinSide side, string databaseName)
    {
        long rows = EstimateRows(engine, side, databaseName);
        return rows is > 0 and <= 1 << 24 ? (int)rows : 0;
    }

    private static bool VisitJoinBuildRow(ref JoinBuildState state, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        JoinSide side = state.Side;
        CellValue[] cells = state.Cells;
        bool passed;
        if (typedCells.IsEmpty)
        {
            passed = LoadSerializedBuildRow(side, state.Load, state.Filter, rowId, rowBytes, cells);
        }
        else
        {
            LoadJoinCells(side, rowId, rowBytes, typedCells, state.DecodeMask, cells);
            passed = !state.Filter || MatchesAll(side.Predicates, cells, side.Columns);
        }

        if (!passed || !TryJoinKey(cells[state.Step.KeyOrdinal], out long key))
        {
            return true;
        }

        if (side.KeyOnly)
        {
            state.Step.Table.AddKey(key);
        }
        else
        {
            state.Step.Table.AddRow(key, (CellValue[])cells.Clone());
        }

        return true;
    }

    /// <summary>
    /// Sets each hashed table's <see cref="JoinSide.KeepMask"/>: the columns a joined row reads after the join.
    /// Those are <paramref name="used"/> (group columns, aggregate arguments, sort keys), the conditions of a
    /// LEFT-joined table, and every column the SELECT list or ORDER BY names. A SELECT *, a HAVING, or a SELECT or
    /// ORDER BY entry this cannot resolve to a column keeps every column. A table that keeps nothing but its join
    /// key is <see cref="JoinSide.KeyOnly"/>.
    /// </summary>
    private void MarkHashedColumns(JoinSide[] sides, int probeIndex, JoinStep[] steps, IEnumerable<(int Side, int Ordinal)> used)
    {
        foreach (JoinStep step in steps)
        {
            JoinSide side = sides[step.Side];
            side.KeepMask = new bool[side.Columns.Count];
            if (step.Outer)
            {
                MarkFilterColumns(side.Predicates, side.KeepMask);
            }
        }

        // A hashed table in the middle of a chain supplies the key of the step after it.
        foreach (JoinStep step in steps)
        {
            if (step.SourceSide != probeIndex)
            {
                sides[step.SourceSide].KeepMask![step.SourceOrdinal] = true;
            }
        }

        foreach ((int side, int ordinal) in used)
        {
            if (side != probeIndex)
            {
                sides[side].KeepMask![ordinal] = true;
            }
        }

        bool all = _model.TableColumnsInUse is null
            || _model.GetHavingExpression() is not null
            || !TryMarkSelectedJoinColumns(sides, probeIndex)
            || !TryMarkOrderByJoinColumns(sides, probeIndex);
        foreach (JoinStep step in steps)
        {
            JoinSide side = sides[step.Side];
            if (all)
            {
                Array.Fill(side.KeepMask!, true);
            }

            side.KeyOnly = true;
            for (int c = 0; c < side.KeepMask!.Length && side.KeyOnly; c++)
            {
                side.KeyOnly = !side.KeepMask[c] || c == step.KeyOrdinal;
            }
        }
    }

    /// <summary>Marks the hashed columns the SELECT list names (<c>T.C</c> or <c>T.C AS x</c>); false for anything else.</summary>
    private bool TryMarkSelectedJoinColumns(JoinSide[] sides, int probeIndex)
    {
        foreach (string selected in _model.GetSelectedColumns())
        {
            int aliasAt = selected.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
            string reference = (aliasAt >= 0 ? selected[..aliasAt] : selected).Trim();
            int dot = reference.LastIndexOf('.');
            if (dot <= 0 || ResolveJoinSideColumn(reference[..dot], reference[(dot + 1)..], sides) is not { } column)
            {
                return false;
            }

            if (column.Side != probeIndex)
            {
                sides[column.Side].KeepMask![column.Ordinal] = true;
            }
        }

        return true;
    }

    /// <summary>
    /// Marks the hashed columns ORDER BY sorts on, directly or through a SELECT alias; an alias of an aggregate
    /// reads the aggregate's value. False for a key this cannot resolve.
    /// </summary>
    private bool TryMarkOrderByJoinColumns(JoinSide[] sides, int probeIndex)
    {
        if (_model.GetOrderByExpression() is not { Columns.Count: > 0 } orderBy)
        {
            return true;
        }

        foreach (OrderByColumnNode orderColumn in orderBy.Columns)
        {
            string token = orderColumn.Column.Name;
            ExpressionNode? reference = _model.GetSelectColumnByAlias(token)?.Expression;
            if (reference is AggregateExpressionNode)
            {
                continue;
            }

            if (reference is null)
            {
                int dot = token.LastIndexOf('.');
                reference = new ColumnRefNode { TableOrAlias = dot > 0 ? token[..dot] : null, Column = dot > 0 ? token[(dot + 1)..] : token };
            }

            if (ResolveJoinColumn(reference, sides) is not { } column)
            {
                return false;
            }

            if (column.Side != probeIndex)
            {
                sides[column.Side].KeepMask![OrdinalOf(sides[column.Side].Columns, column.Column)] = true;
            }
        }

        return true;
    }
}
