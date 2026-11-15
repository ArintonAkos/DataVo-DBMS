using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// The hashed (build) side of the typed join: which columns of each joined table are read once a row is joined,
/// and the hash tables that hold them.
/// </summary>
/// <remarks>
/// A hashed table keeps only the columns a joined row reads after the join: those in the SELECT list, GROUP BY,
/// aggregates and ORDER BY, and those a LEFT-joined table's conditions test. When that is nothing but the join key,
/// as in <c>LEFT JOIN c ... WHERE c.Id IS NULL</c> or <c>COUNT(c.Id)</c>, the table keeps only how many rows have
/// each key: no row is decoded beyond its key and none is copied.
/// </remarks>
internal partial class Select
{
    /// <summary>
    /// The rows of one joined table by join key. A key-only table counts its rows per key and hands out one
    /// shared row holding the key, which is rewritten at every match.
    /// </summary>
    private sealed class JoinHashTable
    {
        private readonly Dictionary<long, int>? _counts;
        private readonly Dictionary<long, (CellValue[] First, List<CellValue[]>? More)>? _rows;
        private readonly CellValue[]? _keyRow;
        private readonly int _keyOrdinal;
        private readonly bool _bigIntKey;

        public JoinHashTable(bool keyOnly, int columns, int keyOrdinal, bool bigIntKey, int capacity)
        {
            _keyOrdinal = keyOrdinal;
            _bigIntKey = bigIntKey;
            if (keyOnly)
            {
                _counts = new Dictionary<long, int>(capacity);
                _keyRow = new CellValue[columns];
            }
            else
            {
                _rows = new Dictionary<long, (CellValue[] First, List<CellValue[]>? More)>(capacity);
            }
        }

        /// <summary>Counts one more row with <paramref name="key"/> in a key-only table.</summary>
        public void AddKey(long key)
        {
            _counts!.TryGetValue(key, out int count);
            _counts[key] = count + 1;
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
            if (_counts is not null)
            {
                if (_counts.TryGetValue(key, out int count))
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

            matches = default;
            return false;
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

            bool bigIntKey = side.Columns[step.KeyOrdinal].Type.Equals("BIGINT", StringComparison.OrdinalIgnoreCase);
            step.Table = new JoinHashTable(side.KeyOnly, side.Columns.Count, step.KeyOrdinal, bigIntKey, capacity);
        }

        public JoinSide Side { get; }
        public JoinStep Step { get; }
        public bool Filter { get; }
        public CellValue[] Cells { get; }
        public bool[] DecodeMask { get; }
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
            if (!engine.StorageContext.ScanTableRows(side.Detail.TableName, databaseName, null, null, ref buildState, VisitJoinBuildRow))
            {
                return false;
            }
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
        LoadJoinCells(side, rowId, rowBytes, typedCells, state.DecodeMask, cells);
        if ((state.Filter && !MatchesAll(side.Predicates, cells, side.Columns))
            || !TryJoinKey(cells[state.Step.KeyOrdinal], out long key))
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
