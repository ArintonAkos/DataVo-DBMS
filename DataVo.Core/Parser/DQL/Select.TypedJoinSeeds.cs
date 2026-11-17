using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// The typed join over a given set of FROM-table rows: the candidates of a vector-index search. Each candidate
/// is tested and joined in stored form, as the typed join does for a scanned table, and only the joined rows
/// that pass every condition are built as dictionaries.
/// </summary>
/// <remarks>
/// The general seeded join loaded every candidate as a dictionary (stored vector included) and every row of each
/// joined table before it tested anything. Here a joined table whose join key is its clustered integer primary
/// key is not read up front at all: a row is fetched by key the first time a candidate refers to it.
/// </remarks>
internal partial class Select
{
    /// <summary>The planned join of a seeded query: the FROM table first, and one step per joined table.</summary>
    private sealed class SeedJoinPlan
    {
        public required JoinSide[] Sides { get; init; }
        public required JoinStep[] Steps { get; init; }

        /// <summary>Set when a joined row could not be read in stored form; the result is then discarded.</summary>
        public bool Unreadable { get; set; }
    }

    /// <summary>Reads one row of a joined table by key, for <see cref="JoinHashTable.Loader"/>.</summary>
    private sealed class SeedRowLookup(JoinBuildState build)
    {
        public JoinBuildState Build { get; } = build;

        /// <summary>The row read, or null when it fails the table's conditions.</summary>
        public CellValue[]? Row { get; set; }
    }

    /// <summary>
    /// Plans the typed join of the query for rows of the FROM table given by key. Returns null when the query
    /// is not an INNER/LEFT join of stored tables on integer columns whose WHERE is made of per-table typed
    /// conditions, or when a joined table cannot be read in stored form; the caller then joins dictionaries.
    /// </summary>
    private SeedJoinPlan? TryPlanSeedJoin(ExpressionNode? where)
    {
        if (_activeSnapshot is not null
            || ast.Ctes.Count > 0
            || _model.Database is null
            || _model.TableService is null
            || _model.FromTable is null
            || _model.JoinStatement.Model.JoinConditions.Count == 0)
        {
            return null;
        }

        DataVoEngine engine = DataVoEngine.Current();
        if (!TryBuildJoinSides(engine, out JoinSide[] sides, out int fromIndex)
            || !TryPlanJoinSteps(engine, sides, fromIndex, streamFrom: true, out int probeIndex, out JoinStep[] steps)
            || (where is not null && !TryCollectSidePredicates(
                where,
                reference => ResolveJoinColumn(reference, sides),
                [.. sides.Select(side => side.Columns)],
                [.. sides.Select(side => side.Predicates)])))
        {
            return null;
        }

        JoinSide probe = sides[probeIndex];
        probe.FilterMask = new bool[probe.Columns.Count];
        probe.NeededMask = new bool[probe.Columns.Count];
        MarkFilterColumns(probe.Predicates, probe.FilterMask);
        foreach (JoinStep step in steps.Where(step => step.SourceSide == probeIndex))
        {
            probe.FilterMask[step.SourceOrdinal] = true;
        }

        var plan = new SeedJoinPlan { Sides = sides, Steps = steps };
        return TryPrepareSeedJoinTables(engine, plan, _model.Database) ? plan : null;
    }

    /// <summary>
    /// Gives every step its table of joined rows. The projection of a seeded query is an expression, so a
    /// joined table keeps all its columns. A table joined on its clustered integer key is read by key on
    /// demand; any other is scanned and hashed once.
    /// </summary>
    private static bool TryPrepareSeedJoinTables(DataVoEngine engine, SeedJoinPlan plan, string databaseName)
    {
        foreach (JoinStep step in plan.Steps)
        {
            JoinSide side = plan.Sides[step.Side];
            side.KeepMask = new bool[side.Columns.Count];
            Array.Fill(side.KeepMask, true);
            side.KeyOnly = false;

            bool byKey = step.KeyOrdinal == side.ClusteredOrdinal && !side.ClusteredIsGuid;
            var build = new JoinBuildState(side, step, filter: !step.Outer, byKey ? 0 : HashCapacity(engine, side, databaseName));
            if (byKey)
            {
                step.Table.Loader = key => LoadSeedJoinRow(engine, plan, build, databaseName, key);
            }
            else if (!engine.StorageContext.ScanTableRows(side.Detail.TableName, databaseName, null, null, ref build, VisitJoinBuildRow))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>The row of a joined table whose clustered key is <paramref name="key"/>; null when there is none or it fails the table's conditions.</summary>
    private static CellValue[]? LoadSeedJoinRow(DataVoEngine engine, SeedJoinPlan plan, JoinBuildState build, string databaseName, long key)
    {
        var lookup = new SeedRowLookup(build);
        if (!engine.StorageContext.TryVisitRowByKey(build.Side.Detail.TableName, databaseName, RowKey.FromInt64(key), ref lookup, VisitSeedJoinRow, out _))
        {
            plan.Unreadable = true;
        }

        return lookup.Row;
    }

    private static bool VisitSeedJoinRow(ref SeedRowLookup lookup, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        JoinBuildState build = lookup.Build;
        LoadJoinCells(build.Side, rowId, rowBytes, typedCells, build.DecodeMask, build.Cells);
        if (!build.Filter || MatchesAll(build.Side.Predicates, build.Cells, build.Side.Columns))
        {
            lookup.Row = (CellValue[])build.Cells.Clone();
        }

        return true;
    }

    /// <summary>
    /// Joins the FROM-table rows <paramref name="seeds"/> along <paramref name="plan"/> and returns the joined
    /// rows that pass every condition, in seed order. Returns null when the rows cannot be read in stored form.
    /// </summary>
    private ListedTable? TryJoinSeeds(SeedJoinPlan plan, List<RowKey> seeds)
    {
        JoinSide probe = plan.Sides[0];
        var state = new JoinRunState
        {
            Sides = plan.Sides,
            Steps = plan.Steps,
            ProbeIndex = 0,
            Current = new CellValue[]?[plan.Sides.Length],
            TopLimit = int.MaxValue,
            Unordered = true,
        };
        state.Current[0] = new CellValue[probe.Columns.Count];

        bool visited = DataVoEngine.Current().StorageContext.VisitRowsByKey(probe.Detail.TableName, _model.Database!, seeds, ref state, VisitJoinProbeRow);
        return visited && !plan.Unreadable ? FinishJoinTop(state) : null;
    }
}
