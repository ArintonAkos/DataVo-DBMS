using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A two-branch incremental <c>UNION</c> / <c>UNION ALL</c> operator over single-table SELECT branches (L4).
/// </summary>
/// <remarks>
/// In the Z-set algebra union is <b>addition</b> of the branch relations. Each branch is a linear
/// <see cref="ReactiveSubscription"/> that builds its borrowed delta into its own arena; the branch rows are
/// matched to the union's columns by position and folded into one signed delta per tuple in a
/// <see cref="ReactiveTupleCounter"/>. <c>UNION ALL</c> passes the net multiplicity through (bag semantics);
/// <c>UNION</c> keeps per-tuple counts and emits only the <c>0 ↔ positive</c> transitions, so a tuple produced
/// by both branches appears once.
/// </remarks>
internal sealed class UnionReactiveQuery : IBorrowedReactiveQuery
{
    private readonly Branch _left;
    private readonly Branch _right;
    private readonly bool _isAll;
    private readonly ReactiveTupleCounter _tuples;
    private readonly CellValue[] _row;

    /// <summary>
    /// Compiles the supplied two-branch UNION statement.
    /// </summary>
    /// <param name="union">The parsed two-branch UNION / UNION ALL statement.</param>
    /// <param name="engine">The owning engine (catalog access for the branch tables).</param>
    /// <param name="databaseName">The database that owns the branch tables.</param>
    /// <exception cref="NotSupportedException">An unsupported UNION shape (more than two branches, final ORDER BY/LIMIT, mismatched branch arity, wildcard/computed branch projections).</exception>
    public UnionReactiveQuery(UnionSelectStatement union, DataVoEngine engine, string databaseName)
    {
        Validate(union);

        _isAll = union.Branches[0].IsAll;
        _left = new Branch(union.Left, engine, databaseName);
        _right = new Branch(union.Branches[0].Select, engine, databaseName);
        if (_right.Query.OutputSchema.ColumnCount != _left.Query.OutputSchema.ColumnCount)
        {
            throw new NotSupportedException("Reactive UNION branches must project the same number of columns.");
        }

        OutputSchema = _left.Query.OutputSchema;
        _tuples = new ReactiveTupleCounter(OutputSchema.ColumnCount, isSet: !_isAll);
        _row = new CellValue[OutputSchema.ColumnCount];
        Tables = new[] { _left.Query.Table, _right.Query.Table }
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables { get; }

    /// <inheritdoc />
    public ReactiveRowSchema OutputSchema { get; }

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<TypedRow> rows)
    {
        // Bag semantics keep no membership; set semantics count the baseline tuples of each observing branch.
        if (_isAll)
        {
            return;
        }

        foreach (Branch branch in new[] { _left, _right })
        {
            if (!branch.Observes(table))
            {
                continue;
            }

            foreach (TypedRow row in rows)
            {
                if (branch.Query.TryProject(row, _row))
                {
                    _tuples.Seed(_row);
                }
            }
        }
    }

    /// <inheritdoc />
    public void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder)
    {
        _left.Collect(changes, _tuples);
        _right.Collect(changes, _tuples);
        _tuples.FlushInto(builder);
    }

    private static void Validate(UnionSelectStatement union)
    {
        if (union.Branches.Count != 1)
        {
            throw new NotSupportedException("Reactive UNION supports exactly two SELECT branches.");
        }

        if (union.OrderByExpression is not null || union.LimitExpression is not null)
        {
            throw new NotSupportedException("Reactive UNION does not support final ORDER BY / LIMIT.");
        }
    }

    private static void ValidateBranch(SelectStatement select)
    {
        if (select.IsDistinct)
        {
            throw new NotSupportedException("Reactive UNION branches do not support DISTINCT.");
        }

        foreach (SelectColumnNode column in select.Columns)
        {
            if (ReactiveTableLayouts.IsStarColumn(column))
            {
                throw new NotSupportedException("Reactive UNION branches do not support wildcard projection.");
            }
        }
    }

    /// <summary>One UNION branch: its linear operator plus the reused arena and change list it runs on.</summary>
    private sealed class Branch
    {
        private readonly QueryChangeBuilder _builder;
        private readonly List<RowChange> _changes = [];

        public Branch(SelectStatement select, DataVoEngine engine, string databaseName)
        {
            ValidateBranch(select);
            Query = new ReactiveSubscription(select, engine, databaseName);
            _builder = new QueryChangeBuilder(Query.OutputSchema);
        }

        public ReactiveSubscription Query { get; }

        public bool Observes(string table) => Query.Table.Equals(table, StringComparison.OrdinalIgnoreCase);

        /// <summary>Runs the branch over its share of <paramref name="changes"/> and folds its delta into <paramref name="tuples"/>.</summary>
        public void Collect(IReadOnlyList<RowChange> changes, ReactiveTupleCounter tuples)
        {
            _changes.Clear();
            for (int i = 0; i < changes.Count; i++)
            {
                if (Observes(changes[i].Table))
                {
                    _changes.Add(changes[i]);
                }
            }

            if (_changes.Count == 0)
            {
                return;
            }

            _builder.Reset();
            Query.ApplyInto(_changes, _builder);
            QueryChangeRef delta = _builder.Build();
            AddRows(delta.Added, tuples, +1);
            AddRows(delta.Removed, tuples, -1);
            AddRows(delta.UpdatedBefore, tuples, -1);
            AddRows(delta.Updated, tuples, +1);
        }

        private static void AddRows(RowSet rows, ReactiveTupleCounter tuples, int weight)
        {
            for (int i = 0; i < rows.Count; i++)
            {
                tuples.Add(rows[i].Values, weight);
            }
        }
    }
}
