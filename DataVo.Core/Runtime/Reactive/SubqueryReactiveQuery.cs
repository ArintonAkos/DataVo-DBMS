using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Incremental IN/EXISTS subquery operator implemented as a semi/anti-join.
/// </summary>
/// <remarks>
/// Two shapes are maintained behind the same routing. An <em>uncorrelated</em> subquery tracks a single
/// global membership index (a per-IN-value count, or a single EXISTS count). A <em>correlated</em>
/// subquery (<c>… WHERE S.k = R.k [AND &lt;inner pred&gt;]</c>) indexes the inner relation by the
/// correlation key, so a change to <c>S</c> re-evaluates only the outer rows whose correlation key was
/// affected. Rows are kept typed in each table's catalog layout and keyed by <see cref="PackedKey"/>; outer
/// rows are identified by their primary key, so the operator is robust to row-id reassignment.
/// </remarks>
internal sealed class SubqueryReactiveQuery : IBorrowedReactiveQuery
{
    private readonly SubqueryShape _shape;
    private readonly ReactivePredicate _innerPredicate;
    private readonly ReactiveRowLayout _outerLayout;
    private readonly ReactiveRowLayout _innerLayout;
    private readonly ReactiveProjection _projection;
    private readonly int[] _outerPrimaryKey;

    // Membership keys: the IN value, the EXISTS correlation value, or (correlation value, IN value).
    private readonly int[] _outerMembershipKey;
    private readonly int[] _innerMembershipKey;

    private readonly Dictionary<PackedKey, CellValue[]> _outerRows = [];
    private readonly Dictionary<PackedKey, HashSet<PackedKey>> _outerByMembership = [];
    private readonly Dictionary<PackedKey, int> _innerCounts = [];
    private readonly Dictionary<PackedKey, CellValue[]> _emitted = [];
    private int _innerExistsCount;
    private bool _seeded;

    // Reused per-batch scratch.
    private readonly HashSet<PackedKey> _candidates = [];
    private readonly HashSet<PackedKey> _touchedMembership = [];
    private readonly CellValue[] _outerImage;
    private readonly CellValue[] _innerImage;
    private readonly CellValue[] _row;

    /// <summary>
    /// Compiles a validated subquery shape.
    /// </summary>
    /// <param name="shape">The extracted IN/EXISTS shape.</param>
    /// <param name="engine">The owning engine (catalog access for both tables).</param>
    /// <param name="databaseName">The database that owns both tables.</param>
    /// <exception cref="NotSupportedException">The outer table has no primary key, or a column does not exist.</exception>
    public SubqueryReactiveQuery(SubqueryShape shape, DataVoEngine engine, string databaseName)
    {
        _shape = shape;
        _innerPredicate = ReactivePredicate.Compile(shape.InnerWhere);
        _outerLayout = ReactiveTableLayouts.ForTable(engine, shape.OuterTable, databaseName);
        _innerLayout = ReactiveTableLayouts.ForTable(engine, shape.InnerTable, databaseName);
        _projection = ReactiveTableLayouts.Project(shape.OuterColumns, _outerLayout, shape.OuterTable, shape.OuterAlias);

        List<string> primaryKeys = engine.Catalog.GetTablePrimaryKeys(shape.OuterTable, databaseName);
        if (primaryKeys.Count == 0)
        {
            throw new NotSupportedException(
                $"Reactive subqueries require a primary key on outer table '{shape.OuterTable}' to identify rows across updates.");
        }

        _outerPrimaryKey = primaryKeys.Select(OuterOrdinal).ToArray();
        _outerMembershipKey = MembershipKey(shape.OuterCorrelationColumn, IsExistsKind ? null : shape.OuterColumn, OuterOrdinal);
        _innerMembershipKey = MembershipKey(shape.InnerCorrelationColumn, IsExistsKind ? null : shape.InnerColumn, InnerOrdinal);

        _outerImage = new CellValue[_outerLayout.Count];
        _innerImage = new CellValue[_innerLayout.Count];
        _row = new CellValue[_projection.Ordinals.Length];
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [_shape.OuterTable, _shape.InnerTable];

    /// <inheritdoc />
    public ReactiveRowSchema OutputSchema => _projection.Schema;

    private bool IsCorrelated => _shape.IsCorrelated;

    private bool IsExistsKind => _shape.Kind is SubqueryKind.Exists or SubqueryKind.NotExists;

    // An uncorrelated EXISTS depends on one global count, so it keeps no per-row membership index.
    private bool IndexesOuterRows => IsCorrelated || !IsExistsKind;

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<TypedRow> rows)
    {
        if (IsOuter(table))
        {
            foreach (TypedRow row in rows)
            {
                AddOuter(_outerLayout.AdoptOrCopy(row));
            }
        }
        else if (IsInner(table))
        {
            foreach (TypedRow row in rows)
            {
                if (_innerLayout.TryLoad(row, _innerImage) && InnerMatches())
                {
                    BumpInner(+1);
                }
            }
        }
    }

    /// <inheritdoc />
    public void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder)
    {
        EnsureBaseline();
        _candidates.Clear();
        _touchedMembership.Clear();
        bool recheckAllOuter = false;

        for (int i = 0; i < changes.Count; i++)
        {
            RowChange change = changes[i];
            if (IsOuter(change.Table))
            {
                if (_outerLayout.TryLoadBefore(change, _outerImage))
                {
                    PackedKey pk = PackedKey.From(_outerImage, _outerPrimaryKey);
                    _candidates.Add(pk);
                    RemoveOuter(pk);
                }

                if (change.Kind != ChangeKind.Delete && change.TypedAfter is { } after)
                {
                    _candidates.Add(AddOuter(_outerLayout.AdoptOrCopy(after)));
                }

                continue;
            }

            if (!IsInner(change.Table))
            {
                continue;
            }

            if (_innerLayout.TryLoadBefore(change, _innerImage) && InnerMatches())
            {
                recheckAllOuter |= BumpInner(-1);
            }

            if (_innerLayout.TryLoadAfter(change, _innerImage) && InnerMatches())
            {
                recheckAllOuter |= BumpInner(+1);
            }
        }

        if (recheckAllOuter)
        {
            foreach (PackedKey pk in _outerRows.Keys)
            {
                _candidates.Add(pk);
            }
        }
        else
        {
            foreach (PackedKey key in _touchedMembership)
            {
                if (_outerByMembership.TryGetValue(key, out HashSet<PackedKey>? pks))
                {
                    _candidates.UnionWith(pks);
                }
            }
        }

        DiffInto(builder);
    }

    /// <summary>The first Apply records the qualifying outer rows as the baseline (never delivered).</summary>
    private void EnsureBaseline()
    {
        if (_seeded)
        {
            return;
        }

        _seeded = true;
        foreach (KeyValuePair<PackedKey, CellValue[]> outer in _outerRows)
        {
            if (Qualifies(outer.Value))
            {
                _emitted[outer.Key] = ProjectCopy(outer.Value);
            }
        }
    }

    private void DiffInto(QueryChangeBuilder builder)
    {
        foreach (PackedKey pk in _candidates)
        {
            bool exists = _outerRows.TryGetValue(pk, out CellValue[]? row) && Qualifies(row!);
            bool wasEmitted = _emitted.TryGetValue(pk, out CellValue[]? previous);
            if (!exists)
            {
                if (wasEmitted)
                {
                    _emitted.Remove(pk);
                    builder.AddRemovedRow(previous);
                }

                continue;
            }

            _projection.Project(row!, _row);
            if (!wasEmitted)
            {
                _emitted[pk] = _row.ToArray();
                builder.AddAddedRow(_row);
            }
            else if (!ReactiveCellOrdering.RowsEqual(previous, _row))
            {
                builder.AddUpdatedBeforeRow(previous);
                builder.AddUpdatedRow(_row);
                _emitted[pk] = _row.ToArray();
            }
        }
    }

    private PackedKey AddOuter(CellValue[] row)
    {
        PackedKey pk = PackedKey.From(row, _outerPrimaryKey);
        RemoveOuter(pk);
        _outerRows[pk] = row;
        if (IndexesOuterRows)
        {
            PackedKey key = PackedKey.From(row, _outerMembershipKey);
            if (!_outerByMembership.TryGetValue(key, out HashSet<PackedKey>? pks))
            {
                pks = [];
                _outerByMembership[key] = pks;
            }

            pks.Add(pk);
        }

        return pk;
    }

    private void RemoveOuter(PackedKey pk)
    {
        if (!_outerRows.Remove(pk, out CellValue[]? existing) || !IndexesOuterRows)
        {
            return;
        }

        PackedKey key = PackedKey.From(existing, _outerMembershipKey);
        if (_outerByMembership.TryGetValue(key, out HashSet<PackedKey>? pks))
        {
            pks.Remove(pk);
            if (pks.Count == 0)
            {
                _outerByMembership.Remove(key);
            }
        }
    }

    /// <summary>Applies one matching inner row (loaded in the inner image) with the given weight.</summary>
    /// <returns><see langword="true"/> when every outer row must be re-evaluated (an uncorrelated EXISTS).</returns>
    private bool BumpInner(int weight)
    {
        if (!IndexesOuterRows)
        {
            _innerExistsCount += weight;
            return true;
        }

        PackedKey key = PackedKey.From(_innerImage, _innerMembershipKey);
        int next = (_innerCounts.TryGetValue(key, out int count) ? count : 0) + weight;
        if (next <= 0)
        {
            _innerCounts.Remove(key);
        }
        else
        {
            _innerCounts[key] = next;
        }

        _touchedMembership.Add(key);
        return false;
    }

    private bool Qualifies(CellValue[] outerRow)
    {
        bool contains = IndexesOuterRows
            ? _innerCounts.ContainsKey(PackedKey.From(outerRow, _outerMembershipKey))
            : _innerExistsCount > 0;

        return _shape.Kind is SubqueryKind.NotIn or SubqueryKind.NotExists ? !contains : contains;
    }

    private bool InnerMatches() => _innerPredicate.Matches(new RowRef(_innerLayout.Schema, _innerImage));

    private CellValue[] ProjectCopy(CellValue[] outerRow)
    {
        _projection.Project(outerRow, _row);
        return _row.ToArray();
    }

    private int OuterOrdinal(string column) =>
        ReactiveTableLayouts.RequireOrdinal(_outerLayout, _shape.OuterTable, _shape.OuterAlias, null, column);

    private int InnerOrdinal(string column) =>
        ReactiveTableLayouts.RequireOrdinal(_innerLayout, _shape.InnerTable, _shape.InnerAlias, null, column);

    // The ordinals of the correlation column and/or the IN value column, in that order (empty for an
    // uncorrelated EXISTS, which never builds a membership key).
    private static int[] MembershipKey(string? correlationColumn, string? valueColumn, Func<string, int> ordinal)
    {
        var ordinals = new List<int>(2);
        if (correlationColumn is not null)
        {
            ordinals.Add(ordinal(correlationColumn));
        }

        if (!string.IsNullOrEmpty(valueColumn))
        {
            ordinals.Add(ordinal(valueColumn!));
        }

        return [.. ordinals];
    }

    private bool IsOuter(string table) => table.Equals(_shape.OuterTable, StringComparison.OrdinalIgnoreCase);

    private bool IsInner(string table) => table.Equals(_shape.InnerTable, StringComparison.OrdinalIgnoreCase);
}
