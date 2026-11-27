using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// The incremental maintenance engine for <see cref="JoinReactiveQuery"/>: indexed arrangements, the
/// DBSP delta-join, and the output diff. INNER semantics live here; the outer-join null-padding
/// extension is layered on top in <c>JoinReactiveQuery.Outer.cs</c>.
/// </summary>
/// <remarks>
/// Arrangements are typed: every side row is an immutable <see cref="SideRow"/> holding the table's catalog
/// columns as unboxed <see cref="CellValue"/> cells, keyed by <see cref="PackedKey"/> primary and join keys.
/// The delivered result is tracked as the pair of side rows that produced each output row rather than as a
/// projected copy, so an unchanged pair is recognized by reference and a changed one is projected straight
/// into the reused output scratch.
/// </remarks>
internal sealed partial class JoinReactiveQuery
{
    /// <summary>What an output identity stands for: a matched pair, or one side padded with NULLs.</summary>
    private enum OutputKind : byte
    {
        Inner = 0,
        LeftPadded = 1,
        RightPadded = 2,
    }

    /// <summary>The stable identity of one output row: both sides' primary keys (the absent one is default).</summary>
    private readonly record struct OutputId(PackedKey Left, PackedKey Right, OutputKind Kind);

    /// <summary>The side rows a delivered output row was projected from (either may be absent for a pad).</summary>
    private readonly record struct EmittedPair(SideRow? Left, SideRow? Right);

    /// <summary>A single signed image delta for one side of the join.</summary>
    private readonly record struct SideDelta(SideRow Row, int Weight);

    // outputIdentity -> the side rows the currently delivered row was projected from.
    private readonly Dictionary<OutputId, EmittedPair> _emitted = [];

    private bool _seeded;

    // Reused per-batch scratch so the dispatch hot path does not allocate fresh containers each Apply.
    private readonly List<SideDelta> _leftDeltas = [];
    private readonly List<SideDelta> _rightDeltas = [];
    private readonly HashSet<OutputId> _candidates = [];

    private void SeedSide(string table, IEnumerable<TypedRow> rows)
    {
        JoinSide side = IsLeftTable(table) ? _left : _right;

        foreach (TypedRow row in rows)
        {
            side.Store(side.LoadStored(row));
        }
    }

    public void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder)
    {
        // The first Apply after both sides are seeded must materialize the baseline result so the diff
        // is taken against the correct starting point (the baseline itself is never delivered).
        EnsureBaseline();

        _leftDeltas.Clear();
        _rightDeltas.Clear();

        // Index loop (not foreach) so iterating the IReadOnlyList does not allocate a boxed enumerator.
        for (int i = 0; i < changes.Count; i++)
        {
            CollectDeltas(changes[i]);
        }

        _candidates.Clear();

        // Probe left deltas against the OLD right arrangement (collect affected identities).
        CollectInnerCandidates(_leftDeltas, probed: _right, deltasAreLeft: true);

        // Apply left deltas to the left arrangement.
        ApplyDeltas(_leftDeltas, _left);

        // Probe right deltas against the NEW left arrangement (collect affected identities).
        CollectInnerCandidates(_rightDeltas, probed: _left, deltasAreLeft: false);

        // Apply right deltas to the right arrangement.
        ApplyDeltas(_rightDeltas, _right);

        // Outer joins additionally re-evaluate null-padded rows for every touched side row.
        CollectOuterCandidates(_leftDeltas, _rightDeltas, _candidates);

        DiffInto(_candidates, builder);
        ReleaseRetractedImages();
    }

    /// <summary>
    /// Expands one committed change into signed side deltas: Insert -> (After,+1); Delete -> (Before,-1);
    /// Update -> (Before,-1),(After,+1). Images are read typed, never as dictionaries.
    /// </summary>
    private void CollectDeltas(RowChange change)
    {
        bool isLeft = IsLeftTable(change.Table);
        JoinSide side = isLeft ? _left : _right;
        List<SideDelta> target = isLeft ? _leftDeltas : _rightDeltas;

        if (change.Kind != ChangeKind.Insert && change.TypedBefore is { } before)
        {
            target.Add(new SideDelta(side.LoadRetracted(before), -1));
        }

        if (change.Kind != ChangeKind.Delete && change.TypedAfter is { } after)
        {
            target.Add(new SideDelta(side.LoadStored(after), +1));
        }
    }

    /// <summary>
    /// Collects the inner-join output identities affected by one side's deltas, probing the opposite
    /// arrangement. Left deltas probe the right side before the left deltas apply; right deltas probe the
    /// left side after (the DBSP linearization). Both the before and after image of a changed row appear as
    /// separate deltas, so removals and additions are both captured.
    /// </summary>
    private void CollectInnerCandidates(List<SideDelta> deltas, JoinSide probed, bool deltasAreLeft)
    {
        foreach (SideDelta delta in deltas)
        {
            if (!probed.TryGetBucket(delta.Row, out Dictionary<PackedKey, SideRow>? bucket))
            {
                continue;
            }

            foreach (PackedKey otherPk in bucket.Keys)
            {
                _candidates.Add(deltasAreLeft
                    ? new OutputId(delta.Row.Pk, otherPk, OutputKind.Inner)
                    : new OutputId(otherPk, delta.Row.Pk, OutputKind.Inner));
            }
        }
    }

    /// <summary>
    /// Recomputes each candidate output identity from the final arrangements and diffs it against the
    /// currently emitted set. Because every identity is recomputed from final state, listing it more
    /// than once (the cross term) is harmless — there is no weight summation to double-count.
    /// </summary>
    private void DiffInto(HashSet<OutputId> candidates, QueryChangeBuilder builder)
    {
        foreach (OutputId identity in candidates)
        {
            bool exists = TryComputeOutput(identity, out EmittedPair current);
            bool wasEmitted = _emitted.TryGetValue(identity, out EmittedPair previous);

            if (!exists)
            {
                if (wasEmitted)
                {
                    _emitted.Remove(identity);
                    WriteRow(previous, _rowScratch);
                    builder.AddRemovedRow(_rowScratch);
                }

                continue;
            }

            if (!wasEmitted)
            {
                _emitted[identity] = current;
                WriteRow(current, _rowScratch);
                builder.AddAddedRow(_rowScratch);
            }
            else if (!ReferenceEquals(previous.Left, current.Left) || !ReferenceEquals(previous.Right, current.Right))
            {
                // A new image of either side may still project to the same values; compare before emitting.
                _emitted[identity] = current;
                WriteRow(previous, _previousScratch);
                WriteRow(current, _rowScratch);
                if (!ReactiveCellOrdering.RowsEqual(_previousScratch, _rowScratch))
                {
                    builder.AddUpdatedBeforeRow(_previousScratch);
                    builder.AddUpdatedRow(_rowScratch);
                }
            }
        }
    }

    /// <summary>
    /// Resolves an output identity against the final arrangements: the pair of side rows it projects from,
    /// or <see langword="false"/> when the identity no longer joins or fails the WHERE predicate. Null-padded
    /// (outer-join) identities are delegated to the outer extension.
    /// </summary>
    private bool TryComputeOutput(OutputId identity, out EmittedPair pair)
    {
        if (identity.Kind != OutputKind.Inner)
        {
            return TryComputeOuterOutput(identity, out pair);
        }

        pair = default;
        if (!_left.Rows.TryGetValue(identity.Left, out SideRow? leftRow)
            || !_right.Rows.TryGetValue(identity.Right, out SideRow? rightRow)
            || !leftRow.HasJoinKey
            || !rightRow.HasJoinKey
            || !leftRow.JoinKey.Equals(rightRow.JoinKey))
        {
            return false;
        }

        pair = new EmittedPair(leftRow, rightRow);
        return PassesWhere(pair);
    }

    private void EnsureBaseline()
    {
        if (_seeded)
        {
            return;
        }

        _seeded = true;
        MaterializeBaseline();
    }

    /// <summary>Materializes the current join result into <see cref="_emitted"/> without emitting it.</summary>
    private void MaterializeBaseline()
    {
        foreach ((PackedKey key, Dictionary<PackedKey, SideRow> leftBucket) in _left.ByKey)
        {
            if (!_right.ByKey.TryGetValue(key, out Dictionary<PackedKey, SideRow>? rightBucket))
            {
                continue;
            }

            foreach ((PackedKey leftPk, SideRow leftRow) in leftBucket)
            {
                foreach ((PackedKey rightPk, SideRow rightRow) in rightBucket)
                {
                    var pair = new EmittedPair(leftRow, rightRow);
                    if (PassesWhere(pair))
                    {
                        _emitted[new OutputId(leftPk, rightPk, OutputKind.Inner)] = pair;
                    }
                }
            }
        }

        MaterializeOuterBaseline();
    }

    /// <summary>Applies one side's signed deltas to its arrangement, in order.</summary>
    private static void ApplyDeltas(List<SideDelta> deltas, JoinSide side)
    {
        foreach (SideDelta delta in deltas)
        {
            if (delta.Weight < 0)
            {
                side.Remove(delta.Row);
            }
            else
            {
                side.Store(delta.Row);
            }
        }
    }

    /// <summary>Returns the retracted (before) images of this batch to their side's pool.</summary>
    private void ReleaseRetractedImages()
    {
        foreach (SideDelta delta in _leftDeltas)
        {
            if (delta.Weight < 0)
            {
                _left.Release(delta.Row);
            }
        }

        foreach (SideDelta delta in _rightDeltas)
        {
            if (delta.Weight < 0)
            {
                _right.Release(delta.Row);
            }
        }
    }

    private bool IsLeftTable(string table) =>
        table.Equals(_shape.LeftTable, StringComparison.OrdinalIgnoreCase);
}
