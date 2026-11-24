namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Outer-join (LEFT/RIGHT/FULL) null-padding extension.
/// </summary>
/// <remarks>
/// A left row with no surviving right match must appear once as <c>joined(left, null)</c>; a right row
/// with no surviving left match must appear once as <c>joined(null, right)</c>. Rather than maintain
/// explicit match counters with hand-written transition logic, the presence of each null-padded row is
/// <b>recomputed from the final arrangements</b> for exactly the identities a change batch can affect —
/// the same recompute-from-final-state model the INNER engine uses — so additions, retractions, and
/// value updates of null-padded rows fall out correctly (including join-key changes on either side).
/// </remarks>
internal sealed partial class JoinReactiveQuery
{
    private bool EmitsLeftNullPad => _shape.Kind is JoinKind.Left or JoinKind.Full;

    private bool EmitsRightNullPad => _shape.Kind is JoinKind.Right or JoinKind.Full;

    /// <summary>
    /// Recomputes a null-padded output identity from final state: a left row with no right match
    /// (<see cref="OutputKind.LeftPadded"/>) or a right row with no left match (<see cref="OutputKind.RightPadded"/>).
    /// </summary>
    private bool TryComputeOuterOutput(OutputId identity, out EmittedPair pair)
    {
        pair = default;
        if (identity.Kind == OutputKind.LeftPadded)
        {
            if (!EmitsLeftNullPad
                || !_left.Rows.TryGetValue(identity.Left, out SideRow? leftRow)
                || _right.HasMatchFor(leftRow))
            {
                return false;
            }

            pair = new EmittedPair(leftRow, null);
            return PassesWhere(pair);
        }

        if (!EmitsRightNullPad
            || !_right.Rows.TryGetValue(identity.Right, out SideRow? rightRow)
            || _left.HasMatchFor(rightRow))
        {
            return false;
        }

        pair = new EmittedPair(null, rightRow);
        return PassesWhere(pair);
    }

    /// <summary>Adds the baseline null-padded rows (left/right rows with no match) without emitting.</summary>
    private void MaterializeOuterBaseline()
    {
        if (EmitsLeftNullPad)
        {
            foreach ((PackedKey leftPk, SideRow leftRow) in _left.Rows)
            {
                var pair = new EmittedPair(leftRow, null);
                if (!_right.HasMatchFor(leftRow) && PassesWhere(pair))
                {
                    _emitted[LeftPadded(leftPk)] = pair;
                }
            }
        }

        if (EmitsRightNullPad)
        {
            foreach ((PackedKey rightPk, SideRow rightRow) in _right.Rows)
            {
                var pair = new EmittedPair(null, rightRow);
                if (!_left.HasMatchFor(rightRow) && PassesWhere(pair))
                {
                    _emitted[RightPadded(rightPk)] = pair;
                }
            }
        }
    }

    /// <summary>
    /// Collects the null-padded identities a change batch can affect: every changed left/right row, plus
    /// every opposite-side row sharing a changed row's join key (its match status may have flipped). All
    /// are recomputed from final state in <see cref="TryComputeOuterOutput"/>.
    /// </summary>
    private void CollectOuterCandidates(
        List<SideDelta> leftDeltas,
        List<SideDelta> rightDeltas,
        HashSet<OutputId> candidates)
    {
        if (EmitsLeftNullPad)
        {
            // A left row may flip its null-padded status when it changes...
            foreach (SideDelta delta in leftDeltas)
            {
                candidates.Add(LeftPadded(delta.Row.Pk));
            }

            // ...or when a right row at its join key appears/disappears.
            foreach (SideDelta delta in rightDeltas)
            {
                if (_left.TryGetBucket(delta.Row, out Dictionary<PackedKey, SideRow>? bucket))
                {
                    foreach (PackedKey leftPk in bucket.Keys)
                    {
                        candidates.Add(LeftPadded(leftPk));
                    }
                }
            }
        }

        if (EmitsRightNullPad)
        {
            foreach (SideDelta delta in rightDeltas)
            {
                candidates.Add(RightPadded(delta.Row.Pk));
            }

            foreach (SideDelta delta in leftDeltas)
            {
                if (_right.TryGetBucket(delta.Row, out Dictionary<PackedKey, SideRow>? bucket))
                {
                    foreach (PackedKey rightPk in bucket.Keys)
                    {
                        candidates.Add(RightPadded(rightPk));
                    }
                }
            }
        }
    }

    private static OutputId LeftPadded(PackedKey leftPk) => new(leftPk, default, OutputKind.LeftPadded);

    private static OutputId RightPadded(PackedKey rightPk) => new(default, rightPk, OutputKind.RightPadded);
}
