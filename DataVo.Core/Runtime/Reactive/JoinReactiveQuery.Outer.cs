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
    /// Recomputes a null-padded output identity from final state. <paramref name="leftPk"/> is the
    /// <see cref="LeftSentinel"/> for a right-padded row; <paramref name="rightPk"/> is the
    /// <see cref="RightSentinel"/> for a left-padded row.
    /// </summary>
    private IReadOnlyDictionary<string, object?>? ComputeOuterOutputRow(string leftPk, string rightPk)
    {
        // Left row padded with NULLs on the right side.
        if (string.Equals(rightPk, RightSentinel, StringComparison.Ordinal))
        {
            if (!EmitsLeftNullPad
                || !_leftRows.TryGetValue(leftPk, out IReadOnlyDictionary<string, object?>? leftRow)
                || HasRightMatch(leftRow))
            {
                return null;
            }

            return ProjectJoined(leftRow, rightRow: null);
        }

        // Right row padded with NULLs on the left side.
        if (string.Equals(leftPk, LeftSentinel, StringComparison.Ordinal))
        {
            if (!EmitsRightNullPad
                || !_rightRows.TryGetValue(rightPk, out IReadOnlyDictionary<string, object?>? rightRow)
                || HasLeftMatch(rightRow))
            {
                return null;
            }

            return ProjectJoined(leftRow: null, rightRow);
        }

        return null;
    }

    /// <summary>Adds the baseline null-padded rows (left/right rows with no match) without emitting.</summary>
    private void MaterializeOuterBaseline()
    {
        if (EmitsLeftNullPad)
        {
            foreach ((string leftPk, IReadOnlyDictionary<string, object?> leftRow) in _leftRows)
            {
                if (!HasRightMatch(leftRow))
                {
                    IReadOnlyDictionary<string, object?>? padded = ProjectJoined(leftRow, rightRow: null);
                    if (padded is not null)
                    {
                        _emitted[OutputIdentity(leftPk, RightSentinel)] = padded;
                    }
                }
            }
        }

        if (EmitsRightNullPad)
        {
            foreach ((string rightPk, IReadOnlyDictionary<string, object?> rightRow) in _rightRows)
            {
                if (!HasLeftMatch(rightRow))
                {
                    IReadOnlyDictionary<string, object?>? padded = ProjectJoined(leftRow: null, rightRow);
                    if (padded is not null)
                    {
                        _emitted[OutputIdentity(LeftSentinel, rightPk)] = padded;
                    }
                }
            }
        }
    }

    /// <summary>
    /// Collects the null-padded identities a change batch can affect: every changed left/right row, plus
    /// every opposite-side row sharing a changed row's join key (its match status may have flipped). All
    /// are recomputed from final state in <see cref="ComputeOuterOutputRow"/>.
    /// </summary>
    private void CollectOuterCandidates(
        List<SideDelta> leftDeltas,
        List<SideDelta> rightDeltas,
        HashSet<string> candidates)
    {
        if (EmitsLeftNullPad)
        {
            // A left row may flip its null-padded status when it changes...
            foreach (SideDelta delta in leftDeltas)
            {
                candidates.Add(OutputIdentity(delta.PrimaryKey, RightSentinel));
            }

            // ...or when a right row at its join key appears/disappears.
            foreach (SideDelta delta in rightDeltas)
            {
                string? key = JoinKey(delta.Row, leftSide: false);
                if (key is not null && _leftByKey.TryGetValue(key, out Dictionary<string, IReadOnlyDictionary<string, object?>>? bucket))
                {
                    foreach (string leftPk in bucket.Keys)
                    {
                        candidates.Add(OutputIdentity(leftPk, RightSentinel));
                    }
                }
            }
        }

        if (EmitsRightNullPad)
        {
            foreach (SideDelta delta in rightDeltas)
            {
                candidates.Add(OutputIdentity(LeftSentinel, delta.PrimaryKey));
            }

            foreach (SideDelta delta in leftDeltas)
            {
                string? key = JoinKey(delta.Row, leftSide: true);
                if (key is not null && _rightByKey.TryGetValue(key, out Dictionary<string, IReadOnlyDictionary<string, object?>>? bucket))
                {
                    foreach (string rightPk in bucket.Keys)
                    {
                        candidates.Add(OutputIdentity(LeftSentinel, rightPk));
                    }
                }
            }
        }
    }

    private bool HasRightMatch(IReadOnlyDictionary<string, object?> leftRow)
    {
        string? key = JoinKey(leftRow, leftSide: true);
        return key is not null
            && _rightByKey.TryGetValue(key, out Dictionary<string, IReadOnlyDictionary<string, object?>>? bucket)
            && bucket.Count > 0;
    }

    private bool HasLeftMatch(IReadOnlyDictionary<string, object?> rightRow)
    {
        string? key = JoinKey(rightRow, leftSide: false);
        return key is not null
            && _leftByKey.TryGetValue(key, out Dictionary<string, IReadOnlyDictionary<string, object?>>? bucket)
            && bucket.Count > 0;
    }
}
