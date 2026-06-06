using System.Globalization;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// The incremental maintenance engine for <see cref="JoinReactiveQuery"/>: indexed arrangements, the
/// DBSP delta-join, and the output diff. INNER semantics live here; the outer-join null-padding
/// extension is layered on top in <c>JoinReactiveQuery.Outer.cs</c>.
/// </summary>
internal sealed partial class JoinReactiveQuery
{
    // Identities and composite keys are length-prefixed ("<len>:<text>" per component) so they are
    // collision-free for arbitrary value content without relying on a magic separator character.
    // Sentinels for the absent side of a null-padded outer-join output identity. They cannot collide
    // with a real primary-key rendering (which is either empty or a "v:"/null-marker join).
    private const string LeftSentinel = "L";
    private const string RightSentinel = "R";

    // joinKey -> (sidePrimaryKey -> row). Only non-null join keys are matchable and indexed here.
    private readonly Dictionary<string, Dictionary<string, IReadOnlyDictionary<string, object?>>> _leftByKey
        = new(StringComparer.Ordinal);

    private readonly Dictionary<string, Dictionary<string, IReadOnlyDictionary<string, object?>>> _rightByKey
        = new(StringComparer.Ordinal);

    // Full per-side row maps keyed by primary key, regardless of join-key nullability. Used to recompute
    // candidate outputs from final state and by the outer extension to enumerate unmatched rows.
    private readonly Dictionary<string, IReadOnlyDictionary<string, object?>> _leftRows
        = new(StringComparer.Ordinal);

    private readonly Dictionary<string, IReadOnlyDictionary<string, object?>> _rightRows
        = new(StringComparer.Ordinal);

    // outputIdentity (length-prefixed leftPk followed by rightPk) -> currently emitted projected row.
    private readonly Dictionary<string, IReadOnlyDictionary<string, object?>> _emitted
        = new(StringComparer.Ordinal);

    private bool _seeded;

    /// <summary>A single signed image delta for one side of the join.</summary>
    private readonly record struct SideDelta(string PrimaryKey, IReadOnlyDictionary<string, object?> Row, int Weight);

    private void SeedSide(string table, IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        bool isLeft = IsLeftTable(table);

        foreach ((long _, IReadOnlyDictionary<string, object?> row) in rows)
        {
            if (isLeft)
            {
                AddRow(_leftByKey, _leftRows, _leftPrimaryKeys, leftSide: true, row);
            }
            else
            {
                AddRow(_rightByKey, _rightRows, _rightPrimaryKeys, leftSide: false, row);
            }
        }
    }

    private QueryChange ApplyDelta(IReadOnlyList<RowChange> changes)
    {
        // The first Apply after both sides are seeded must materialize the baseline result so the diff
        // is taken against the correct starting point (the baseline itself is never delivered).
        EnsureBaseline();

        List<SideDelta> leftDeltas = [];
        List<SideDelta> rightDeltas = [];

        foreach (RowChange change in changes)
        {
            bool isLeft = IsLeftTable(change.Table);
            List<SideDelta> target = isLeft ? leftDeltas : rightDeltas;
            IReadOnlyList<string> primaryKeys = isLeft ? _leftPrimaryKeys : _rightPrimaryKeys;

            // Insert -> (After,+1); Delete -> (Before,-1); Update -> (Before,-1),(After,+1).
            if (change.Before is not null && change.Kind is ChangeKind.Delete or ChangeKind.Update)
            {
                target.Add(new SideDelta(Identity(primaryKeys, change.Before), change.Before, -1));
            }

            if (change.After is not null && change.Kind is ChangeKind.Insert or ChangeKind.Update)
            {
                target.Add(new SideDelta(Identity(primaryKeys, change.After), change.After, +1));
            }
        }

        var candidates = new HashSet<string>(StringComparer.Ordinal);

        // Probe left deltas against the OLD right arrangement (collect affected identities).
        CollectLeftCandidates(leftDeltas, candidates);

        // Apply left deltas to the left arrangement.
        ApplyDeltas(leftDeltas, _leftByKey, _leftRows, leftSide: true);

        // Probe right deltas against the NEW left arrangement (collect affected identities).
        CollectRightCandidates(rightDeltas, candidates);

        // Apply right deltas to the right arrangement.
        ApplyDeltas(rightDeltas, _rightByKey, _rightRows, leftSide: false);

        // Outer joins additionally re-evaluate null-padded rows for every touched side row.
        CollectOuterCandidates(leftDeltas, rightDeltas, candidates);

        return Diff(candidates);
    }

    /// <summary>
    /// Collects the inner-join output identities affected by the left-side deltas, probing against the
    /// right arrangement as it stands before the left deltas are applied. Both the before image (old
    /// match) and the after image (new match) of each changed left row are probed (they appear as two
    /// separate deltas), so removals and additions are both captured.
    /// </summary>
    private void CollectLeftCandidates(List<SideDelta> leftDeltas, HashSet<string> candidates)
    {
        foreach (SideDelta delta in leftDeltas)
        {
            string? key = JoinKey(delta.Row, leftSide: true);
            if (key is null || !_rightByKey.TryGetValue(key, out Dictionary<string, IReadOnlyDictionary<string, object?>>? bucket))
            {
                continue;
            }

            foreach (string rightPk in bucket.Keys)
            {
                candidates.Add(OutputIdentity(delta.PrimaryKey, rightPk));
            }
        }
    }

    /// <summary>
    /// Collects the inner-join output identities affected by the right-side deltas, probing against the
    /// left arrangement after the left deltas have been applied (the DBSP linearization).
    /// </summary>
    private void CollectRightCandidates(List<SideDelta> rightDeltas, HashSet<string> candidates)
    {
        foreach (SideDelta delta in rightDeltas)
        {
            string? key = JoinKey(delta.Row, leftSide: false);
            if (key is null || !_leftByKey.TryGetValue(key, out Dictionary<string, IReadOnlyDictionary<string, object?>>? bucket))
            {
                continue;
            }

            foreach (string leftPk in bucket.Keys)
            {
                candidates.Add(OutputIdentity(leftPk, delta.PrimaryKey));
            }
        }
    }

    /// <summary>
    /// Recomputes each candidate output identity from the final arrangements and diffs it against the
    /// currently emitted set. Because every identity is recomputed from final state, listing it more
    /// than once (the cross term) is harmless — there is no weight summation to double-count.
    /// </summary>
    private QueryChange Diff(HashSet<string> candidates)
    {
        List<IReadOnlyDictionary<string, object?>> added = [];
        List<IReadOnlyDictionary<string, object?>> removed = [];
        List<IReadOnlyDictionary<string, object?>> updated = [];
        List<IReadOnlyDictionary<string, object?>> updatedBefore = [];

        foreach (string identity in candidates)
        {
            IReadOnlyDictionary<string, object?>? current = ComputeOutputRow(identity);
            bool wasEmitted = _emitted.TryGetValue(identity, out IReadOnlyDictionary<string, object?>? previous);

            if (current is null)
            {
                if (wasEmitted)
                {
                    _emitted.Remove(identity);
                    removed.Add(previous!);
                }

                continue;
            }

            if (!wasEmitted)
            {
                _emitted[identity] = current;
                added.Add(current);
            }
            else if (!RowsEqual(previous!, current))
            {
                _emitted[identity] = current;
                updatedBefore.Add(previous!);
                updated.Add(current);
            }
        }

        return new QueryChange(added, removed, updated, updatedBefore);
    }

    /// <summary>
    /// Computes the projected output row for an output identity from the final arrangements, or
    /// <c>null</c> when the identity no longer joins or fails the WHERE predicate. Null-padded
    /// (outer-join) identities are delegated to the outer extension.
    /// </summary>
    private IReadOnlyDictionary<string, object?>? ComputeOutputRow(string identity)
    {
        (string leftPk, string rightPk) = SplitIdentity(identity);

        if (string.Equals(leftPk, LeftSentinel, StringComparison.Ordinal)
            || string.Equals(rightPk, RightSentinel, StringComparison.Ordinal))
        {
            return ComputeOuterOutputRow(leftPk, rightPk);
        }

        if (!_leftRows.TryGetValue(leftPk, out IReadOnlyDictionary<string, object?>? leftRow)
            || !_rightRows.TryGetValue(rightPk, out IReadOnlyDictionary<string, object?>? rightRow))
        {
            return null;
        }

        string? leftKey = JoinKey(leftRow, leftSide: true);
        string? rightKey = JoinKey(rightRow, leftSide: false);
        if (leftKey is null || rightKey is null || !leftKey.Equals(rightKey, StringComparison.Ordinal))
        {
            return null;
        }

        return ProjectJoined(leftRow, rightRow);
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
        foreach ((string key, Dictionary<string, IReadOnlyDictionary<string, object?>> leftBucket) in _leftByKey)
        {
            if (!_rightByKey.TryGetValue(key, out Dictionary<string, IReadOnlyDictionary<string, object?>>? rightBucket))
            {
                continue;
            }

            foreach ((string leftPk, IReadOnlyDictionary<string, object?> leftRow) in leftBucket)
            {
                foreach ((string rightPk, IReadOnlyDictionary<string, object?> rightRow) in rightBucket)
                {
                    IReadOnlyDictionary<string, object?>? projected = ProjectJoined(leftRow, rightRow);
                    if (projected is not null)
                    {
                        _emitted[OutputIdentity(leftPk, rightPk)] = projected;
                    }
                }
            }
        }

        MaterializeOuterBaseline();
    }

    private void AddRow(
        Dictionary<string, Dictionary<string, IReadOnlyDictionary<string, object?>>> byKey,
        Dictionary<string, IReadOnlyDictionary<string, object?>> rows,
        IReadOnlyList<string> primaryKeys,
        bool leftSide,
        IReadOnlyDictionary<string, object?> row)
    {
        string pk = Identity(primaryKeys, row);
        IReadOnlyDictionary<string, object?> copy = Copy(row);
        rows[pk] = copy;

        string? key = JoinKey(copy, leftSide);
        if (key is null)
        {
            return;
        }

        if (!byKey.TryGetValue(key, out Dictionary<string, IReadOnlyDictionary<string, object?>>? bucket))
        {
            bucket = new Dictionary<string, IReadOnlyDictionary<string, object?>>(StringComparer.Ordinal);
            byKey[key] = bucket;
        }

        bucket[pk] = copy;
    }

    private void ApplyDeltas(
        List<SideDelta> deltas,
        Dictionary<string, Dictionary<string, IReadOnlyDictionary<string, object?>>> byKey,
        Dictionary<string, IReadOnlyDictionary<string, object?>> rows,
        bool leftSide)
    {
        foreach (SideDelta delta in deltas)
        {
            string? key = JoinKey(delta.Row, leftSide);

            if (delta.Weight < 0)
            {
                RemoveFromArrangement(byKey, rows, delta.PrimaryKey, key);
            }
            else
            {
                IReadOnlyDictionary<string, object?> copy = Copy(delta.Row);
                rows[delta.PrimaryKey] = copy;

                if (key is not null)
                {
                    if (!byKey.TryGetValue(key, out Dictionary<string, IReadOnlyDictionary<string, object?>>? bucket))
                    {
                        bucket = new Dictionary<string, IReadOnlyDictionary<string, object?>>(StringComparer.Ordinal);
                        byKey[key] = bucket;
                    }

                    bucket[delta.PrimaryKey] = copy;
                }
            }
        }
    }

    private static void RemoveFromArrangement(
        Dictionary<string, Dictionary<string, IReadOnlyDictionary<string, object?>>> byKey,
        Dictionary<string, IReadOnlyDictionary<string, object?>> rows,
        string primaryKey,
        string? key)
    {
        rows.Remove(primaryKey);

        if (key is null || !byKey.TryGetValue(key, out Dictionary<string, IReadOnlyDictionary<string, object?>>? bucket))
        {
            return;
        }

        bucket.Remove(primaryKey);
        if (bucket.Count == 0)
        {
            byKey.Remove(key);
        }
    }

    private bool IsLeftTable(string table) =>
        table.Equals(_shape.LeftTable, StringComparison.OrdinalIgnoreCase);

    // Output identity = length-prefixed leftPk followed by rightPk, so the split point is unambiguous
    // for any pk content (including the "L"/"R" outer-join sentinels).
    private static string OutputIdentity(string leftPk, string rightPk) =>
        leftPk.Length.ToString(CultureInfo.InvariantCulture) + ":" + leftPk + rightPk;

    private static (string LeftPk, string RightPk) SplitIdentity(string identity)
    {
        int colon = identity.IndexOf(':');
        int leftLength = int.Parse(identity.AsSpan(0, colon), CultureInfo.InvariantCulture);
        int start = colon + 1;
        return (identity.Substring(start, leftLength), identity[(start + leftLength)..]);
    }

    private static string Identity(IReadOnlyList<string> primaryKeys, IReadOnlyDictionary<string, object?> row)
    {
        return Compose(primaryKeys.Select(column =>
        {
            object? value = row.TryGetValue(column, out object? v) ? v : null;
            return value is null ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
        }));
    }

    /// <summary>
    /// Computes the equi-join key for a row from the relevant side's join columns, or <c>null</c> when
    /// any join column is NULL (SQL equi-joins never match on NULL).
    /// </summary>
    private string? JoinKey(IReadOnlyDictionary<string, object?> row, bool leftSide)
    {
        var parts = new List<string?>(_shape.Keys.Count);
        foreach (JoinKeyColumn key in _shape.Keys)
        {
            string column = leftSide ? key.LeftColumn : key.RightColumn;
            object? value = row.TryGetValue(column, out object? v) ? v : null;
            if (value is null)
            {
                return null;
            }

            parts.Add(Convert.ToString(value, CultureInfo.InvariantCulture));
        }

        return Compose(parts);
    }

    /// <summary>
    /// Renders an ordered list of component strings as a single collision-free key by length-prefixing
    /// each component ("&lt;len&gt;:&lt;text&gt;"); a <c>null</c> component renders as "n".
    /// </summary>
    private static string Compose(IEnumerable<string?> components)
    {
        var builder = new System.Text.StringBuilder();
        foreach (string? component in components)
        {
            if (component is null)
            {
                builder.Append('n');
            }
            else
            {
                builder.Append('v')
                    .Append(component.Length.ToString(CultureInfo.InvariantCulture))
                    .Append(':')
                    .Append(component);
            }
        }

        return builder.ToString();
    }

    private static IReadOnlyDictionary<string, object?> Copy(IReadOnlyDictionary<string, object?> row) =>
        new Dictionary<string, object?>(
            row.ToDictionary(pair => pair.Key, pair => pair.Value),
            StringComparer.OrdinalIgnoreCase);

    private static bool RowsEqual(IReadOnlyDictionary<string, object?> a, IReadOnlyDictionary<string, object?> b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        foreach ((string key, object? value) in a)
        {
            if (!b.TryGetValue(key, out object? other))
            {
                return false;
            }

            if (ReactiveValueComparer.Instance.Compare(value, other) != 0)
            {
                return false;
            }
        }

        return true;
    }
}
