using DataVo.Core.Indexing.Columnar;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Decides from a chunk group's zone maps (per column: min, max and NULL/NaN flags, all in the segment's
/// directory) whether a condition can accept any row of it, so a scan skips the group without reading it.
/// </summary>
/// <remarks>
/// Each step resolves to <see cref="NoneMatch"/>, <see cref="AllMatch"/> or <see cref="Mixed"/> and the verdicts
/// are joined in postfix order. A group's zones cover every row written to it, including rows a newer source
/// has since replaced; those only ever make the verdict less sure, never wrong.
/// </remarks>
internal static class SsTableColumnarZones
{
    /// <summary>No row of the group can meet the condition.</summary>
    internal const byte NoneMatch = 0;

    /// <summary>Every row of the group meets the condition.</summary>
    internal const byte AllMatch = 1;

    /// <summary>Some rows may meet it and some may not, or the zones cannot say.</summary>
    internal const byte Mixed = 2;

    /// <summary>Whether the condition provably accepts no row of chunk group <paramref name="chunk"/>.</summary>
    /// <param name="segment">The segment.</param>
    /// <param name="chunk">The chunk group.</param>
    /// <param name="steps">The bound condition in postfix order; an empty one accepts every row.</param>
    public static bool CanSkip(SsTableColumnarSegment segment, int chunk, ReadOnlySpan<ColumnarStep> steps) =>
        !steps.IsEmpty && Evaluate(segment, chunk, steps) == NoneMatch;

    /// <summary>The verdict for a whole condition; <see cref="Mixed"/> when it is malformed.</summary>
    internal static byte Evaluate(SsTableColumnarSegment segment, int chunk, ReadOnlySpan<ColumnarStep> steps)
    {
        Span<byte> stack = stackalloc byte[ColumnarScan.MaxSteps];
        int depth = 0;
        foreach (ColumnarStep step in steps)
        {
            if (step.Kind is ColumnarStep.And or ColumnarStep.Or)
            {
                if (depth < 2)
                {
                    return Mixed;
                }

                byte right = stack[--depth];
                byte left = stack[depth - 1];
                stack[depth - 1] = step.Kind == ColumnarStep.And ? And(left, right) : Or(left, right);
                continue;
            }

            if (depth >= stack.Length)
            {
                return Mixed;
            }

            stack[depth++] = Resolve(segment, chunk, in step);
        }

        return depth == 1 ? stack[0] : Mixed;
    }

    private static byte And(byte left, byte right) =>
        left == NoneMatch || right == NoneMatch ? NoneMatch : left == AllMatch && right == AllMatch ? AllMatch : Mixed;

    private static byte Or(byte left, byte right) =>
        left == AllMatch || right == AllMatch ? AllMatch : left == NoneMatch && right == NoneMatch ? NoneMatch : Mixed;

    private static byte Resolve(SsTableColumnarSegment segment, int chunk, in ColumnarStep step)
    {
        ref readonly SsTableColumnarColumn column = ref segment.Column(chunk, step.Column);
        bool allNull = (column.Flags & SsTableColumnarFormat.AllNull) != 0;
        bool anyNull = (column.Flags & SsTableColumnarFormat.AnyNull) != 0;

        if (step.Kind == ColumnarStep.NullTest)
        {
            // IS NULL is Equal, IS NOT NULL is NotEqual.
            bool wantNull = step.Test == ColumnarTest.Equal;
            if (allNull)
            {
                return wantNull ? AllMatch : NoneMatch;
            }

            return anyNull ? Mixed : wantNull ? NoneMatch : AllMatch;
        }

        if (allNull)
        {
            return step.NullsMatch ? AllMatch : NoneMatch;
        }

        byte values = ResolveValues(segment.Spec.KindOf(step.Column), in column, in step);
        if (!anyNull)
        {
            return values;
        }

        // The group also holds NULLs; whether they meet the comparison is the step's NullsMatch.
        return step.NullsMatch
            ? values == NoneMatch ? Mixed : values
            : values == AllMatch ? Mixed : values;
    }

    // The verdict for the group's non-NULL values; the zone is meaningless for a group with none.
    private static byte ResolveValues(StorageKind kind, in SsTableColumnarColumn column, in ColumnarStep step)
    {
        if (kind is StorageKind.Float or StorageKind.Double)
        {
            double value = step.ValueKind == 'f' ? BitConverter.Int32BitsToSingle((int)step.Bits) : BitConverter.Int64BitsToDouble(step.Bits);
            if (double.IsNaN(value))
            {
                return Mixed;
            }

            int low = BitConverter.Int64BitsToDouble(column.Min).CompareTo(value);
            int high = BitConverter.Int64BitsToDouble(column.Max).CompareTo(value);
            byte verdict = Verdict(step.Test, low, high);
            if ((column.Flags & SsTableColumnarFormat.AnyNaN) == 0)
            {
                return verdict;
            }

            // A NaN is neither above nor below anything and differs from everything: it meets only !=, and
            // it can never make a group all-matching. The zone leaves NaNs out, so only "none" survives.
            return step.Test != ColumnarTest.NotEqual && verdict == NoneMatch ? NoneMatch : Mixed;
        }

        return Verdict(step.Test, column.Min.CompareTo(step.Bits), column.Max.CompareTo(step.Bits));
    }

    // low and high are the sign of (min - value) and (max - value).
    private static byte Verdict(ColumnarTest test, int low, int high) => test switch
    {
        ColumnarTest.Equal => low > 0 || high < 0 ? NoneMatch : low == 0 && high == 0 ? AllMatch : Mixed,
        ColumnarTest.NotEqual => low > 0 || high < 0 ? AllMatch : low == 0 && high == 0 ? NoneMatch : Mixed,
        ColumnarTest.Less => high < 0 ? AllMatch : low >= 0 ? NoneMatch : Mixed,
        ColumnarTest.LessOrEqual => high <= 0 ? AllMatch : low > 0 ? NoneMatch : Mixed,
        ColumnarTest.Greater => low > 0 ? AllMatch : high <= 0 ? NoneMatch : Mixed,
        ColumnarTest.GreaterOrEqual => low >= 0 ? AllMatch : high < 0 ? NoneMatch : Mixed,
        _ => Mixed,
    };
}
