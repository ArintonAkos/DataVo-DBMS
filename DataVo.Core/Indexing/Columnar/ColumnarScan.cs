using DataVo.Core.Memory.Flat;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core.Indexing.Columnar;

/// <summary>One aggregate a columnar scan computes over the rows its condition accepts.</summary>
internal struct ColumnarFold
{
    /// <summary>The folded column's slot in the index.</summary>
    public int Slot;

    /// <summary>The operation and the column's type.</summary>
    public RowFoldKind Kind;
}

/// <summary>
/// Runs a condition and aggregates over a <see cref="ColumnarIndex"/>, a chunk of 4,096 rows at a time: each
/// comparison turns a run of one column into a bit mask, the masks are joined, and each aggregate reads only the
/// values the final mask selects. No row is decoded and nothing is allocated.
/// </summary>
internal static class ColumnarScan
{
    /// <summary>The most steps a condition may have.</summary>
    public const int MaxSteps = 32;

    /// <summary>The most aggregates one scan computes.</summary>
    public const int MaxFolds = 16;

    // The deepest the mask stack gets; a condition that needs more is left to the row path.
    private const int MaxDepth = 8;
    private const int Attempts = 4;

    /// <summary>A thread's buffers for describing and running a scan, kept between scans.</summary>
    internal sealed class Workspace
    {
        public readonly ColumnarStep[] Steps = new ColumnarStep[MaxSteps];
        public readonly ColumnarFold[] Folds = new ColumnarFold[MaxFolds];
        public readonly RowAggregate[] Totals = new RowAggregate[MaxFolds];
        public readonly ulong[] Masks = new ulong[(MaxDepth + 1) * Bitmask.WordsPerChunk];
    }

    [ThreadStatic]
    private static Workspace? t_workspace;

    /// <summary>This thread's workspace.</summary>
    public static Workspace Rent() => t_workspace ??= new Workspace();

    /// <summary>
    /// Points each step at its column's slot in the index and checks the index can run it.
    /// </summary>
    /// <returns>
    /// <see langword="false"/> when a column is not indexed, a value's type is not the column's, or the
    /// condition is malformed or too deep; the steps are then unusable.
    /// </returns>
    public static bool TryBind(ColumnarIndex index, Span<ColumnarStep> steps)
    {
        int depth = 0;
        for (int i = 0; i < steps.Length; i++)
        {
            ref ColumnarStep step = ref steps[i];
            if (step.Kind is ColumnarStep.And or ColumnarStep.Or)
            {
                if (--depth < 1)
                {
                    return false;
                }

                continue;
            }

            int slot = index.SlotOf(step.Column);
            if (slot < 0 || ++depth > MaxDepth)
            {
                return false;
            }

            if (step.Kind == ColumnarStep.Compare)
            {
                bool fits = (index.KindOf(slot), step.ValueKind) switch
                {
                    (StorageKind.Int, 'i') or (StorageKind.BigInt, 'l') or (StorageKind.DateTime, 'l') or (StorageKind.Float, 'f') or (StorageKind.Double, 'd') => true,
                    (StorageKind.Bit, 'b') => step.Test is ColumnarTest.Equal or ColumnarTest.NotEqual,
                    _ => false,
                };
                if (!fits)
                {
                    return false;
                }
            }

            step.Column = slot;
        }

        return steps.Length == 0 || depth == 1;
    }

    /// <summary>
    /// Counts the rows a bound condition accepts and computes each aggregate over them.
    /// </summary>
    /// <param name="index">The index to read.</param>
    /// <param name="steps">The condition, bound by <see cref="TryBind"/>; empty to accept every row.</param>
    /// <param name="folds">The aggregates.</param>
    /// <param name="totals">Receives one result per aggregate; its <see cref="RowAggregate.Count"/> is the number of matching rows.</param>
    /// <param name="limit">Stop once this many rows matched, or 0 to visit them all.</param>
    /// <param name="matched">The number of matching rows, at most <paramref name="limit"/> when one is given.</param>
    /// <returns>
    /// <see langword="false"/> when rows kept being removed while the scan ran, so no consistent result was
    /// reached; the caller then reads rows.
    /// </returns>
    /// <exception cref="OverflowException">An integer sum left the range of <see cref="long"/>.</exception>
    public static bool TryRun(ColumnarIndex index, ReadOnlySpan<ColumnarStep> steps, ReadOnlySpan<ColumnarFold> folds, Span<RowAggregate> totals, int limit, out int matched)
    {
        Workspace workspace = Rent();
        for (int attempt = 0; attempt < Attempts; attempt++)
        {
            long version = index.Version;
            if ((version & 1) != 0)
            {
                Thread.SpinWait(8); // a removal is in progress
                continue;
            }

            try
            {
                matched = Scan(index, steps, folds, totals, limit, workspace.Masks);
            }
            catch (OverflowException) when (index.Version != version)
            {
                continue; // the rows changed underneath; the sum was not of one consistent set
            }

            if (index.Version == version)
            {
                index.CountScan();
                return true;
            }
        }

        matched = 0;
        return false;
    }

    private static int Scan(ColumnarIndex index, ReadOnlySpan<ColumnarStep> steps, ReadOnlySpan<ColumnarFold> folds, Span<RowAggregate> totals, int limit, Span<ulong> masks)
    {
        totals.Slice(0, folds.Length).Clear();
        int count = index.Count;
        int matched = 0;
        for (int chunk = 0, first = 0; first < count; chunk++, first += FlatChunk.Slots)
        {
            int rows = Math.Min(FlatChunk.Slots, count - first);
            var chunks = new IndexChunks(index, chunk);
            matched += ScanChunk(ref chunks, steps, folds, totals, rows, index.Live(chunk), masks);
            if (limit > 0 && matched >= limit)
            {
                matched = limit;
                break;
            }
        }

        Finish(totals, folds.Length, matched);
        return matched;
    }

    /// <summary>Gives every aggregate the number of rows the scan matched, once its last chunk is folded.</summary>
    internal static void Finish(Span<RowAggregate> totals, int folds, int matched)
    {
        for (int i = 0; i < folds; i++)
        {
            totals[i].Count = matched;
        }
    }

    /// <summary>
    /// Runs a bound condition over one chunk and folds each aggregate over the rows it accepts.
    /// </summary>
    /// <param name="chunks">The chunk's columns.</param>
    /// <param name="steps">The condition; empty to accept every row.</param>
    /// <param name="folds">The aggregates.</param>
    /// <param name="totals">The running result of each aggregate, added to.</param>
    /// <param name="rows">The number of rows in the chunk.</param>
    /// <param name="live">A bit per row, set for the rows that count; empty when they all do.</param>
    /// <param name="masks">This thread's <see cref="Workspace.Masks"/>.</param>
    /// <returns>The number of rows that are live and accepted.</returns>
    /// <exception cref="OverflowException">An integer sum left the range of <see cref="long"/>.</exception>
    internal static int ScanChunk<TChunks>(
        ref TChunks chunks,
        ReadOnlySpan<ColumnarStep> steps,
        ReadOnlySpan<ColumnarFold> folds,
        Span<RowAggregate> totals,
        int rows,
        ReadOnlySpan<ulong> live,
        Span<ulong> masks)
        where TChunks : struct, IColumnarChunks
    {
        int words = Bitmask.WordsFor(rows);
        Span<ulong> mask = Evaluate(ref chunks, steps, words, masks);
        if (!live.IsEmpty)
        {
            Bitmask.And(mask, live.Slice(0, words));
        }

        Bitmask.TrimTo(mask, rows); // positions past the rows this scan reads
        int hits = Bitmask.Count(mask);
        if (hits == 0)
        {
            return 0;
        }

        Span<ulong> scratch = masks.Slice(MaxDepth * Bitmask.WordsPerChunk, Bitmask.WordsPerChunk);
        for (int i = 0; i < folds.Length; i++)
        {
            ColumnarFold fold = folds[i];
            ReadOnlySpan<ulong> selected = mask;
            int values = hits;
            if (chunks.AnyNull(fold.Slot))
            {
                Span<ulong> present = scratch.Slice(0, words);
                mask.CopyTo(present);
                Bitmask.AndNot(present, chunks.Nulls(fold.Slot).Slice(0, words));
                selected = present;
                values = Bitmask.Count(present);
            }

            if (values > 0)
            {
                Fold(ref totals[i], fold.Kind, ref chunks, fold.Slot, selected, values);
            }
        }

        return hits;
    }

    /// <summary>Evaluates the condition for one chunk and returns the mask of the rows it accepts.</summary>
    private static Span<ulong> Evaluate<TChunks>(ref TChunks chunks, ReadOnlySpan<ColumnarStep> steps, int words, Span<ulong> masks)
        where TChunks : struct, IColumnarChunks
    {
        if (steps.Length == 0)
        {
            Span<ulong> all = masks.Slice(0, words);
            all.Fill(ulong.MaxValue);
            return all;
        }

        int depth = 0;
        for (int i = 0; i < steps.Length; i++)
        {
            ColumnarStep step = steps[i];
            if (step.Kind is ColumnarStep.And or ColumnarStep.Or)
            {
                depth--;
                Span<ulong> left = masks.Slice((depth - 1) * Bitmask.WordsPerChunk, words);
                ReadOnlySpan<ulong> right = masks.Slice(depth * Bitmask.WordsPerChunk, words);
                if (step.Kind == ColumnarStep.And)
                {
                    Bitmask.And(left, right);
                }
                else
                {
                    Bitmask.Or(left, right);
                }

                continue;
            }

            Span<ulong> target = masks.Slice(depth++ * Bitmask.WordsPerChunk, words);
            bool anyNull = chunks.AnyNull(step.Column);
            if (step.Kind == ColumnarStep.NullTest)
            {
                if (anyNull)
                {
                    chunks.Nulls(step.Column).Slice(0, words).CopyTo(target);
                }
                else
                {
                    target.Clear();
                }

                if (step.Test != ColumnarTest.Equal)
                {
                    Bitmask.Not(target);
                }

                continue;
            }

            Compare(ref chunks, in step, target);
            if (anyNull)
            {
                ReadOnlySpan<ulong> nulls = chunks.Nulls(step.Column).Slice(0, words);
                if (step.NullsMatch)
                {
                    Bitmask.Or(target, nulls);
                }
                else
                {
                    Bitmask.AndNot(target, nulls);
                }
            }
        }

        return masks.Slice(0, words);
    }

    private static void Compare<TChunks>(ref TChunks chunks, in ColumnarStep step, Span<ulong> target)
        where TChunks : struct, IColumnarChunks
    {
        var op = (ColumnarCompare)step.Test; // the two enums list the comparisons in the same order
        switch (step.ValueKind)
        {
            case 'i':
                ColumnarKernels.Compare(chunks.Values<int>(step.Column), op, (int)step.Bits, target);
                break;
            case 'l':
                ColumnarKernels.Compare(chunks.Values<long>(step.Column), op, step.Bits, target);
                break;
            case 'f':
                ColumnarKernels.Compare(chunks.Values<float>(step.Column), op, BitConverter.Int32BitsToSingle((int)step.Bits), target);
                break;
            case 'd':
                ColumnarKernels.Compare(chunks.Values<double>(step.Column), op, BitConverter.Int64BitsToDouble(step.Bits), target);
                break;
            default:
                // BIT: the stored bits are the rows that hold true.
                chunks.Bits(step.Column).Slice(0, target.Length).CopyTo(target);
                if ((step.Test == ColumnarTest.Equal) != (step.Bits != 0))
                {
                    Bitmask.Not(target);
                }

                break;
        }
    }

    /// <summary>
    /// Folds one chunk's selected values into a running result, to the same answer the row path reaches by
    /// folding them one at a time.
    /// </summary>
    private static void Fold<TChunks>(ref RowAggregate total, RowFoldKind kind, ref TChunks chunks, int slot, ReadOnlySpan<ulong> selected, int values)
        where TChunks : struct, IColumnarChunks
    {
        switch (kind)
        {
            case RowFoldKind.SumInt32:
                total.Whole = checked(total.Whole + ColumnarKernels.SumInt32(chunks.Values<int>(slot), selected));
                break;
            case RowFoldKind.SumInt64:
                ColumnarKernels.SumInt64(chunks.Values<long>(slot), selected, ref total.Whole);
                break;
            case RowFoldKind.SumFloat:
                ColumnarKernels.SumSingle(chunks.Values<float>(slot), selected, ref total.Real);
                break;
            case RowFoldKind.SumDouble:
                ColumnarKernels.SumDouble(chunks.Values<double>(slot), selected, ref total.Real);
                break;
            case RowFoldKind.MinInt32:
            case RowFoldKind.MinDate:
            {
                ColumnarKernels.Extreme(chunks.Values<int>(slot), selected, true, out int best);
                total.Whole = total.Values == 0 ? best : Math.Min(total.Whole, best);
                break;
            }

            case RowFoldKind.MaxInt32:
            case RowFoldKind.MaxDate:
            {
                ColumnarKernels.Extreme(chunks.Values<int>(slot), selected, false, out int best);
                total.Whole = total.Values == 0 ? best : Math.Max(total.Whole, best);
                break;
            }

            case RowFoldKind.MinInt64:
            {
                ColumnarKernels.Extreme(chunks.Values<long>(slot), selected, true, out long best);
                total.Whole = total.Values == 0 ? best : Math.Min(total.Whole, best);
                break;
            }

            case RowFoldKind.MaxInt64:
            {
                ColumnarKernels.Extreme(chunks.Values<long>(slot), selected, false, out long best);
                total.Whole = total.Values == 0 ? best : Math.Max(total.Whole, best);
                break;
            }

            case RowFoldKind.MinFloat:
            case RowFoldKind.MaxFloat:
            {
                bool smallest = kind == RowFoldKind.MinFloat;
                ColumnarKernels.Extreme(chunks.Values<float>(slot), selected, smallest, out float best, out bool anyNumber, out bool anyNaN);
                Combine(ref total, smallest, best, anyNumber, anyNaN);
                break;
            }

            case RowFoldKind.MinDouble:
            case RowFoldKind.MaxDouble:
            {
                bool smallest = kind == RowFoldKind.MinDouble;
                ColumnarKernels.Extreme(chunks.Values<double>(slot), selected, smallest, out double best, out bool anyNumber, out bool anyNaN);
                Combine(ref total, smallest, best, anyNumber, anyNaN);
                break;
            }
        }

        total.Values += values;
    }

    // Enumerable.Min and Max order NaN below every number. So a minimum is NaN once any value is, and a
    // maximum is NaN only when every value is. RowAggregate reaches the same results one value at a time.
    private static void Combine(ref RowAggregate total, bool smallest, double best, bool anyNumber, bool anyNaN)
    {
        bool first = total.Values == 0;
        if (smallest)
        {
            if (anyNaN)
            {
                total.Real = double.NaN;
            }
            else if (first || (!double.IsNaN(total.Real) && best < total.Real))
            {
                total.Real = best;
            }
        }
        else if (anyNumber)
        {
            if (first || double.IsNaN(total.Real) || best > total.Real)
            {
                total.Real = best;
            }
        }
        else if (first)
        {
            total.Real = double.NaN;
        }
    }
}

/// <summary>
/// The columns of one chunk of up to 4,096 rows, as <see cref="ColumnarScan"/> reads them: from a
/// <see cref="ColumnarIndex"/> in memory or from the columnar segment of an SSTable. Implemented by structs, so
/// the scan is compiled for each source without a call through the interface.
/// </summary>
internal interface IColumnarChunks
{
    /// <summary>Whether the column may hold a NULL in this chunk; only then is <see cref="Nulls"/> read.</summary>
    bool AnyNull(int slot);

    /// <summary>The column's values, a whole number of 64-value runs. <typeparamref name="T"/> must be the column's value type.</summary>
    ReadOnlySpan<T> Values<T>(int slot)
        where T : unmanaged;

    /// <summary>A BIT column's values, a bit per row.</summary>
    ReadOnlySpan<ulong> Bits(int slot);

    /// <summary>The column's NULL mask, a bit per row.</summary>
    ReadOnlySpan<ulong> Nulls(int slot);
}

/// <summary>One chunk of a <see cref="ColumnarIndex"/>.</summary>
internal readonly struct IndexChunks(ColumnarIndex index, int chunk) : IColumnarChunks
{
    /// <inheritdoc />
    public bool AnyNull(int slot) => index.AnyNull(slot);

    /// <inheritdoc />
    public ReadOnlySpan<T> Values<T>(int slot)
        where T : unmanaged => index.Values<T>(slot, chunk);

    /// <inheritdoc />
    public ReadOnlySpan<ulong> Bits(int slot) => index.Bits(slot, chunk);

    /// <inheritdoc />
    public ReadOnlySpan<ulong> Nulls(int slot) => index.Nulls(slot, chunk);
}
