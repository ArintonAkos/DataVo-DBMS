namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Per-tuple multiplicities for the set and bag reactive operators (<c>DISTINCT</c>, <c>UNION</c>,
/// <c>UNION ALL</c>): signed deltas are accumulated per tuple during a batch and turned into output rows when
/// the batch is flushed.
/// </summary>
/// <remarks>
/// Tuples are keyed by all of their cells with SQL value equality (NULLs compare equal, numeric widths are
/// normalized), so equal projected rows from different sources fold together. In set mode a tuple is emitted
/// as <c>Added</c> on its <c>0 → &gt;0</c> transition and as <c>Removed</c> on <c>&gt;0 → 0</c>; in bag mode a
/// net delta of <c>±n</c> emits the tuple <c>n</c> times and no membership is kept between batches.
/// </remarks>
internal sealed class ReactiveTupleCounter
{
    private sealed class Tuple(CellValue[] row)
    {
        public readonly CellValue[] Row = row;
        public int Count;
        public int Pending;
        public bool Touched;
    }

    private readonly Dictionary<PackedKey, Tuple> _tuples = [];
    private readonly List<Tuple> _touched = [];
    private readonly int[] _ordinals;
    private readonly bool _isSet;

    /// <summary>Creates a counter for tuples of <paramref name="width"/> cells.</summary>
    /// <param name="width">The number of cells per tuple.</param>
    /// <param name="isSet"><see langword="true"/> for set semantics; <see langword="false"/> for bag semantics.</param>
    public ReactiveTupleCounter(int width, bool isSet)
    {
        _ordinals = Enumerable.Range(0, Math.Max(1, width)).ToArray();
        _isSet = isSet;
    }

    /// <summary>Counts one baseline occurrence of <paramref name="row"/> without emitting it.</summary>
    /// <param name="row">The tuple's cells.</param>
    public void Seed(ReadOnlySpan<CellValue> row)
    {
        if (_isSet)
        {
            GetOrCreate(row).Count++;
        }
    }

    /// <summary>Accumulates a signed delta for <paramref name="row"/> in the current batch.</summary>
    /// <param name="row">The tuple's cells.</param>
    /// <param name="weight">The signed multiplicity change.</param>
    public void Add(ReadOnlySpan<CellValue> row, int weight)
    {
        Tuple tuple = GetOrCreate(row);
        tuple.Pending += weight;
        if (!tuple.Touched)
        {
            tuple.Touched = true;
            _touched.Add(tuple);
        }
    }

    /// <summary>Emits the batch's net changes into <paramref name="builder"/> and resets the batch.</summary>
    /// <param name="builder">The builder receiving added and removed tuples.</param>
    public void FlushInto(QueryChangeBuilder builder)
    {
        foreach (Tuple tuple in _touched)
        {
            int previous = tuple.Count;
            int next = previous + tuple.Pending;
            if (_isSet)
            {
                if (previous <= 0 && next > 0)
                {
                    builder.AddAddedRow(tuple.Row);
                }
                else if (previous > 0 && next <= 0)
                {
                    builder.AddRemovedRow(tuple.Row);
                }
            }
            else
            {
                for (int i = 0; i < tuple.Pending; i++)
                {
                    builder.AddAddedRow(tuple.Row);
                }

                for (int i = 0; i < -tuple.Pending; i++)
                {
                    builder.AddRemovedRow(tuple.Row);
                }

                next = 0;
            }

            tuple.Count = next;
            tuple.Pending = 0;
            tuple.Touched = false;
            if (next <= 0)
            {
                _tuples.Remove(PackedKey.From(tuple.Row, _ordinals));
            }
        }

        _touched.Clear();
    }

    private Tuple GetOrCreate(ReadOnlySpan<CellValue> row)
    {
        PackedKey key = PackedKey.From(row, _ordinals);
        if (!_tuples.TryGetValue(key, out Tuple? tuple))
        {
            CellValue[] owned = row.ToArray();
            tuple = new Tuple(owned);
            _tuples[PackedKey.From(owned, _ordinals)] = tuple;
        }

        return tuple;
    }
}
