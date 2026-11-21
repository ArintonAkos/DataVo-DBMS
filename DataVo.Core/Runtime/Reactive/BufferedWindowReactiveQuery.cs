using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A reactive operator that processes filtered table changes directly and delivers its own notification
/// without allocating intermediate <see cref="QueryChange"/> collections or delta arenas.
/// </summary>
internal interface IDirectReactiveQuery : IReactiveQuery
{
    /// <summary>
    /// Applies a batch of committed row changes for this operator's observed table and invokes the
    /// consumer callback when the live result changed.
    /// </summary>
    /// <param name="tableChanges">The committed row changes targeting this operator's table.</param>
    void ApplyAndNotify(List<RowChange> tableChanges);
}

/// <summary>
/// Describes one sort column and direction for a <see cref="BufferedWindowReactiveQuery{TProjection, TProjector}"/>.
/// </summary>
/// <param name="Column">The column name to sort by.</param>
/// <param name="Ascending"><see langword="true"/> for ascending order; <see langword="false"/> for descending.</param>
internal readonly record struct BufferedWindowSortKey(string Column, bool Ascending);

/// <summary>
/// Maintains a live filtered and optionally ordered window of rows in a flat pre-allocated
/// <see cref="CellValue"/> slab, projecting the visible window directly into caller-owned
/// <see cref="Memory{T}"/> with zero steady-state managed heap allocations.
/// </summary>
/// <typeparam name="TProjection">The projected element type written into the destination buffer.</typeparam>
/// <typeparam name="TProjector">The value-type projector mapping each borrowed <see cref="RowRef"/> to <typeparamref name="TProjection"/>.</typeparam>
/// <remarks>
/// <para>
/// Matching rows are stored in contiguous fixed-width slots inside a flat <see cref="CellValue"/> slab
/// whose leading cells match the projector's column schema in exact ordinal order. Row identity is tracked
/// by primary-key <see cref="CellValue"/> without string formatting or boxing, and live ordering is kept in
/// a dense <see cref="int"/> slot-index array updated via binary search and span copies.
/// </para>
/// <para>
/// On each drain, only when the visible window <c>[Skip .. Skip + min(Take, destination.Length))</c>
/// actually changed does the operator project the live rows in place into <c>destination.Span[0..count]</c>
/// and invoke <c>onUpdated(count)</c>.
/// </para>
/// </remarks>
internal sealed class BufferedWindowReactiveQuery<TProjection, TProjector> : IDirectReactiveQuery
    where TProjector : struct, IReactiveRowProjector<TProjection>
{
    private const int InitialSlotCapacity = 32;

    private readonly string _table;
    private readonly ReactivePredicate? _predicate;
    private readonly Func<IReadOnlyDictionary<string, object?>, bool>? _fallbackPredicate;
    private readonly TProjector _projector;
    private readonly Memory<TProjection> _destination;
    private readonly Action<int> _onUpdated;
    private readonly int _skip;
    private readonly int _windowMax;

    private readonly ReactiveRowSchema _outputSchema;
    private readonly int _outputWidth;
    private readonly string[] _slotColumns;
    private readonly int _slotWidth;
    private readonly int _keySlotOffset;
    private readonly int[] _sortSlotOffsets;
    private readonly bool[] _sortAscending;

    private readonly Dictionary<CellValue, int> _slotByKey;
    private CellValue[] _rowSlab;
    private int _slotCapacity;
    private int _nextSlot;
    private int[] _freeSlots;
    private int _freeCount;

    private int[] _orderedSlots;
    private int _activeCount;

    private readonly CellValue[] _incomingScratch;
    private readonly CellValue[] _prevWindowCells;
    private int _prevWindowCount;

    private int[]? _cachedTypedOrdinals;

    /// <summary>
    /// Initializes a new zero-allocation buffered window reactive query.
    /// </summary>
    /// <param name="table">The canonical table name observed by this operator.</param>
    /// <param name="primaryKeyColumn">The primary-key column name used for stable row identity and tie-breaking.</param>
    /// <param name="outputColumns">The columns read by <paramref name="projector"/>, in ordinal order.</param>
    /// <param name="sortKeys">Zero or more ordering columns applied before window slicing.</param>
    /// <param name="skip">Number of leading rows to skip.</param>
    /// <param name="take">Maximum number of rows in the query window (<see cref="int.MaxValue"/> for unbounded).</param>
    /// <param name="predicate">Compiled SQL predicate, or <see langword="null"/> when using <paramref name="fallbackPredicate"/>.</param>
    /// <param name="fallbackPredicate">Optional C# predicate used when the filter was not compiled at build time.</param>
    /// <param name="projector">The value-type projector transforming borrowed rows into <typeparamref name="TProjection"/>.</param>
    /// <param name="destination">Caller-owned destination buffer receiving the live window.</param>
    /// <param name="onUpdated">Callback invoked with the live written count whenever the visible window changes.</param>
    public BufferedWindowReactiveQuery(
        string table,
        string primaryKeyColumn,
        string[] outputColumns,
        IReadOnlyList<BufferedWindowSortKey> sortKeys,
        int skip,
        int take,
        ReactivePredicate? predicate,
        Func<IReadOnlyDictionary<string, object?>, bool>? fallbackPredicate,
        TProjector projector,
        Memory<TProjection> destination,
        Action<int> onUpdated)
    {
        _table = table;
        _predicate = predicate;
        _fallbackPredicate = fallbackPredicate;
        _projector = projector;
        _destination = destination;
        _onUpdated = onUpdated;
        _skip = Math.Max(0, skip);
        _windowMax = Math.Min(Math.Max(0, take), destination.Length);

        _outputSchema = new ReactiveRowSchema(outputColumns);
        _outputWidth = outputColumns.Length;

        (_slotColumns, _keySlotOffset, _sortSlotOffsets, _sortAscending) =
            BuildSlotLayout(outputColumns, primaryKeyColumn, sortKeys);
        _slotWidth = _slotColumns.Length;

        _slotCapacity = InitialSlotCapacity;
        _slotByKey = new Dictionary<CellValue, int>(_slotCapacity, CellValueEqualityComparer.Instance);
        _rowSlab = new CellValue[_slotCapacity * _slotWidth];
        _freeSlots = new int[_slotCapacity];
        _orderedSlots = new int[_slotCapacity];
        _incomingScratch = new CellValue[_slotWidth];
        _prevWindowCells = new CellValue[_windowMax * _outputWidth];
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Tables => [_table];

    /// <inheritdoc />
    public void Seed(string table, IEnumerable<(RowKey RowId, IReadOnlyDictionary<string, object?> Row)> rows)
    {
        foreach ((RowKey _, IReadOnlyDictionary<string, object?> row) in rows)
        {
            if (MatchesDictionary(row))
            {
                FillScratchFromDictionary(row, _incomingScratch);
                UpsertSlot(_incomingScratch);
            }
        }

        SnapshotWindowBaseline();
    }

    /// <inheritdoc />
    public QueryChange Apply(IReadOnlyList<RowChange> changes)
    {
        throw new NotSupportedException("Buffered window subscriptions use direct zero-allocation dispatch.");
    }

    /// <inheritdoc />
    public void ApplyAndNotify(List<RowChange> tableChanges)
    {
        for (int i = 0; i < tableChanges.Count; i++)
        {
            ApplySingleChange(tableChanges[i]);
        }

        int count = ComputeWindowCount();
        if (!HasWindowChanged(count))
        {
            return;
        }

        RecordWindowSnapshot(count);
        WriteDestination(count);
        _onUpdated(count);
    }

    private void ApplySingleChange(RowChange change)
    {
        switch (change.Kind)
        {
            case ChangeKind.Insert:
                ApplyInsert(change);
                break;

            case ChangeKind.Delete:
                ApplyDelete(change);
                break;

            case ChangeKind.Update:
                ApplyUpdate(change);
                break;
        }
    }

    private void ApplyInsert(RowChange change)
    {
        if (change.TypedAfter is { } typed && _fallbackPredicate is null)
        {
            RowRef typedRow = typed.AsRowRef();
            if (_predicate is null || _predicate.Matches(typedRow))
            {
                FillScratchFromTyped(typedRow, _incomingScratch);
                UpsertSlot(_incomingScratch);
            }

            return;
        }

        if (change.After is { } after && MatchesDictionary(after))
        {
            FillScratchFromDictionary(after, _incomingScratch);
            UpsertSlot(_incomingScratch);
        }
    }

    private void ApplyDelete(RowChange change)
    {
        if (change.Before is not { } before)
        {
            return;
        }

        CellValue key = ReadDictionaryCell(before, _slotColumns[_keySlotOffset]);
        RemoveSlot(key);
    }

    private void ApplyUpdate(RowChange change)
    {
        bool beforeMatches = change.Before is not null && MatchesDictionary(change.Before);
        CellValue beforeKey = change.Before is not null
            ? ReadDictionaryCell(change.Before, _slotColumns[_keySlotOffset])
            : CellValue.Null;

        bool afterMatches;
        if (change.TypedAfter is { } typed && _fallbackPredicate is null)
        {
            RowRef typedRow = typed.AsRowRef();
            afterMatches = _predicate is null || _predicate.Matches(typedRow);
            if (afterMatches)
            {
                FillScratchFromTyped(typedRow, _incomingScratch);
            }
        }
        else if (change.After is { } after)
        {
            afterMatches = MatchesDictionary(after);
            if (afterMatches)
            {
                FillScratchFromDictionary(after, _incomingScratch);
            }
        }
        else
        {
            afterMatches = false;
        }

        if (beforeMatches && !afterMatches)
        {
            RemoveSlot(beforeKey);
            return;
        }

        if (!beforeMatches && afterMatches)
        {
            UpsertSlot(_incomingScratch);
            return;
        }

        if (beforeMatches && afterMatches)
        {
            CellValue afterKey = _incomingScratch[_keySlotOffset];
            if (!CellValueEqualityComparer.AreEqual(beforeKey, afterKey))
            {
                RemoveSlot(beforeKey);
            }

            UpsertSlot(_incomingScratch);
        }
    }

    private bool MatchesDictionary(IReadOnlyDictionary<string, object?> row)
    {
        if (_fallbackPredicate is not null)
        {
            return _fallbackPredicate(row);
        }

        return _predicate is null || _predicate.Matches(row);
    }

    private void FillScratchFromDictionary(IReadOnlyDictionary<string, object?> row, CellValue[] target)
    {
        for (int i = 0; i < _slotWidth; i++)
        {
            target[i] = ReadDictionaryCell(row, _slotColumns[i]);
        }
    }

    private void FillScratchFromTyped(in RowRef row, CellValue[] target)
    {
        int[] ordinals = ResolveTypedOrdinals(row);
        for (int i = 0; i < _slotWidth; i++)
        {
            int ordinal = ordinals[i];
            target[i] = ordinal >= 0 ? row[ordinal] : CellValue.Null;
        }
    }

    private int[] ResolveTypedOrdinals(in RowRef row)
    {
        // RowRef exposes Columns span; cache against the first TypedRow schema instance we see.
        // ReactiveRowSchema is created once per table schema and reused across typed inserts.
        ReadOnlySpan<string> columns = row.Columns;
        if (_cachedTypedOrdinals is not null && _cachedTypedOrdinals.Length == _slotWidth
            && MatchesColumns(columns, _cachedTypedOrdinals))
        {
            return _cachedTypedOrdinals;
        }

        int[] ordinals = new int[_slotWidth];
        for (int i = 0; i < _slotWidth; i++)
        {
            ordinals[i] = FindColumnIndex(columns, _slotColumns[i]);
        }

        _cachedTypedOrdinals = ordinals;
        return ordinals;
    }

    private bool MatchesColumns(ReadOnlySpan<string> columns, int[] ordinals)
    {
        for (int i = 0; i < ordinals.Length; i++)
        {
            int ordinal = ordinals[i];
            if (ordinal < 0 || ordinal >= columns.Length
                || !columns[ordinal].Equals(_slotColumns[i], StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
    }

    private static int FindColumnIndex(ReadOnlySpan<string> columns, string name)
    {
        for (int i = 0; i < columns.Length; i++)
        {
            if (columns[i].Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    private static CellValue ReadDictionaryCell(IReadOnlyDictionary<string, object?> row, string column) =>
        row.TryGetValue(column, out object? value) ? CellValue.FromBorrowedObject(value) : CellValue.Null;

    private void UpsertSlot(ReadOnlySpan<CellValue> incoming)
    {
        CellValue key = incoming[_keySlotOffset];
        if (_slotByKey.TryGetValue(key, out int existingSlot))
        {
            UpdateExistingSlot(existingSlot, incoming);
            return;
        }

        int newSlot = AllocateSlot();
        incoming.CopyTo(_rowSlab.AsSpan(newSlot * _slotWidth, _slotWidth));
        _slotByKey[key] = newSlot;

        int pos = FindInsertionPosition(newSlot);
        InsertOrderedSlot(pos, newSlot);
    }

    private void UpdateExistingSlot(int slot, ReadOnlySpan<CellValue> incoming)
    {
        int baseIndex = slot * _slotWidth;
        bool sortChanged = SortKeysChanged(baseIndex, incoming);
        if (!sortChanged)
        {
            incoming.CopyTo(_rowSlab.AsSpan(baseIndex, _slotWidth));
            return;
        }

        int oldPos = FindSlotPosition(slot);
        RemoveOrderedAt(oldPos);
        incoming.CopyTo(_rowSlab.AsSpan(baseIndex, _slotWidth));
        int newPos = FindInsertionPosition(slot);
        InsertOrderedSlot(newPos, slot);
    }

    private bool SortKeysChanged(int baseIndex, ReadOnlySpan<CellValue> incoming)
    {
        for (int i = 0; i < _sortSlotOffsets.Length; i++)
        {
            int col = _sortSlotOffsets[i];
            if (CellValueComparer.Compare(_rowSlab[baseIndex + col], incoming[col]) != 0)
            {
                return true;
            }
        }

        return false;
    }

    private void RemoveSlot(CellValue key)
    {
        if (!_slotByKey.TryGetValue(key, out int slot))
        {
            return;
        }

        int pos = FindSlotPosition(slot);
        RemoveOrderedAt(pos);
        _slotByKey.Remove(key);
        Array.Clear(_rowSlab, slot * _slotWidth, _slotWidth);
        _freeSlots[_freeCount++] = slot;
    }

    private int AllocateSlot()
    {
        if (_freeCount > 0)
        {
            return _freeSlots[--_freeCount];
        }

        if (_nextSlot == _slotCapacity)
        {
            GrowCapacity();
        }

        return _nextSlot++;
    }

    private void GrowCapacity()
    {
        int nextCapacity = _slotCapacity * 2;
        Array.Resize(ref _rowSlab, nextCapacity * _slotWidth);
        Array.Resize(ref _freeSlots, nextCapacity);
        Array.Resize(ref _orderedSlots, nextCapacity);
        _slotCapacity = nextCapacity;
    }

    private int FindSlotPosition(int slot)
    {
        int lo = 0;
        int hi = _activeCount - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int cmp = CompareSlots(_orderedSlots[mid], slot);
            if (cmp == 0)
            {
                return mid;
            }

            if (cmp < 0)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        throw new InvalidOperationException("Tracked slot was not found in the ordered index.");
    }

    private int FindInsertionPosition(int slot)
    {
        int lo = 0;
        int hi = _activeCount;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (CompareSlots(_orderedSlots[mid], slot) < 0)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return lo;
    }

    private void InsertOrderedSlot(int index, int slot)
    {
        if (_activeCount == _orderedSlots.Length)
        {
            Array.Resize(ref _orderedSlots, _orderedSlots.Length * 2);
        }

        if (index < _activeCount)
        {
            Array.Copy(_orderedSlots, index, _orderedSlots, index + 1, _activeCount - index);
        }

        _orderedSlots[index] = slot;
        _activeCount++;
    }

    private void RemoveOrderedAt(int index)
    {
        _activeCount--;
        if (index < _activeCount)
        {
            Array.Copy(_orderedSlots, index + 1, _orderedSlots, index, _activeCount - index);
        }
    }

    private int CompareSlots(int slotA, int slotB)
    {
        if (slotA == slotB)
        {
            return 0;
        }

        int baseA = slotA * _slotWidth;
        int baseB = slotB * _slotWidth;
        for (int i = 0; i < _sortSlotOffsets.Length; i++)
        {
            int col = _sortSlotOffsets[i];
            int cmp = CellValueComparer.Compare(_rowSlab[baseA + col], _rowSlab[baseB + col]);
            if (cmp != 0)
            {
                return _sortAscending[i] ? cmp : -cmp;
            }
        }

        return CellValueComparer.Compare(_rowSlab[baseA + _keySlotOffset], _rowSlab[baseB + _keySlotOffset]);
    }

    private int ComputeWindowCount()
    {
        if (_windowMax == 0 || _activeCount <= _skip)
        {
            return 0;
        }

        int available = _activeCount - _skip;
        return available < _windowMax ? available : _windowMax;
    }

    private bool HasWindowChanged(int count)
    {
        if (count != _prevWindowCount)
        {
            return true;
        }

        for (int i = 0; i < count; i++)
        {
            int slotBase = _orderedSlots[_skip + i] * _slotWidth;
            int prevBase = i * _outputWidth;
            for (int c = 0; c < _outputWidth; c++)
            {
                if (!CellValueEqualityComparer.AreEqual(_rowSlab[slotBase + c], _prevWindowCells[prevBase + c]))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private void SnapshotWindowBaseline()
    {
        int count = ComputeWindowCount();
        RecordWindowSnapshot(count);
    }

    private void RecordWindowSnapshot(int count)
    {
        int oldCount = _prevWindowCount;
        for (int i = 0; i < count; i++)
        {
            int slotBase = _orderedSlots[_skip + i] * _slotWidth;
            _rowSlab.AsSpan(slotBase, _outputWidth).CopyTo(_prevWindowCells.AsSpan(i * _outputWidth, _outputWidth));
        }

        if (oldCount > count)
        {
            Array.Clear(_prevWindowCells, count * _outputWidth, (oldCount - count) * _outputWidth);
        }

        _prevWindowCount = count;
    }

    private void WriteDestination(int count)
    {
        Span<TProjection> span = _destination.Span;
        for (int i = 0; i < count; i++)
        {
            int slotBase = _orderedSlots[_skip + i] * _slotWidth;
            var rowRef = new RowRef(_outputSchema, _rowSlab.AsSpan(slotBase, _outputWidth));
            span[i] = _projector.Project(in rowRef);
        }
    }

    private static (string[] Columns, int KeyOffset, int[] SortOffsets, bool[] SortAscending) BuildSlotLayout(
        string[] outputColumns,
        string primaryKeyColumn,
        IReadOnlyList<BufferedWindowSortKey> sortKeys)
    {
        var columns = new List<string>(outputColumns.Length + sortKeys.Count + 1);
        for (int i = 0; i < outputColumns.Length; i++)
        {
            columns.Add(outputColumns[i]);
        }

        int keyOffset = FindOrAddColumn(columns, primaryKeyColumn);
        int[] sortOffsets = new int[sortKeys.Count];
        bool[] sortAscending = new bool[sortKeys.Count];
        for (int i = 0; i < sortKeys.Count; i++)
        {
            sortOffsets[i] = FindOrAddColumn(columns, sortKeys[i].Column);
            sortAscending[i] = sortKeys[i].Ascending;
        }

        return (columns.ToArray(), keyOffset, sortOffsets, sortAscending);
    }

    private static int FindOrAddColumn(List<string> columns, string columnName)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i].Equals(columnName, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        columns.Add(columnName);
        return columns.Count - 1;
    }
}
