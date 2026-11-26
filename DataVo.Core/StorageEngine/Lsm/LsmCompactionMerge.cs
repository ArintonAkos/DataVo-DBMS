using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// One input of a compaction: a file's entries in internal-key order (user key ascending, then sequence
/// descending), read one at a time. The key and value are valid until the next <see cref="MoveNext"/>.
/// </summary>
internal abstract class LsmMergeSource(int ordinal) : IDisposable
{
    /// <summary>The input's place among the compaction's inputs; breaks ties between equal internal keys.</summary>
    public int Ordinal { get; } = ordinal;

    /// <summary>The current entry's internal key.</summary>
    public abstract ReadOnlySpan<byte> InternalKey { get; }

    /// <summary>The current entry's value (empty for a deletion).</summary>
    public abstract ReadOnlySpan<byte> Value { get; }

    /// <summary>Moves to the next entry (the first, on the first call); false when there is none.</summary>
    public abstract bool MoveNext();

    /// <summary>Releases what the input reads from.</summary>
    public virtual void Dispose()
    {
    }
}

/// <summary>The rows of a row-keyed (v6 or v8) SSTable, read through a cursor over its blocks.</summary>
internal sealed class LsmRowFileMergeSource(int ordinal, SsTableV2Reader reader, bool ownsReader) : LsmMergeSource(ordinal)
{
    private readonly SsTableV2Cursor _cursor = reader.OpenCursor();
    private readonly byte[] _key = new byte[LsmRowKeyCodec.RowKeySize + Lsm.InternalKey.TagSize];

    /// <inheritdoc />
    public override ReadOnlySpan<byte> InternalKey => _key;

    /// <inheritdoc />
    public override ReadOnlySpan<byte> Value => _cursor.Type == LsmValueType.Put ? _cursor.Value : default;

    /// <inheritdoc />
    public override bool MoveNext()
    {
        if (!_cursor.MoveNext())
        {
            return false;
        }

        _cursor.Key.WriteTo(_key);
        Lsm.InternalKey.Write(_key, _key.AsSpan(0, LsmRowKeyCodec.RowKeySize), _cursor.Sequence, _cursor.Type);
        return true;
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        _cursor.Release();
        if (ownsReader)
        {
            reader.Dispose();
        }
    }
}

/// <summary>The entries of a keyed (v3) SSTable, read through a cursor over its blocks.</summary>
internal sealed class LsmKeyedFileMergeSource(int ordinal, SsTableKeyedReader reader, bool ownsReader) : LsmMergeSource(ordinal)
{
    private readonly SsTableKeyedCursor _cursor = reader.OpenCursor();
    private byte[] _key = new byte[64];
    private int _length;

    /// <inheritdoc />
    public override ReadOnlySpan<byte> InternalKey => _key.AsSpan(0, _length);

    /// <inheritdoc />
    public override ReadOnlySpan<byte> Value => _cursor.Value;

    /// <inheritdoc />
    public override bool MoveNext()
    {
        if (!_cursor.MoveNext())
        {
            return false;
        }

        ReadOnlySpan<byte> userKey = _cursor.UserKey;
        _length = userKey.Length + Lsm.InternalKey.TagSize;
        if (_key.Length < _length)
        {
            _key = new byte[Math.Max(_length, _key.Length * 2)];
        }

        Lsm.InternalKey.Write(_key, userKey, _cursor.Sequence, _cursor.Type);
        return true;
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        if (ownsReader)
        {
            reader.Dispose();
        }
    }
}

/// <summary>Entries already in memory: a row-keyed file's table metadata.</summary>
internal sealed class LsmListMergeSource(int ordinal, IReadOnlyList<(byte[] InternalKey, byte[] Value)> entries) : LsmMergeSource(ordinal)
{
    private int _index = -1;

    /// <inheritdoc />
    public override ReadOnlySpan<byte> InternalKey => entries[_index].InternalKey;

    /// <inheritdoc />
    public override ReadOnlySpan<byte> Value => entries[_index].Value;

    /// <inheritdoc />
    public override bool MoveNext() => ++_index < entries.Count;
}

/// <summary>
/// Merges the inputs of a compaction into the entries of its output, without holding more than each input's
/// current entry: per user key, only the newest version is kept (the first in internal-key order, ties going to
/// the input with the lower ordinal), and with <c>dropTombstones</c> a key whose newest version is a deletion is
/// left out altogether.
/// </summary>
/// <remarks>
/// An emitted entry is read straight from its input, which is only advanced on the next <see cref="MoveNext"/>,
/// so no entry is copied. With <c>keepLastRowTombstone</c> (row-keyed output), when every row was dropped but
/// something else (the table metadata) was emitted, the last dropped row tombstone is emitted at the end: a
/// row-keyed file needs at least one row to carry the metadata.
/// </remarks>
internal sealed class LsmCompactionMerge
{
    private readonly LsmMergeSource[] _heap;
    private readonly bool _dropTombstones;
    private readonly bool _keepLastRowTombstone;
    private int _count;
    private bool _advanceTop; // the top input's current entry was emitted and is still to be stepped past
    private byte[] _previousUserKey = new byte[64];
    private int _previousUserKeyLength = -1;
    private byte[] _orderCheck = new byte[64];
    private byte[]? _lastDroppedRowTombstone;
    private bool _emittedAny;
    private bool _emittedRow;
    private int _state; // 0 merging, 1 emitting the kept tombstone, 2 done

    /// <summary>Starts a merge of <paramref name="sources"/>; each is positioned on its first entry here.</summary>
    public LsmCompactionMerge(IReadOnlyList<LsmMergeSource> sources, bool dropTombstones, bool keepLastRowTombstone)
    {
        _dropTombstones = dropTombstones;
        _keepLastRowTombstone = keepLastRowTombstone;
        _heap = new LsmMergeSource[sources.Count];
        foreach (LsmMergeSource source in sources)
        {
            if (source.MoveNext())
            {
                _heap[_count++] = source;
            }
        }

        for (int i = (_count / 2) - 1; i >= 0; i--)
        {
            SiftDown(i);
        }
    }

    /// <summary>The internal key of the entry to write.</summary>
    public ReadOnlySpan<byte> InternalKey => _state == 1 ? _lastDroppedRowTombstone! : _heap[0].InternalKey;

    /// <summary>The value of the entry to write.</summary>
    public ReadOnlySpan<byte> Value => _state == 1 ? default : _heap[0].Value;

    /// <summary>Moves to the next entry to write; false when the output is complete.</summary>
    public bool MoveNext()
    {
        if (_state != 0)
        {
            _state = 2;
            return false;
        }

        if (_advanceTop)
        {
            AdvanceTop();
            _advanceTop = false;
        }

        while (_count > 0)
        {
            LsmMergeSource top = _heap[0];
            ReadOnlySpan<byte> key = top.InternalKey;
            ReadOnlySpan<byte> userKey = Lsm.InternalKey.UserKey(key);
            if (_previousUserKeyLength < 0 || !userKey.SequenceEqual(_previousUserKey.AsSpan(0, _previousUserKeyLength)))
            {
                RememberUserKey(userKey);
                bool rowKey = LsmRowKeyCodec.IsRowKey(userKey);
                if (!_dropTombstones || Lsm.InternalKey.ValueType(key) != LsmValueType.Deletion)
                {
                    _emittedAny = true;
                    _emittedRow |= rowKey;
                    _advanceTop = true;
                    return true;
                }

                if (rowKey)
                {
                    _lastDroppedRowTombstone = key.ToArray();
                }
            }

            AdvanceTop();
        }

        if (_keepLastRowTombstone && _lastDroppedRowTombstone is not null && !_emittedRow && _emittedAny)
        {
            _state = 1;
            return true;
        }

        _state = 2;
        return false;
    }

    // Steps the top input past its entry and restores the heap. An input that goes backwards is corrupt: the
    // merge relies on every input being in order.
    private void AdvanceTop()
    {
        LsmMergeSource top = _heap[0];
        ReadOnlySpan<byte> current = top.InternalKey;
        if (_orderCheck.Length < current.Length)
        {
            _orderCheck = new byte[Math.Max(current.Length, _orderCheck.Length * 2)];
        }

        int length = current.Length;
        current.CopyTo(_orderCheck);
        if (!top.MoveNext())
        {
            _heap[0] = _heap[--_count];
            _heap[_count] = null!;
        }
        else if (Lsm.InternalKey.Compare(top.InternalKey, _orderCheck.AsSpan(0, length)) <= 0)
        {
            throw new InvalidDataException("An SSTable being compacted is not in internal-key order.");
        }

        if (_count > 0)
        {
            SiftDown(0);
        }
    }

    private void RememberUserKey(ReadOnlySpan<byte> userKey)
    {
        if (_previousUserKey.Length < userKey.Length)
        {
            _previousUserKey = new byte[Math.Max(userKey.Length, _previousUserKey.Length * 2)];
        }

        userKey.CopyTo(_previousUserKey);
        _previousUserKeyLength = userKey.Length;
    }

    private void SiftDown(int at)
    {
        LsmMergeSource item = _heap[at];
        while (true)
        {
            int child = (2 * at) + 1;
            if (child >= _count)
            {
                break;
            }

            if (child + 1 < _count && Less(_heap[child + 1], _heap[child]))
            {
                child++;
            }

            if (!Less(_heap[child], item))
            {
                break;
            }

            _heap[at] = _heap[child];
            at = child;
        }

        _heap[at] = item;
    }

    private static bool Less(LsmMergeSource left, LsmMergeSource right)
    {
        int order = Lsm.InternalKey.Compare(left.InternalKey, right.InternalKey);
        return order != 0 ? order < 0 : left.Ordinal < right.Ordinal;
    }
}
