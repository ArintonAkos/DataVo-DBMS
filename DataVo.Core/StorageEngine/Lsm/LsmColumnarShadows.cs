using DataVo.Core.Memory.Flat;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// Which rows of a table's SSTable columnar segments a scan must leave out because a newer source holds
/// their key: a bit per row, kept only for the chunks that have such a row.
/// </summary>
/// <remarks>
/// A scan uses two. One says which rows of a file are replaced or deleted by a newer file; files never change,
/// so it is worked out once for a set of files and then only read. The other says which rows are replaced or
/// deleted by rows not flushed yet; it is filled again by every scan, and <see cref="Clear"/> keeps its
/// buffers so that costs no allocation.
/// </remarks>
internal sealed class LsmColumnarShadows
{
    private readonly Dictionary<long, int> _slots = [];
    private readonly List<ulong[]> _masks = [];
    private int _used;

    /// <summary>
    /// Set when two files share keys and neither is known to be the newer one, so no scan of this set of
    /// files can tell which rows count.
    /// </summary>
    public bool Unordered { get; set; }

    /// <summary>Whether no row of any file is left out.</summary>
    public bool IsEmpty => _used == 0;

    /// <summary>Forgets every mask and keeps the buffers for the next scan.</summary>
    public void Clear()
    {
        _slots.Clear();
        _used = 0;
    }

    /// <summary>Marks a row of a chunk of a file as one to leave out.</summary>
    public void Shadow(int file, int chunk, int row)
    {
        long key = Key(file, chunk);
        if (!_slots.TryGetValue(key, out int slot))
        {
            slot = _used++;
            if (slot == _masks.Count)
            {
                _masks.Add(new ulong[Bitmask.WordsPerChunk]);
            }
            else
            {
                _masks[slot].AsSpan().Clear();
            }

            _slots.Add(key, slot);
        }

        Bitmask.Set(_masks[slot], row);
    }

    /// <summary>The rows to leave out of a chunk of a file, a bit per row; false when it has none.</summary>
    public bool TryGet(int file, int chunk, out ReadOnlySpan<ulong> shadowed)
    {
        if (_used != 0 && _slots.TryGetValue(Key(file, chunk), out int slot))
        {
            shadowed = _masks[slot];
            return true;
        }

        shadowed = default;
        return false;
    }

    private static long Key(int file, int chunk) => ((long)file << 32) | (uint)chunk;
}
