using System.Runtime.CompilerServices;

namespace DataVo.Core.Indexing.Columnar;

/// <summary>
/// Where each row key's row sits in a <see cref="ColumnarIndex"/>: a hash map from <see cref="RowKey"/> to
/// position, held in three flat arrays (open addressing, linear probing), so a million keys cost three large
/// arrays and no object per key.
/// </summary>
/// <remarks>
/// Only the writer of the index uses the map, to find the position a delete or an update replaces; scans never
/// look at it. It is therefore not safe for concurrent use. Removing a key shifts the entries after it back,
/// so the table never fills with markers of removed keys.
/// </remarks>
internal sealed class RowKeyPositionMap
{
    private const int InitialCapacity = 64;

    private ulong[] _high;
    private ulong[] _low;
    private int[] _slots; // position + 1; 0 marks an empty slot
    private int _mask;
    private int _count;

    /// <summary>Creates an empty map.</summary>
    /// <param name="capacity">The number of keys to make room for.</param>
    public RowKeyPositionMap(int capacity = 0)
    {
        int size = InitialCapacity;
        while (size < capacity * 2L && size < (1 << 30))
        {
            size <<= 1;
        }

        _high = new ulong[size];
        _low = new ulong[size];
        _slots = new int[size];
        _mask = size - 1;
    }

    /// <summary>The number of keys.</summary>
    public int Count => _count;

    /// <summary>Finds a key's position.</summary>
    public bool TryGet(RowKey key, out int position)
    {
        int at = Find(key);
        position = at < 0 ? -1 : _slots[at] - 1;
        return at >= 0;
    }

    /// <summary>Sets a key's position, adding the key when it is new.</summary>
    public void Set(RowKey key, int position)
    {
        int at = Find(key);
        if (at >= 0)
        {
            _slots[at] = position + 1;
            return;
        }

        if ((_count + 1) * 2L > _slots.Length)
        {
            Grow();
        }

        Insert(key.High, key.Low, position + 1);
        _count++;
    }

    /// <summary>Removes a key and reports the position it had.</summary>
    public bool Remove(RowKey key, out int position)
    {
        int at = Find(key);
        if (at < 0)
        {
            position = -1;
            return false;
        }

        position = _slots[at] - 1;
        _count--;

        // Close the gap: move back each following entry that would no longer be reachable from its home slot.
        int next = (at + 1) & _mask;
        while (_slots[next] != 0)
        {
            int home = Home(_high[next], _low[next]);
            if (((next - home) & _mask) >= ((next - at) & _mask))
            {
                _high[at] = _high[next];
                _low[at] = _low[next];
                _slots[at] = _slots[next];
                at = next;
            }

            next = (next + 1) & _mask;
        }

        _slots[at] = 0;
        return true;
    }

    private int Find(RowKey key)
    {
        ulong high = key.High;
        ulong low = key.Low;
        for (int at = Home(high, low); _slots[at] != 0; at = (at + 1) & _mask)
        {
            if (_low[at] == low && _high[at] == high)
            {
                return at;
            }
        }

        return -1;
    }

    private void Insert(ulong high, ulong low, int slot)
    {
        int at = Home(high, low);
        while (_slots[at] != 0)
        {
            at = (at + 1) & _mask;
        }

        _high[at] = high;
        _low[at] = low;
        _slots[at] = slot;
    }

    private void Grow()
    {
        ulong[] high = _high;
        ulong[] low = _low;
        int[] slots = _slots;
        int size = checked(slots.Length * 2);
        _high = new ulong[size];
        _low = new ulong[size];
        _slots = new int[size];
        _mask = size - 1;
        for (int i = 0; i < slots.Length; i++)
        {
            if (slots[i] != 0)
            {
                Insert(high[i], low[i], slots[i]);
            }
        }
    }

    // Integer keys differ only in their low bits and GUIDs in every bit; multiplying spreads both over the table.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Home(ulong high, ulong low)
    {
        ulong mixed = (low ^ (high * 0xC2B2_AE3D_27D4_EB4FUL)) * 0x9E37_79B9_7F4A_7C15UL;
        return (int)(mixed >> 32) & _mask;
    }
}
