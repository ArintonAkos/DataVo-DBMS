using System.Threading;

namespace DataVo.Core.Indexing;

/// <summary>
/// A long → long hash map with lock-free reads and internally serialized writers, tuned for the
/// integer primary-key fast lane. Replaces <c>ConcurrentDictionary&lt;long, long&gt;</c>, whose
/// per-entry node allocations dominated bulk-ingest GC: entries live in flat key/value/state arrays
/// (open addressing, linear probing), so inserting a million keys allocates a handful of large arrays
/// instead of a million nodes.
/// <para>Read protocol: a slot's key is written before its state is published as full (volatile
/// store), and a published key never changes in place — upserts overwrite only the 8-byte value
/// (atomic), removals only flip the state to tombstone. A reader therefore acquires the table
/// reference once, probes, and can trust any slot whose state reads full. Resizes build a fresh
/// table and publish it with a single volatile store; readers mid-probe keep a consistent snapshot.</para>
/// <para>Writers (upsert/remove) serialize on an internal lock; occupancy (live + tombstones) is kept
/// under ~60% so probe chains always terminate on an empty slot.</para>
/// </summary>
internal sealed class ConcurrentInt64Int64Map
{
    private const byte SlotEmpty = 0;
    private const byte SlotFull = 1;
    private const byte SlotTombstone = 2;
    private const int InitialCapacity = 16;

    private sealed class Table
    {
        public readonly long[] Keys;
        public readonly long[] Values;
        public readonly byte[] States;
        public readonly int Mask;

        public Table(int capacity)
        {
            Keys = new long[capacity];
            Values = new long[capacity];
            States = new byte[capacity];
            Mask = capacity - 1;
        }
    }

    private readonly object _writeLock = new();
    private Table _table = new(InitialCapacity);
    private int _count;    // Live entries; volatile-read by IsEmpty/Count.
    private int _occupied; // Live + tombstoned slots; writer-only, gates resize.

    /// <summary>Whether the map currently holds no live entries. Lock-free.</summary>
    public bool IsEmpty => Volatile.Read(ref _count) == 0;

    /// <summary>Number of live entries. Lock-free.</summary>
    public int Count => Volatile.Read(ref _count);

    /// <summary>Inserts or overwrites <paramref name="key"/> with <paramref name="value"/>.</summary>
    public void Set(long key, long value)
    {
        lock (_writeLock)
        {
            SetNoLock(key, value);
        }
    }

    /// <summary>Upserts a batch of pairs under one writer-lock acquisition.</summary>
    public void SetRange(ReadOnlySpan<long> keys, ReadOnlySpan<long> values)
    {
        if (keys.Length != values.Length)
        {
            throw new ArgumentException("Key and value batches must have the same length.", nameof(values));
        }

        lock (_writeLock)
        {
            EnsureCapacityNoLock(_occupied + keys.Length);
            for (int i = 0; i < keys.Length; i++)
            {
                SetNoLock(keys[i], values[i]);
            }
        }
    }

    /// <summary>Looks up <paramref name="key"/>. Lock-free.</summary>
    public bool TryGetValue(long key, out long value)
    {
        Table table = Volatile.Read(ref _table);
        int index = Hash(key) & table.Mask;
        while (true)
        {
            byte state = Volatile.Read(ref table.States[index]);
            if (state == SlotEmpty)
            {
                value = 0;
                return false;
            }

            if (state == SlotFull && table.Keys[index] == key)
            {
                value = Volatile.Read(ref table.Values[index]);
                return true;
            }

            index = (index + 1) & table.Mask;
        }
    }

    /// <summary>Removes <paramref name="key"/> if present; returns whether it was removed.</summary>
    public bool TryRemove(long key)
    {
        lock (_writeLock)
        {
            Table table = _table;
            int index = Hash(key) & table.Mask;
            while (true)
            {
                byte state = table.States[index];
                if (state == SlotEmpty)
                {
                    return false;
                }

                if (state == SlotFull && table.Keys[index] == key)
                {
                    Volatile.Write(ref table.States[index], SlotTombstone);
                    Volatile.Write(ref _count, _count - 1);
                    return true;
                }

                index = (index + 1) & table.Mask;
            }
        }
    }

    /// <summary>
    /// Invokes <paramref name="visit"/> for every live pair; returning <see langword="false"/> stops the
    /// walk. Enumerates a table snapshot, so concurrent mutations may or may not be observed (same
    /// contract as ConcurrentDictionary enumeration).
    /// </summary>
    public void ForEach<TState>(TState state, Func<TState, long, long, bool> visit)
    {
        Table table = Volatile.Read(ref _table);
        for (int i = 0; i < table.States.Length; i++)
        {
            if (Volatile.Read(ref table.States[i]) == SlotFull
                && !visit(state, table.Keys[i], Volatile.Read(ref table.Values[i])))
            {
                return;
            }
        }
    }

    private void SetNoLock(long key, long value)
    {
        EnsureCapacityNoLock(_occupied + 1);

        Table table = _table;
        int index = Hash(key) & table.Mask;
        int firstTombstone = -1;
        while (true)
        {
            byte state = table.States[index];
            if (state == SlotFull && table.Keys[index] == key)
            {
                Volatile.Write(ref table.Values[index], value);
                return;
            }

            if (state == SlotTombstone)
            {
                if (firstTombstone < 0)
                {
                    firstTombstone = index;
                }
            }
            else if (state == SlotEmpty)
            {
                int target = firstTombstone >= 0 ? firstTombstone : index;
                table.Values[target] = value;
                table.Keys[target] = key;
                Volatile.Write(ref table.States[target], SlotFull);
                if (firstTombstone < 0)
                {
                    _occupied++;
                }

                Volatile.Write(ref _count, _count + 1);
                return;
            }

            index = (index + 1) & table.Mask;
        }
    }

    private void EnsureCapacityNoLock(int requiredOccupied)
    {
        Table table = _table;
        int capacity = table.Mask + 1;
        // Keep occupancy under ~60% so probes always hit an empty slot.
        if (requiredOccupied * 5L < capacity * 3L)
        {
            return;
        }

        // Size for live entries plus the pending inserts: a rebuild drops tombstones, so a
        // tombstone-heavy table may rebuild at the same capacity.
        long needed = _count + (requiredOccupied - _occupied);
        int newCapacity = capacity;
        while (needed * 5L >= newCapacity * 3L)
        {
            newCapacity *= 2;
        }

        var next = new Table(newCapacity);
        for (int i = 0; i < table.States.Length; i++)
        {
            if (table.States[i] != SlotFull)
            {
                continue;
            }

            int index = Hash(table.Keys[i]) & next.Mask;
            while (next.States[index] == SlotFull)
            {
                index = (index + 1) & next.Mask;
            }

            next.Keys[index] = table.Keys[i];
            next.Values[index] = table.Values[i];
            next.States[index] = SlotFull;
        }

        _occupied = _count;
        Volatile.Write(ref _table, next);
    }

    private static int Hash(long key)
    {
        // splitmix64/murmur3 finalizer: full-avalanche so sequential keys don't cluster probe chains.
        ulong x = (ulong)key;
        x ^= x >> 33;
        x *= 0xFF51AFD7ED558CCDUL;
        x ^= x >> 33;
        x *= 0xC4CEB9FE1A85EC53UL;
        x ^= x >> 33;
        return (int)x;
    }
}
