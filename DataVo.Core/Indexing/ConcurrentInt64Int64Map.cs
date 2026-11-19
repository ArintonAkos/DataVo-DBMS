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
/// <para>Key ranges (<see cref="TryCollectValuesInKeyRange"/>) are read from a sorted copy of the keys, built by
/// the first range read and kept up to date by logging the keys inserted since; a map that is never read by
/// range pays nothing for it.</para>
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
    private SortedKeys? _sortedKeys; // Writer-lock guarded; null until a range is read, or after too many inserts.

    /// <summary>
    /// The keys in ascending order as of when they were sorted, and the keys inserted since. A removed key stays
    /// listed until the next sort; readers drop it by looking it up.
    /// </summary>
    private sealed class SortedKeys(long[] keys)
    {
        public long[] Keys { get; } = keys;
        public List<long> Inserted { get; } = [];
    }

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
    /// Appends to <paramref name="values"/> the values of the keys in <c>[lower, upperExclusive)</c> (a null bound
    /// is open), in ascending key order; false, with nothing appended, when there are more than
    /// <paramref name="maxCount"/>. The first call sorts the keys under the writer lock (O(n log n), once); later
    /// calls binary-search that order and merge the keys inserted since. Keys removed since are skipped, and a
    /// key's value is read when it is collected, so an upsert in between is seen.
    /// </summary>
    public bool TryCollectValuesInKeyRange(long? lower, long? upperExclusive, int maxCount, List<long> values)
    {
        long[] sorted;
        long[] inserted;
        lock (_writeLock)
        {
            _sortedKeys ??= SortKeysNoLock();
            sorted = _sortedKeys.Keys;
            inserted = InsertedInRangeNoLock(_sortedKeys.Inserted, lower, upperExclusive);
        }

        int from = lower is { } low ? LowerBound(sorted, low) : 0;
        int to = upperExclusive is { } high ? LowerBound(sorted, high) : sorted.Length;
        if (to - from + inserted.Length > maxCount)
        {
            return false;
        }

        Array.Sort(inserted);
        int next = 0;
        long previous = 0;
        bool any = false;
        for (int i = from; i < to || next < inserted.Length;)
        {
            // Merge the two ascending lists; a key in both (removed, then inserted again) is taken once.
            long key = next >= inserted.Length || (i < to && sorted[i] <= inserted[next]) ? sorted[i++] : inserted[next++];
            if ((any && key == previous) || !TryGetValue(key, out long value))
            {
                continue;
            }

            (previous, any) = (key, true);
            values.Add(value);
        }

        return true;
    }

    /// <summary>The logged inserted keys in range, as a new array only when there are some.</summary>
    private static long[] InsertedInRangeNoLock(List<long> inserted, long? lower, long? upperExclusive)
    {
        int count = 0;
        foreach (long key in inserted)
        {
            if (InRange(key, lower, upperExclusive))
            {
                count++;
            }
        }

        if (count == 0)
        {
            return [];
        }

        var keys = new long[count];
        count = 0;
        foreach (long key in inserted)
        {
            if (InRange(key, lower, upperExclusive))
            {
                keys[count++] = key;
            }
        }

        return keys;
    }

    private static bool InRange(long key, long? lower, long? upperExclusive) =>
        (lower is not { } low || key >= low) && (upperExclusive is not { } high || key < high);

    /// <summary>The first index of <paramref name="sorted"/> whose key is at least <paramref name="key"/>.</summary>
    private static int LowerBound(long[] sorted, long key)
    {
        int low = 0;
        int high = sorted.Length;
        while (low < high)
        {
            int mid = (low + high) >>> 1;
            if (sorted[mid] < key)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        return low;
    }

    private SortedKeys SortKeysNoLock()
    {
        var keys = new long[_count];
        int count = 0;
        Table table = _table;
        for (int i = 0; i < table.States.Length && count < keys.Length; i++)
        {
            if (table.States[i] == SlotFull)
            {
                keys[count++] = table.Keys[i];
            }
        }

        Array.Sort(keys, 0, count);
        return new SortedKeys(count == keys.Length ? keys : keys[..count]);
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
                LogInsertedNoLock(key);
                return;
            }

            index = (index + 1) & table.Mask;
        }
    }

    /// <summary>
    /// Logs a new key for the sorted order; once the log is past an eighth of the sorted keys (plus some slack),
    /// the order is dropped and the next range read sorts again, so the log stays short.
    /// </summary>
    private void LogInsertedNoLock(long key)
    {
        if (_sortedKeys is not { } sorted)
        {
            return;
        }

        sorted.Inserted.Add(key);
        if (sorted.Inserted.Count > (sorted.Keys.Length / 8) + 1024)
        {
            _sortedKeys = null;
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
