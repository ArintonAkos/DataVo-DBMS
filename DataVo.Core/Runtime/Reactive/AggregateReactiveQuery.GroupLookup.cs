namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Finds an existing group by its rendered key without allocating a string for the probe.
/// </summary>
/// <remarks>
/// .NET 9 and later probe the group dictionary directly through its <c>ReadOnlySpan&lt;char&gt;</c> alternate
/// lookup. Earlier targets (net8.0, netstandard2.1) keep a small side index from a hash of the key's characters
/// to the stored key, maintained wherever a group is added or dropped, so their steady-state dispatch is
/// allocation-free too.
/// </remarks>
internal sealed partial class AggregateReactiveQuery
{
#if NET9_0_OR_GREATER
    private bool TryGetGroup(ReadOnlySpan<char> keySpan, out string? key, out GroupState? state) =>
        _groups.GetAlternateLookup<ReadOnlySpan<char>>().TryGetValue(keySpan, out key, out state);

    // The dictionary itself answers span probes; there is no side index to maintain.
    private static void IndexGroup(string key, GroupState state)
    {
    }

    private static void UnindexGroup(string key)
    {
    }
#else
    /// <summary>One group in the side index; groups whose keys share a hash are chained.</summary>
    private sealed class IndexedGroup(string key, GroupState state, IndexedGroup? next)
    {
        public string Key { get; } = key;

        public GroupState State { get; } = state;

        public IndexedGroup? Next { get; set; } = next;
    }

    private readonly Dictionary<int, IndexedGroup> _groupsByKeyHash = [];

    private bool TryGetGroup(ReadOnlySpan<char> keySpan, out string? key, out GroupState? state)
    {
        if (_groupsByKeyHash.TryGetValue(HashKey(keySpan), out IndexedGroup? entry))
        {
            for (; entry is not null; entry = entry.Next)
            {
                if (keySpan.SequenceEqual(entry.Key.AsSpan()))
                {
                    key = entry.Key;
                    state = entry.State;
                    return true;
                }
            }
        }

        key = null;
        state = null;
        return false;
    }

    private void IndexGroup(string key, GroupState state)
    {
        int hash = HashKey(key.AsSpan());
        _groupsByKeyHash.TryGetValue(hash, out IndexedGroup? head);
        _groupsByKeyHash[hash] = new IndexedGroup(key, state, head);
    }

    private void UnindexGroup(string key)
    {
        int hash = HashKey(key.AsSpan());
        if (!_groupsByKeyHash.TryGetValue(hash, out IndexedGroup? entry))
        {
            return;
        }

        IndexedGroup? previous = null;
        for (; entry is not null; previous = entry, entry = entry.Next)
        {
            if (!string.Equals(entry.Key, key, StringComparison.Ordinal))
            {
                continue;
            }

            if (previous is not null)
            {
                previous.Next = entry.Next;
            }
            else if (entry.Next is not null)
            {
                _groupsByKeyHash[hash] = entry.Next;
            }
            else
            {
                _groupsByKeyHash.Remove(hash);
            }

            return;
        }
    }

    // FNV-1a over the UTF-16 code units: deterministic on every target, and only compared within this process.
    private static int HashKey(ReadOnlySpan<char> key)
    {
        uint hash = 2166136261;
        foreach (char c in key)
        {
            hash = (hash ^ c) * 16777619;
        }

        return unchecked((int)hash);
    }
#endif
}
