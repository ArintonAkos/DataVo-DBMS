using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Reads every entry of a keyed SSTable (the format generic-key <see cref="LsmTable"/>s flush and compact to) into
/// memory, so tests can look keys up as of a snapshot sequence.
/// </summary>
internal sealed class TestSsTable
{
    private readonly List<(byte[] UserKey, ulong Sequence, LsmValueType Type, byte[] Value)> _entries;

    private TestSsTable(List<(byte[] UserKey, ulong Sequence, LsmValueType Type, byte[] Value)> entries) => _entries = entries;

    /// <summary>Every entry in file order (user key ascending, newest version first).</summary>
    public IReadOnlyList<(byte[] UserKey, ulong Sequence, LsmValueType Type, byte[] Value)> Entries => _entries;

    /// <summary>Opens the keyed SSTable at <paramref name="path"/>, validating it as the engine does.</summary>
    public static TestSsTable Open(string path)
    {
        var entries = new List<(byte[] UserKey, ulong Sequence, LsmValueType Type, byte[] Value)>();
        using SsTableKeyedReader reader = SsTableKeyedReader.Open(path, LsmReadMode.PositionalRead);
        reader.ForEach(entries, static (userKey, sequence, type, value, list) => list.Add((userKey.ToArray(), sequence, type, value.ToArray())));
        return new TestSsTable(entries);
    }

    /// <summary>Opens a keyed SSTable image held in memory.</summary>
    public static TestSsTable Load(byte[] image)
    {
        string path = Path.Combine(Path.GetTempPath(), $"datavo_test_sst_{Guid.NewGuid():N}.sst");
        File.WriteAllBytes(path, image);
        try
        {
            return Open(path);
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>
    /// The newest version of <paramref name="userKey"/> at or below <paramref name="snapshotSeqno"/>: true with its
    /// value for a put, false with <paramref name="isTombstone"/> set for a delete, false otherwise.
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> userKey, ulong snapshotSeqno, out byte[] value, out bool isTombstone)
    {
        foreach ((byte[] key, ulong sequence, LsmValueType type, byte[] entryValue) in _entries)
        {
            if (sequence <= snapshotSeqno && key.AsSpan().SequenceEqual(userKey))
            {
                isTombstone = type == LsmValueType.Deletion;
                value = isTombstone ? [] : entryValue;
                return !isTombstone;
            }
        }

        value = [];
        isTombstone = false;
        return false;
    }
}
