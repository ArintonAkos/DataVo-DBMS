using System.Buffers.Binary;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

/// <summary>
/// The compaction algorithm as it was before compaction streamed: every entry of every input is read into
/// memory, sorted, merged through a priority queue and written from the merged list. Kept as the oracle the
/// streaming compactor must match byte for byte.
/// </summary>
internal static class ReferenceCompaction
{
    /// <summary>The output file's bytes for compacting <paramref name="files"/> (in this order), or null for no output.</summary>
    public static byte[]? Run(string directory, IReadOnlyList<LsmTableFileMetadata> files, LsmSstableFormat format, ColumnarSpec? columnar, bool dropTombstones)
    {
        bool writesV2 = format == LsmSstableFormat.RowKeyed;
        List<(byte[] Key, byte[] Value)> merged = Merge(directory, files, writesV2, dropTombstones);
        if (merged.Count == 0)
        {
            return null;
        }

        var stream = new MemoryStream();
        if (writesV2)
        {
            using var writer = new SsTableV2Writer(stream, alignment: 0, merged.Count, columnar: columnar);
            foreach ((byte[] key, byte[] value) in merged)
            {
                LsmRowKeyCodec.AddInternalEntry(writer, key, value);
            }

            writer.Finish();
        }
        else if (format == LsmSstableFormat.Keyed)
        {
            using var writer = new SsTableKeyedWriter(stream, LsmIndexKeyCodec.BloomSuffix);
            foreach ((byte[] key, byte[] value) in merged)
            {
                writer.Add(InternalKey.UserKey(key), InternalKey.Sequence(key), InternalKey.ValueType(key), value);
            }

            writer.Finish();
        }
        else
        {
            var writer = new SsTableWriter(merged.Count);
            foreach ((byte[] key, byte[] value) in merged)
            {
                writer.Add(key, value);
            }

            return writer.Finish();
        }

        return stream.ToArray();
    }

    private static List<(byte[] Key, byte[] Value)> Merge(string directory, IReadOnlyList<LsmTableFileMetadata> files, bool writesV2, bool dropTombstones)
    {
        var queue = new PriorityQueue<(int Ordinal, List<(byte[] Key, byte[] Value)> Entries, int Index), (byte[] Key, int Ordinal)>(Comparer.Instance);
        for (int i = 0; i < files.Count; i++)
        {
            List<(byte[] Key, byte[] Value)> entries = Read(Path.Combine(directory, files[i].FileName), writesV2);
            entries.Sort(static (x, y) => InternalKey.Compare(x.Key, y.Key));
            if (entries.Count > 0)
            {
                queue.Enqueue((i, entries, 0), (entries[0].Key, i));
            }
        }

        var merged = new List<(byte[] Key, byte[] Value)>();
        byte[]? previousUserKey = null;
        (byte[] Key, byte[] Value)? lastDroppedRowTombstone = null;
        while (queue.TryDequeue(out var cursor, out _))
        {
            (byte[] key, byte[] value) = cursor.Entries[cursor.Index];
            if (previousUserKey is null || !InternalKey.UserKey(key).SequenceEqual(previousUserKey))
            {
                previousUserKey = InternalKey.UserKey(key).ToArray();
                if (!dropTombstones || InternalKey.ValueType(key) != LsmValueType.Deletion)
                {
                    merged.Add((key, value));
                }
                else if (LsmRowKeyCodec.IsRowKey(InternalKey.UserKey(key)))
                {
                    lastDroppedRowTombstone = (key, value);
                }
            }

            if (cursor.Index + 1 < cursor.Entries.Count)
            {
                queue.Enqueue((cursor.Ordinal, cursor.Entries, cursor.Index + 1), (cursor.Entries[cursor.Index + 1].Key, cursor.Ordinal));
            }
        }

        if (writesV2
            && lastDroppedRowTombstone is { } kept
            && !merged.Exists(static e => LsmRowKeyCodec.IsRowKey(InternalKey.UserKey(e.Key)))
            && merged.Count > 0)
        {
            merged.Add(kept);
            merged.Sort(static (x, y) => InternalKey.Compare(x.Key, y.Key));
        }

        return merged;
    }

    private static List<(byte[] Key, byte[] Value)> Read(string path, bool writesV2)
    {
        var entries = new List<(byte[] Key, byte[] Value)>();
        if (SsTableKeyedFormat.IsKeyedFile(path))
        {
            using SsTableKeyedReader keyed = SsTableKeyedReader.Open(path);
            keyed.ForEach(entries, static (userKey, sequence, type, value, list) =>
            {
                var key = new byte[userKey.Length + InternalKey.TagSize];
                InternalKey.Write(key, userKey, sequence, type);
                list.Add((key, value.ToArray()));
            });
            return entries;
        }

        if (!SsTableV2Format.IsV2File(path))
        {
            byte[] bytes = File.ReadAllBytes(path);
            foreach ((byte[] key, byte[] value) in ReadV1(bytes))
            {
                entries.Add((writesV2 ? Widen(key) : key, value));
            }

            return entries;
        }

        using SsTableV2Reader reader = SsTableV2Reader.Open(path);
        SsTableV2Cursor cursor = reader.OpenCursor();
        while (cursor.MoveNext())
        {
            entries.Add((LsmRowKeyCodec.RowInternalKey(cursor.Key, cursor.Sequence, cursor.Type), cursor.Type == LsmValueType.Put ? cursor.Value.ToArray() : []));
        }

        if (reader.Properties.NextRowId > 0)
        {
            entries.Add((LsmRowKeyCodec.MetadataInternalKey(reader.Properties.NextRowIdSequence), LsmRowKeyCodec.EncodeTableMetadata(reader.Properties.NextRowId, reader.Properties.LiveRowCount)));
        }

        return entries;
    }

    private static byte[] Widen(byte[] key)
    {
        ReadOnlySpan<byte> userKey = InternalKey.UserKey(key);
        if (userKey.Length != LsmRowKeyCodec.LegacyRowKeySize)
        {
            return key;
        }

        var widened = new byte[LsmRowKeyCodec.RowKeySize + InternalKey.TagSize];
        userKey.CopyTo(widened.AsSpan(LsmRowKeyCodec.RowKeySize - LsmRowKeyCodec.LegacyRowKeySize));
        key.AsSpan(userKey.Length).CopyTo(widened.AsSpan(LsmRowKeyCodec.RowKeySize));
        return widened;
    }

    // A v1 file: footer -> index block with one data block -> length-prefixed entries.
    private static IEnumerable<(byte[] Key, byte[] Value)> ReadV1(byte[] bytes)
    {
        Assert.True(SsTableFormat.TryReadFooter(bytes, out SsTableBlockHandle index, out _));
        ReadOnlySpan<byte> indexBlock = bytes.AsSpan((int)index.Offset, index.Length);
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(indexBlock));
        long dataOffset = BinaryPrimitives.ReadInt64LittleEndian(indexBlock[4..]);
        int dataLength = BinaryPrimitives.ReadInt32LittleEndian(indexBlock[12..]);
        var result = new List<(byte[] Key, byte[] Value)>();
        int offset = (int)dataOffset;
        int end = offset + dataLength;
        while (offset < end)
        {
            int keyLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset));
            int valueLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset + 4));
            offset += 8;
            result.Add((bytes.AsSpan(offset, keyLength).ToArray(), bytes.AsSpan(offset + keyLength, valueLength).ToArray()));
            offset += keyLength + valueLength;
        }

        return result;
    }

    private sealed class Comparer : IComparer<(byte[] Key, int Ordinal)>
    {
        public static readonly Comparer Instance = new();

        public int Compare((byte[] Key, int Ordinal) x, (byte[] Key, int Ordinal) y)
        {
            int order = InternalKey.Compare(x.Key, y.Key);
            return order != 0 ? order : x.Ordinal.CompareTo(y.Ordinal);
        }
    }
}
