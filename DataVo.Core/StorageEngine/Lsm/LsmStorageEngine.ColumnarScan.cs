using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Memory.Flat;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Core.StorageEngine.Lsm;

// A columnar scan of an LSM table reads the tail in memory and then the columnar segment of every SSTable, a
// chunk of up to 4,096 rows at a time, straight from the mapped file. A row counts once: where a newer source
// (the tail, or a newer file) holds a row's key, as a row or as a deletion, the older copy is masked out.
public sealed partial class LsmStorageEngine
{
    private const int ColumnarScanAttempts = 4;

    [ThreadStatic]
    private static ColumnarScanScratch? t_columnarScratch;

    /// <summary>A thread's buffers for a columnar scan, kept between scans so a scan allocates nothing.</summary>
    private sealed class ColumnarScanScratch
    {
        public readonly SsTableColumnarChunkReader Chunk = new();
        public readonly SsTableColumnarChunkReader Other = new();
        public readonly List<RowKey> Keys = [];
        public readonly LsmColumnarShadows TailShadows = new();
        public readonly ulong[] Live = new ulong[Bitmask.WordsPerChunk];
        public SsTableV2Reader[] Readers = [];
    }

    /// <summary>What one scan reads: the tail as it was at one moment and the files that were live then.</summary>
    private readonly struct ColumnarSnapshot(ColumnarIndex tail, TableVersion version, int count, int deleted, long tailVersion)
    {
        public ColumnarIndex Tail { get; } = tail;

        public TableVersion Version { get; } = version;

        /// <summary>The tail positions the scan reads.</summary>
        public int Count { get; } = count;

        /// <summary>The tail deletes the scan honours.</summary>
        public int Deleted { get; } = deleted;

        /// <summary>The tail's version when it was read; the scan stands only if it has not moved since.</summary>
        public long TailVersion { get; } = tailVersion;
    }

    /// <inheritdoc cref="IColumnarRowStore.TryScan"/>
    internal bool TryScanColumnar(
        string databaseName,
        string tableName,
        ColumnarIndex index,
        ReadOnlySpan<ColumnarStep> steps,
        ReadOnlySpan<ColumnarFold> folds,
        Span<RowAggregate> totals,
        int limit,
        out int matched)
    {
        matched = 0;
        if (!BitConverter.IsLittleEndian // column values are read from the file as they lie
            || !_tables.TryGetValue((databaseName, tableName), out TableState? state))
        {
            return false;
        }

        ColumnarScanScratch scratch = t_columnarScratch ??= new ColumnarScanScratch();
        for (int attempt = 0; attempt < ColumnarScanAttempts; attempt++)
        {
            if (!TryCaptureColumnar(state, index, out ColumnarSnapshot snapshot))
            {
                return false;
            }

            try
            {
                if (!TryScanSnapshot(state, in snapshot, steps, folds, totals, limit, scratch, out matched))
                {
                    matched = 0;
                    return false;
                }

                if (snapshot.Tail.Version == snapshot.TailVersion)
                {
                    snapshot.Tail.CountScan();
                    return true;
                }
            }
            catch (OverflowException) when (snapshot.Tail.Version != snapshot.TailVersion)
            {
                // The rows changed underneath; the sum was not of one consistent set.
            }
            finally
            {
                // The scratch outlives the scan on this thread: it must not keep the readers of files that are
                // replaced later (their index, Bloom filter and directory) reachable.
                scratch.Chunk.Release();
                scratch.Other.Release();
                Array.Clear(scratch.Readers, 0, scratch.Readers.Length);
                snapshot.Version.Release();
            }
        }

        matched = 0;
        return false;
    }

    // Reads the tail and the file set in one step. Rows are published, and the tail is replaced after a flush,
    // under the same gate, so the two always describe the same moment.
    private static bool TryCaptureColumnar(TableState state, ColumnarIndex index, out ColumnarSnapshot snapshot)
    {
        lock (state.OverlayGate)
        {
            if (state.Columnar is not { IsTail: true } tail || !tail.SameColumns(index))
            {
                snapshot = default;
                return false;
            }

            snapshot = new ColumnarSnapshot(tail, state.AcquireVersion(), tail.Count, tail.DeletedCount, tail.Version);
            return true;
        }
    }

    // Returns false when the files cannot answer: one lacks a usable segment for the table's spec, or two of
    // them share keys in an order that cannot be told.
    private bool TryScanSnapshot(
        TableState state,
        in ColumnarSnapshot snapshot,
        ReadOnlySpan<ColumnarStep> steps,
        ReadOnlySpan<ColumnarFold> folds,
        Span<RowAggregate> totals,
        int limit,
        ColumnarScanScratch scratch,
        out int matched)
    {
        matched = 0;
        LsmSstableHandle[] files = snapshot.Version.Files;
        if (!TryOpenSegments(state, snapshot.Tail, files, scratch))
        {
            return false;
        }

        ReadOnlySpan<SsTableV2Reader> readers = scratch.Readers.AsSpan(0, files.Length);
        LsmColumnarShadows fileShadows = snapshot.Version.ColumnarShadows ?? BuildFileShadows(snapshot.Version, readers, scratch);
        if (fileShadows.Unordered)
        {
            return false;
        }

        ShadowByTail(in snapshot, readers, scratch);

        totals.Slice(0, folds.Length).Clear();
        Span<ulong> masks = ColumnarScan.Rent().Masks;
        bool full = ScanTail(in snapshot, steps, folds, totals, limit, masks, ref matched);
        for (int file = 0; !full && file < readers.Length; file++)
        {
            full = ScanSegment(readers[file].Columnar!, file, fileShadows, steps, folds, totals, limit, masks, scratch, ref matched);
        }

        ColumnarScan.Finish(totals, folds.Length, matched);
        return true;
    }

    // Collects each file's reader, and checks each has a usable segment written for the table's spec.
    private bool TryOpenSegments(TableState state, ColumnarIndex tail, LsmSstableHandle[] files, ColumnarScanScratch scratch)
    {
        if (state.Table.ColumnarSpec is not { } spec)
        {
            return false;
        }

        if (scratch.Readers.Length < files.Length)
        {
            scratch.Readers = new SsTableV2Reader[Math.Max(files.Length, 16)];
        }

        for (int i = 0; i < files.Length; i++)
        {
            SsTableV2Reader reader = files[i].GetV2Reader(_readMode);
            if (reader.Columnar is not { } segment || !segment.Spec.Matches(spec))
            {
                // A file written before the table had this spec: the next query has it rewritten.
                tail.RequestRebuild();
                return false;
            }

            if (!segment.Usable)
            {
                return false;
            }

            scratch.Readers[i] = reader;
        }

        return true;
    }

    // Returns true once the limit is reached.
    private static bool ScanTail(
        in ColumnarSnapshot snapshot,
        ReadOnlySpan<ColumnarStep> steps,
        ReadOnlySpan<ColumnarFold> folds,
        Span<RowAggregate> totals,
        int limit,
        Span<ulong> masks,
        ref int matched)
    {
        ColumnarIndex tail = snapshot.Tail;
        for (int chunk = 0, first = 0; first < snapshot.Count; chunk++, first += FlatChunk.Slots)
        {
            var chunks = new IndexChunks(tail, chunk);
            matched += ColumnarScan.ScanChunk(ref chunks, steps, folds, totals, Math.Min(FlatChunk.Slots, snapshot.Count - first), tail.Live(chunk), masks);
            if (limit > 0 && matched >= limit)
            {
                matched = limit;
                return true;
            }
        }

        return false;
    }

    // Returns true once the limit is reached.
    private static bool ScanSegment(
        SsTableColumnarSegment segment,
        int file,
        LsmColumnarShadows fileShadows,
        ReadOnlySpan<ColumnarStep> steps,
        ReadOnlySpan<ColumnarFold> folds,
        Span<RowAggregate> totals,
        int limit,
        Span<ulong> masks,
        ColumnarScanScratch scratch,
        ref int matched)
    {
        for (int chunk = 0; chunk < segment.ChunkCount; chunk++)
        {
            int rows = segment.Chunk(chunk).Rows;
            ReadOnlySpan<ulong> live = LiveRows(fileShadows, scratch, file, chunk, rows);
            if (steps.IsEmpty && folds.IsEmpty)
            {
                // A plain count needs no column: the directory knows how many rows the chunk has.
                matched += live.IsEmpty ? rows : Bitmask.Count(live);
            }
            else if (!SsTableColumnarZones.CanSkip(segment, chunk, steps))
            {
                // A chunk whose zone maps rule the condition out is never read: no page fault, no copy.
                scratch.Chunk.Load(segment, chunk);
                var chunks = new SegmentChunks(scratch.Chunk);
                matched += ColumnarScan.ScanChunk(ref chunks, steps, folds, totals, rows, live, masks);
            }

            if (limit > 0 && matched >= limit)
            {
                matched = limit;
                return true;
            }
        }

        return false;
    }

    // The rows of a chunk no newer source replaces, a bit per row; empty when that is all of them.
    private static ReadOnlySpan<ulong> LiveRows(LsmColumnarShadows fileShadows, ColumnarScanScratch scratch, int file, int chunk, int rows)
    {
        bool byFile = fileShadows.TryGet(file, chunk, out ReadOnlySpan<ulong> replacedByFile);
        bool byTail = scratch.TailShadows.TryGet(file, chunk, out ReadOnlySpan<ulong> replacedByTail);
        if (!byFile && !byTail)
        {
            return default;
        }

        Span<ulong> live = scratch.Live.AsSpan(0, Bitmask.WordsFor(rows));
        live.Fill(ulong.MaxValue);
        if (byFile)
        {
            Bitmask.AndNot(live, replacedByFile.Slice(0, live.Length));
        }

        if (byTail)
        {
            Bitmask.AndNot(live, replacedByTail.Slice(0, live.Length));
        }

        Bitmask.TrimTo(live, rows);
        return live;
    }

    // Works out, for every pair of files that share part of their key range, which rows of the older one the
    // newer one replaces or deletes. Files never change, so the result is kept with the set of files.
    private static LsmColumnarShadows BuildFileShadows(TableVersion version, ReadOnlySpan<SsTableV2Reader> readers, ColumnarScanScratch scratch)
    {
        var shadows = new LsmColumnarShadows();
        for (int a = 0; a < readers.Length && !shadows.Unordered; a++)
        {
            for (int b = a + 1; b < readers.Length; b++)
            {
                SsTableV2Properties first = readers[a].Properties;
                SsTableV2Properties second = readers[b].Properties;
                if (first.SmallestKey > second.LargestKey || second.SmallestKey > first.LargestKey)
                {
                    continue;
                }

                // Every version in one file is older than every version in the other, or the order of two
                // versions of a key cannot be told from the files' properties.
                int older;
                if (first.LargestSequence < second.SmallestSequence)
                {
                    older = a;
                }
                else if (second.LargestSequence < first.SmallestSequence)
                {
                    older = b;
                }
                else
                {
                    shadows.Unordered = true;
                    break;
                }

                ShadowByFile(shadows, older, readers[older], readers[older == a ? b : a], scratch);
            }
        }

        return Interlocked.CompareExchange(ref version.ColumnarShadows, shadows, null) ?? shadows;
    }

    // Marks the rows of an older file whose key a newer file holds, as a row or as a deletion.
    private static void ShadowByFile(LsmColumnarShadows shadows, int olderFile, SsTableV2Reader older, SsTableV2Reader newer, ColumnarScanScratch scratch)
    {
        SsTableColumnarSegment target = older.Columnar!;
        SsTableColumnarSegment source = newer.Columnar!;
        RowKey from = older.Properties.SmallestKey > newer.Properties.SmallestKey ? older.Properties.SmallestKey : newer.Properties.SmallestKey;
        RowKey to = older.Properties.LargestKey < newer.Properties.LargestKey ? older.Properties.LargestKey : newer.Properties.LargestKey;

        for (int chunk = source.FindChunk(from); chunk < source.ChunkCount && source.Chunk(chunk).FirstKey <= to; chunk++)
        {
            // Skip a chunk of the newer file that lies between two chunks of the older one.
            int facing = target.FindChunk(source.Chunk(chunk).FirstKey);
            if (facing == target.ChunkCount || target.Chunk(facing).FirstKey > source.Chunk(chunk).LastKey)
            {
                continue;
            }

            scratch.Other.Load(source, chunk);
            for (int row = 0; row < scratch.Other.Rows; row++)
            {
                Shadow(shadows, olderFile, target, scratch.Other.KeyAt(row), scratch.Chunk);
            }
        }

        ReadOnlySpan<byte> tombstones = source.Tombstones();
        for (int i = 0; i < source.TombstoneCount; i++)
        {
            Shadow(shadows, olderFile, target, SsTableColumnarSegment.TombstoneAt(tombstones, source.TombstoneKeyWidth, i), scratch.Chunk);
        }
    }

    // Marks the rows of the files whose key the tail holds, as a row it added or as a key it deleted. Costs
    // nothing when the tail's keys lie outside every file, as they do for a table that only grows.
    private static void ShadowByTail(in ColumnarSnapshot snapshot, ReadOnlySpan<SsTableV2Reader> readers, ColumnarScanScratch scratch)
    {
        scratch.TailShadows.Clear();
        ColumnarIndex tail = snapshot.Tail;
        if ((snapshot.Count == 0 && snapshot.Deleted == 0) || !AnyFileWithin(tail, snapshot, readers))
        {
            return;
        }

        // In key order, so each file's chunks are visited front to back.
        List<RowKey> keys = scratch.Keys;
        keys.Clear();
        for (int position = 0; position < snapshot.Count; position++)
        {
            keys.Add(tail.KeyAt(position));
        }

        for (int i = 0; i < snapshot.Deleted; i++)
        {
            keys.Add(tail.DeletedKeyAt(i));
        }

        keys.Sort();
        for (int file = 0; file < readers.Length; file++)
        {
            SsTableColumnarSegment segment = readers[file].Columnar!;
            SsTableV2Properties properties = readers[file].Properties;
            int at = keys.BinarySearch(properties.SmallestKey);
            for (at = at < 0 ? ~at : at; at < keys.Count && keys[at] <= properties.LargestKey; at++)
            {
                Shadow(scratch.TailShadows, file, segment, keys[at], scratch.Chunk);
            }
        }

        keys.Clear();
    }

    // Whether any file's key range reaches into the range of keys the tail has touched. The range may have
    // grown since the snapshot was taken, which only makes this say yes more often.
    private static bool AnyFileWithin(ColumnarIndex tail, in ColumnarSnapshot snapshot, ReadOnlySpan<SsTableV2Reader> readers)
    {
        if (!tail.TryGetKeyRange(out RowKey smallest, out RowKey largest))
        {
            return false;
        }

        foreach (SsTableV2Reader reader in readers)
        {
            if (reader.Properties.SmallestKey <= largest && reader.Properties.LargestKey >= smallest)
            {
                return true;
            }
        }

        return false;
    }

    // Marks the row of a file's segment that holds a key, if it holds it.
    private static void Shadow(LsmColumnarShadows shadows, int file, SsTableColumnarSegment segment, RowKey key, SsTableColumnarChunkReader reader)
    {
        int chunk = segment.FindChunk(key);
        if (chunk == segment.ChunkCount || segment.Chunk(chunk).FirstKey > key)
        {
            return;
        }

        reader.Load(segment, chunk);
        int row = reader.Find(key);
        if (row >= 0)
        {
            shadows.Shadow(file, chunk, row);
        }
    }
}
