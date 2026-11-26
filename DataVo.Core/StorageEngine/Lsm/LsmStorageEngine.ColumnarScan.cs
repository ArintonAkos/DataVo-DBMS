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

        // What TailShadows holds: the rows of one version's files that the first TailPositions positions and
        // TailDeletes deletes of one tail replace. Both only grow, so the next scan of the same tail and
        // version adds what came since. Kept by number, not by reference, so no tail or file stays reachable.
        public long TailIdentity = -1;
        public long VersionId = -1;
        public int TailPositions;
        public int TailDeletes;
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

    /// <summary>Diagnostics: holds the table's current set of files, as a scan that began before later flushes does.</summary>
    internal IDisposable PinFilesForTesting(string databaseName, string tableName) =>
        new FilesPin(GetOrCreateTable(databaseName, tableName).AcquireVersion());

    /// <summary>
    /// Diagnostics: works out the columnar delete vectors of a set of files (the pinned one, or the current one)
    /// starting from the vectors the files kept, as a scan does, and again from nothing, and throws unless every
    /// file's vector marks the same rows.
    /// </summary>
    /// <returns>How many files started from a kept vector.</returns>
    internal int VerifyColumnarDeleteVectorsForTesting(string databaseName, string tableName, IDisposable? pin = null)
    {
        TableState state = GetOrCreateTable(databaseName, tableName);
        TableVersion version = pin is FilesPin pinned ? pinned.Version : state.AcquireVersion();
        var scratch = new ColumnarScanScratch();
        try
        {
            if (!TryOpenSegments(state, tail: null, version.Files, scratch))
            {
                throw new InvalidOperationException("A file of the set has no usable columnar segment for the table's spec.");
            }

            ReadOnlySpan<SsTableV2Reader> readers = scratch.Readers.AsSpan(0, version.Files.Length);
            LsmFileShadows kept = BuildFileShadows(version.Files, readers, scratch, reuse: true, out int reused);
            LsmFileShadows fresh = BuildFileShadows(version.Files, readers, scratch, reuse: false, out _);
            if (kept.Unordered != fresh.Unordered)
            {
                throw new InvalidOperationException("The two ways disagree on whether the files can be ordered.");
            }

            for (int file = 0; !fresh.Unordered && file < readers.Length; file++)
            {
                for (int chunk = 0; chunk < readers[file].Columnar!.ChunkCount; chunk++)
                {
                    kept.TryGet(file, chunk, out ReadOnlySpan<ulong> a);
                    fresh.TryGet(file, chunk, out ReadOnlySpan<ulong> b);
                    for (int word = 0; word < Bitmask.WordsPerChunk; word++)
                    {
                        if ((a.IsEmpty ? 0 : a[word]) != (b.IsEmpty ? 0 : b[word]))
                        {
                            throw new InvalidOperationException($"File {version.Files[file].FileNumber}, chunk {chunk}, word {word}: started from a kept vector {(a.IsEmpty ? 0 : a[word]):X}, from nothing {(b.IsEmpty ? 0 : b[word]):X}.");
                        }
                    }
                }
            }

            return reused;
        }
        finally
        {
            scratch.Chunk.Release();
            scratch.Other.Release();
            if (pin is null)
            {
                version.Release();
            }
        }
    }

    private sealed class FilesPin(TableVersion version) : IDisposable
    {
        private int _released;

        public TableVersion Version { get; } = version;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
            {
                Version.Release();
            }
        }
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
        LsmFileShadows fileShadows = snapshot.Version.ColumnarShadows ?? CacheFileShadows(snapshot.Version, readers, scratch);
        if (fileShadows.Unordered)
        {
            return false;
        }

        ShadowByTail(in snapshot, readers, snapshot.Version.Id, scratch);
        if (snapshot.Tail.Version != snapshot.TailVersion)
        {
            // Tail moved while building shadows; let the caller retry with a fresh snapshot instead of scanning chunks.
            return true;
        }

        totals.Slice(0, folds.Length).Clear();
        Span<ulong> masks = ColumnarScan.Rent().Masks;
        bool full = ScanTail(in snapshot, steps, folds, totals, limit, masks, ref matched);
        for (int file = 0; !full && file < readers.Length; file++)
        {
            if (snapshot.Tail.Version != snapshot.TailVersion)
            {
                return true;
            }

            full = ScanSegment(readers[file].Columnar!, file, fileShadows, steps, folds, totals, limit, masks, scratch, ref matched);
        }

        ColumnarScan.Finish(totals, folds.Length, matched);
        return true;
    }

    // Collects each file's reader, and checks each has a usable segment written for the table's spec.
    private bool TryOpenSegments(TableState state, ColumnarIndex? tail, LsmSstableHandle[] files, ColumnarScanScratch scratch)
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
            SsTableV2Reader reader = files[i].GetV2Reader(_readMode, _blockCache);
            if (reader.Columnar is not { } segment || !segment.Spec.Matches(spec))
            {
                // A file written before the table had this spec: the next query has it rewritten.
                tail?.RequestRebuild();
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
        LsmFileShadows fileShadows,
        ReadOnlySpan<ColumnarStep> steps,
        ReadOnlySpan<ColumnarFold> folds,
        Span<RowAggregate> totals,
        int limit,
        Span<ulong> masks,
        ColumnarScanScratch scratch,
        ref int matched)
    {
        // EXPLAIN counts chunk groups read and skipped; an ordinary scan reads null here once per file.
        Runtime.Diagnostics.QueryExecutionTrace? trace = Runtime.Diagnostics.QueryExecutionTrace.Current;
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
                trace?.CountSegment(skipped: false);
                scratch.Chunk.Load(segment, chunk);
                var chunks = new SegmentChunks(scratch.Chunk);
                matched += ColumnarScan.ScanChunk(ref chunks, steps, folds, totals, rows, live, masks);
            }
            else
            {
                trace?.CountSegment(skipped: true);
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
    private static ReadOnlySpan<ulong> LiveRows(LsmFileShadows fileShadows, ColumnarScanScratch scratch, int file, int chunk, int rows)
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

    // Works out the file shadows of a set of files once and keeps them with the set, as the files never change.
    private static LsmFileShadows CacheFileShadows(TableVersion version, ReadOnlySpan<SsTableV2Reader> readers, ColumnarScanScratch scratch)
    {
        LsmFileShadows shadows = BuildFileShadows(version.Files, readers, scratch, reuse: true, out _);
        return Interlocked.CompareExchange(ref version.ColumnarShadows, shadows, null) ?? shadows;
    }

    // Works out, for each file, which of its rows the newer files it shares keys with replace or delete. With
    // `reuse`, a file starts from the vector it kept for an earlier set of files when that is sound (see
    // LsmDeleteVector) and marks only the keys of the files that vector lacks; a set that differs from the last
    // by one flushed file costs the keys of that file. `reused` counts the files that started from a kept vector.
    private static LsmFileShadows BuildFileShadows(
        LsmSstableHandle[] files,
        ReadOnlySpan<SsTableV2Reader> readers,
        ColumnarScanScratch scratch,
        bool reuse,
        out int reused)
    {
        reused = 0;
        if (!TryFindNewer(readers, out List<int>[] newer))
        {
            return LsmFileShadows.UnorderedSet;
        }

        long[] live = new long[files.Length];
        for (int i = 0; i < files.Length; i++)
        {
            live[i] = files[i].FileNumber;
        }

        Array.Sort(live);
        var vectors = new LsmDeleteVector?[files.Length];
        for (int older = 0; older < files.Length; older++)
        {
            if (newer[older].Count == 0)
            {
                continue;
            }

            long[] needed = new long[newer[older].Count];
            for (int i = 0; i < needed.Length; i++)
            {
                needed[i] = files[newer[older][i]].FileNumber;
            }

            Array.Sort(needed);
            LsmDeleteVector? start = reuse ? files[older].ColumnarDeleteVector : null;
            if (start is not null && !start.CanStart(live, needed))
            {
                start = null;
            }

            if (start is not null)
            {
                reused++;
                if (start.Applied.AsSpan().SequenceEqual(needed))
                {
                    vectors[older] = start;
                    continue;
                }
            }

            var builder = new LsmDeleteVector.Builder(readers[older].Columnar!.ChunkCount, start);
            foreach (int file in newer[older])
            {
                if (start is null || Array.BinarySearch(start.Applied, files[file].FileNumber) < 0)
                {
                    ShadowByFile(builder, readers[older], readers[file], scratch);
                }
            }

            LsmDeleteVector vector = builder.Build(needed, live[^1]);
            vectors[older] = vector;
            if (reuse)
            {
                files[older].OfferColumnarDeleteVector(vector);
            }
        }

        return new LsmFileShadows(vectors, unordered: false);
    }

    // For each file, the files that share part of its key range and are newer: every version in them is newer
    // than every version in it. False when two files share keys and their sequence ranges interleave.
    private static bool TryFindNewer(ReadOnlySpan<SsTableV2Reader> readers, out List<int>[] newer)
    {
        newer = new List<int>[readers.Length];
        for (int i = 0; i < readers.Length; i++)
        {
            newer[i] = [];
        }

        for (int a = 0; a < readers.Length; a++)
        {
            for (int b = a + 1; b < readers.Length; b++)
            {
                SsTableV2Properties first = readers[a].Properties;
                SsTableV2Properties second = readers[b].Properties;
                if (first.SmallestKey > second.LargestKey || second.SmallestKey > first.LargestKey)
                {
                    continue;
                }

                if (first.LargestSequence < second.SmallestSequence)
                {
                    newer[a].Add(b);
                }
                else if (second.LargestSequence < first.SmallestSequence)
                {
                    newer[b].Add(a);
                }
                else
                {
                    return false;
                }
            }
        }

        return true;
    }

    // Marks the rows of an older file whose key a newer file holds, as a row or as a deletion.
    private static void ShadowByFile(LsmDeleteVector.Builder shadows, SsTableV2Reader older, SsTableV2Reader newer, ColumnarScanScratch scratch)
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
                if (TryFindRow(target, scratch.Other.KeyAt(row), scratch.Chunk, out int targetChunk, out int targetRow))
                {
                    shadows.Mark(targetChunk, targetRow);
                }
            }
        }

        ReadOnlySpan<byte> tombstones = source.Tombstones();
        for (int i = 0; i < source.TombstoneCount; i++)
        {
            if (TryFindRow(target, SsTableColumnarSegment.TombstoneAt(tombstones, source.TombstoneKeyWidth, i), scratch.Chunk, out int targetChunk, out int targetRow))
            {
                shadows.Mark(targetChunk, targetRow);
            }
        }
    }

    // Marks the rows of the files whose key the tail holds, as a row it added or as a key it deleted. The
    // tail only grows and the files never change, so a thread keeps what it marked for a tail and a version
    // and marks only the keys published since its last scan of them. Costs nothing when the tail's keys lie
    // outside every file, as they do for a table that only grows.
    private static void ShadowByTail(in ColumnarSnapshot snapshot, ReadOnlySpan<SsTableV2Reader> readers, long versionId, ColumnarScanScratch scratch)
    {
        ColumnarIndex tail = snapshot.Tail;
        if (scratch.TailIdentity != tail.Identity
            || scratch.VersionId != versionId
            || scratch.TailPositions > snapshot.Count
            || scratch.TailDeletes > snapshot.Deleted)
        {
            scratch.TailShadows.Clear();
            scratch.TailIdentity = tail.Identity;
            scratch.VersionId = versionId;
            scratch.TailPositions = 0;
            scratch.TailDeletes = 0;
        }

        int fromPosition = scratch.TailPositions;
        int fromDelete = scratch.TailDeletes;
        if (fromPosition == snapshot.Count && fromDelete == snapshot.Deleted)
        {
            return;
        }

        // A key outside every file's range never replaces a file row; the range only grows, so keys skipped
        // here stay outside the files of this version.
        if (AnyFileWithin(tail, readers))
        {
            // In key order, so each file's chunks are visited front to back.
            List<RowKey> keys = scratch.Keys;
            keys.Clear();
            for (int position = fromPosition; position < snapshot.Count; position++)
            {
                keys.Add(tail.KeyAt(position));
            }

            for (int i = fromDelete; i < snapshot.Deleted; i++)
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

        // Recorded only once marked: a scan that failed part way marks the same keys again (marks are idempotent).
        scratch.TailPositions = snapshot.Count;
        scratch.TailDeletes = snapshot.Deleted;
    }

    // Whether any file's key range reaches into the range of keys the tail has touched. The range may have
    // grown since the snapshot was taken, which only makes this say yes more often.
    private static bool AnyFileWithin(ColumnarIndex tail, ReadOnlySpan<SsTableV2Reader> readers)
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
        if (TryFindRow(segment, key, reader, out int chunk, out int row))
        {
            shadows.Shadow(file, chunk, row);
        }
    }

    // The chunk and row of a file's segment that hold a key; false when the segment does not hold it.
    private static bool TryFindRow(SsTableColumnarSegment segment, RowKey key, SsTableColumnarChunkReader reader, out int chunk, out int row)
    {
        chunk = segment.FindChunk(key);
        row = -1;
        if (chunk == segment.ChunkCount || segment.Chunk(chunk).FirstKey > key)
        {
            return false;
        }

        reader.Load(segment, chunk);
        row = reader.Find(key);
        return row >= 0;
    }
}
