using DataVo.Core.Memory.Flat;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// The rows of one SSTable's columnar segment that newer files replace or delete, a bit per row, kept only for
/// the chunks that have such a row. Immutable once built.
/// </summary>
/// <remarks>
/// <para>
/// A vector is made for one set of live files and records which newer files it took keys from
/// (<see cref="Applied"/>) and the highest file number of that set (<see cref="Horizon"/>). The file keeps its
/// newest vector, and the next set of files starts from it instead of from nothing, which is sound because:
/// </para>
/// <list type="bullet">
/// <item>Rows only ever become replaced. Compaction keeps the newest version of every key, and drops a deletion
/// only where every file that could hold an older version of the key is compacted with it; so once a newer file
/// held a key, some live newer file holds it for as long as this file lives.</item>
/// <item>Files are numbered in the order they are published (flush, compaction and backfill allocate, write and
/// publish under one lock), so a file published after a set of files has a higher number than all of them.</item>
/// </list>
/// <para>
/// So a vector may be reused for a set of files when every file it applied is either live in the set (and then
/// newer than this file and overlapping it) or no longer live and numbered below the set's highest file: such a
/// file was replaced before the set existed, and what it held is still held by a live newer file. A vector that
/// applied a file published after the set (numbered above it) is never reused for it. Files the set needs and
/// the vector has not applied are then marked on a copy.
/// </para>
/// </remarks>
internal sealed class LsmDeleteVector
{
    private readonly ulong[]?[] _chunks;

    private LsmDeleteVector(ulong[]?[] chunks, long[] applied, long horizon)
    {
        _chunks = chunks;
        Applied = applied;
        Horizon = horizon;
    }

    /// <summary>The file numbers, ascending, whose keys are marked.</summary>
    public long[] Applied { get; }

    /// <summary>The highest file number live in the set of files this vector was made for.</summary>
    public long Horizon { get; }

    /// <summary>The rows to leave out of a chunk, a bit per row; false when none.</summary>
    public bool TryGet(int chunk, out ReadOnlySpan<ulong> shadowed)
    {
        ulong[]? mask = _chunks[chunk];
        shadowed = mask;
        return mask is not null;
    }

    /// <summary>
    /// Whether this vector can be the start of the one for a set of files whose numbers are <paramref name="live"/>
    /// (ascending; the last is the highest), where <paramref name="needed"/> lists (ascending) the live files newer
    /// than this file that overlap it.
    /// </summary>
    public bool CanStart(long[] live, long[] needed)
    {
        long horizon = live[^1];
        foreach (long file in Applied)
        {
            if (Array.BinarySearch(live, file) >= 0 ? Array.BinarySearch(needed, file) < 0 : file >= horizon)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Collects marks for a vector, starting from an earlier one without changing it.</summary>
    internal sealed class Builder
    {
        private readonly ulong[]?[] _chunks;
        private readonly bool[] _owned;

        /// <summary>Starts a vector for a segment of <paramref name="chunkCount"/> chunks, from <paramref name="start"/> if given.</summary>
        public Builder(int chunkCount, LsmDeleteVector? start)
        {
            _chunks = start is null ? new ulong[]?[chunkCount] : (ulong[]?[])start._chunks.Clone();
            _owned = new bool[chunkCount];
        }

        /// <summary>Marks a row of a chunk as one to leave out.</summary>
        public void Mark(int chunk, int row)
        {
            ulong[]? mask = _chunks[chunk];
            if (!_owned[chunk])
            {
                // Copy on first write: the earlier vector stays as it was.
                mask = mask is null ? new ulong[Bitmask.WordsPerChunk] : (ulong[])mask.Clone();
                _chunks[chunk] = mask;
                _owned[chunk] = true;
            }

            Bitmask.Set(mask!, row);
        }

        /// <summary>The finished vector, recording which files it applied and the set's highest file number.</summary>
        public LsmDeleteVector Build(long[] applied, long horizon) => new(_chunks, applied, horizon);
    }
}
