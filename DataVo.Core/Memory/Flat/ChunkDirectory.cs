namespace DataVo.Core.Memory.Flat;

/// <summary>
/// A growing sequence of <see cref="FlatChunk"/>s addressed by slot index. One writer at a time adds chunks;
/// any number of readers look slots up without a lock. See <see cref="ChunkTable{TChunk}"/> for what a reader
/// may rely on.
/// </summary>
internal sealed class ChunkDirectory : ChunkTable<FlatChunk>
{
    /// <summary>Creates an empty directory whose chunks are all full size.</summary>
    /// <param name="stride">The width of one slot in bytes.</param>
    public ChunkDirectory(int stride)
        : this(stride, FlatChunk.Slots)
    {
    }

    /// <summary>Creates an empty directory.</summary>
    /// <param name="stride">The width of one slot in bytes.</param>
    /// <param name="firstChunkSlots">The number of slots the first chunk starts with, from 1 to <see cref="FlatChunk.Slots"/>.</param>
    public ChunkDirectory(int stride, int firstChunkSlots)
        : base(firstChunkSlots)
    {
        if (stride <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(stride));
        }

        Stride = stride;
    }

    /// <summary>The width of one slot in bytes.</summary>
    public int Stride { get; }

    /// <inheritdoc />
    protected override FlatChunk Allocate(int capacity) => new(Stride, capacity);

    /// <inheritdoc />
    protected override void Copy(FlatChunk from, FlatChunk to) => from.Bytes.CopyTo(to.Bytes);
}
