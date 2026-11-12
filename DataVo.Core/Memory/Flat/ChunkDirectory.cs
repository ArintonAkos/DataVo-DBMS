namespace DataVo.Core.Memory.Flat;

/// <summary>
/// A growing sequence of <see cref="FlatChunk"/>s addressed by slot index. One writer at a time adds chunks;
/// any number of readers look slots up without a lock.
/// </summary>
/// <remarks>
/// The directory holds no count. Its owner publishes one after filling a slot, and a reader looks only below
/// the count it read: the chunk for such a slot was stored, and the chunk table that names it published,
/// before that count was. Several directories can therefore share one count, as the columns of a table do.
/// </remarks>
internal sealed class ChunkDirectory
{
    private FlatChunk?[] _chunks = new FlatChunk?[4];
    private int _allocated;

    /// <summary>Creates an empty directory.</summary>
    /// <param name="stride">The width of one slot in bytes.</param>
    public ChunkDirectory(int stride)
    {
        if (stride <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(stride));
        }

        Stride = stride;
    }

    /// <summary>The width of one slot in bytes.</summary>
    public int Stride { get; }

    /// <summary>The number of slots that have memory. Read by the writer only.</summary>
    public int Capacity => _allocated << FlatChunk.Shift;

    /// <summary>The chunk with the given number: slots <c>number * Slots</c> onward.</summary>
    public FlatChunk Chunk(int number) => Volatile.Read(ref _chunks)[number]!;

    /// <summary>The chunk that holds a slot.</summary>
    public FlatChunk ChunkOf(int slot) => Chunk(slot >> FlatChunk.Shift);

    /// <summary>Makes sure the first <paramref name="slots"/> slots have memory. Called by the writer.</summary>
    public void EnsureSlots(int slots)
    {
        int needed = (slots + FlatChunk.SlotMask) >> FlatChunk.Shift;
        if (needed <= _allocated)
        {
            return;
        }

        FlatChunk?[] chunks = _chunks;
        if (needed > chunks.Length)
        {
            Array.Resize(ref chunks, Math.Max(needed, chunks.Length * 2));
        }

        for (int number = _allocated; number < needed; number++)
        {
            chunks[number] = new FlatChunk(Stride);
        }

        // Readers see the new table only once its chunks are in place.
        Volatile.Write(ref _chunks, chunks);
        _allocated = needed;
    }
}
