namespace DataVo.Core.Memory.Flat;

/// <summary>
/// A growing sequence of equal-sized chunks addressed by slot index, <see cref="FlatChunk.Slots"/> slots to a
/// chunk. One writer at a time adds chunks; any number of readers look slots up without a lock.
/// </summary>
/// <typeparam name="TChunk">What holds one chunk of slots.</typeparam>
/// <remarks>
/// The table holds no count. Its owner publishes one after filling a slot, and a reader looks only below the
/// count it read: the chunk for such a slot was stored, and the table that names it published, before that
/// count was. Several tables can therefore share one count, as the columns of a table do.
/// <para>
/// A table can be told to start its first chunk small, so that a sequence of a few slots does not pay for a
/// whole chunk. That chunk is then replaced by a larger copy each time it fills, until it is full size. A
/// reader that still holds the chunk that was replaced sees the slots below the count it read as they were at
/// the moment of replacement; changes made to those slots afterwards are in the copy.
/// </para>
/// </remarks>
internal abstract class ChunkTable<TChunk>
    where TChunk : class
{
    private const int FirstChunkGrowth = 4;

    private TChunk?[] _chunks = new TChunk?[4];
    private int _allocated;
    private int _firstCapacity;

    /// <summary>Creates an empty table.</summary>
    /// <param name="firstChunkSlots">The number of slots the first chunk starts with, from 1 to <see cref="FlatChunk.Slots"/>.</param>
    protected ChunkTable(int firstChunkSlots)
    {
        if (firstChunkSlots <= 0 || firstChunkSlots > FlatChunk.Slots)
        {
            throw new ArgumentOutOfRangeException(nameof(firstChunkSlots));
        }

        _firstCapacity = firstChunkSlots;
    }

    /// <summary>The number of slots that have memory. Read by the writer only.</summary>
    public int Capacity => _allocated == 0 ? 0 : ((_allocated - 1) << FlatChunk.Shift) + _firstCapacity;

    /// <summary>The chunk with the given number: slots <c>number * Slots</c> onward.</summary>
    public TChunk Chunk(int number) => Volatile.Read(ref _chunks)[number]!;

    /// <summary>The chunk that holds a slot.</summary>
    public TChunk ChunkOf(int slot) => Chunk(slot >> FlatChunk.Shift);

    /// <summary>Makes sure the first <paramref name="slots"/> slots have memory. Called by the writer.</summary>
    public void EnsureSlots(int slots)
    {
        int needed = (slots + FlatChunk.SlotMask) >> FlatChunk.Shift;
        if (needed > _allocated)
        {
            AddChunks(needed);
        }

        if (slots > _firstCapacity && _firstCapacity < FlatChunk.Slots)
        {
            GrowFirstChunk(Math.Min(slots, FlatChunk.Slots));
        }
    }

    /// <summary>Allocates a zeroed chunk of <paramref name="capacity"/> slots.</summary>
    protected abstract TChunk Allocate(int capacity);

    /// <summary>Copies every slot of a chunk into the same slots of a larger one.</summary>
    protected abstract void Copy(TChunk from, TChunk to);

    /// <summary>Allocates chunks up to <paramref name="needed"/> of them and publishes the array that names them.</summary>
    private void AddChunks(int needed)
    {
        TChunk?[] chunks = _chunks;
        if (needed > chunks.Length)
        {
            Array.Resize(ref chunks, Math.Max(needed, chunks.Length * 2));
        }

        for (int number = _allocated; number < needed; number++)
        {
            chunks[number] = Allocate(number == 0 ? _firstCapacity : FlatChunk.Slots);
        }

        // Readers see the new array only once its chunks are in place.
        Volatile.Write(ref _chunks, chunks);
        _allocated = needed;
    }

    /// <summary>Replaces the first chunk with a copy that has room for <paramref name="slots"/> slots.</summary>
    private void GrowFirstChunk(int slots)
    {
        int capacity = _firstCapacity;
        while (capacity < slots)
        {
            capacity = Math.Min(FlatChunk.Slots, capacity * FirstChunkGrowth);
        }

        TChunk grown = Allocate(capacity);
        Copy(_chunks[0]!, grown);

        // Readers see the copy only once it holds everything the old chunk did.
        Volatile.Write(ref _chunks[0], grown);
        _firstCapacity = capacity;
    }
}
