namespace DataVo.Core.Memory.Flat;

/// <summary>
/// An append-only list of fixed-width records in flat chunks. One writer at a time appends; readers take no
/// lock. A record is visible once <see cref="Publish"/> has returned, and never moves afterwards.
/// </summary>
internal sealed class FlatSlotList
{
    private readonly ChunkDirectory _chunks;
    private int _count;

    /// <summary>Creates an empty list.</summary>
    /// <param name="stride">The width of one record in bytes.</param>
    public FlatSlotList(int stride) => _chunks = new ChunkDirectory(stride);

    /// <summary>The width of one record in bytes.</summary>
    public int Stride => _chunks.Stride;

    /// <summary>The number of published records.</summary>
    public int Count => Volatile.Read(ref _count);

    /// <summary>The slot the next record goes in, not yet visible to readers. Called by the writer.</summary>
    public Span<byte> Next()
    {
        int index = _count;
        _chunks.EnsureSlots(index + 1);
        return _chunks.ChunkOf(index).Slot(index & FlatChunk.SlotMask);
    }

    /// <summary>Makes the record written to <see cref="Next"/> visible. Called by the writer.</summary>
    /// <returns>The record's index.</returns>
    public int Publish()
    {
        int index = _count;
        Volatile.Write(ref _count, index + 1);
        return index;
    }

    /// <summary>Reads a published record without a lock.</summary>
    /// <returns><see langword="false"/> when no record has that index yet.</returns>
    public bool TryRead(int index, out ReadOnlySpan<byte> record)
    {
        if ((uint)index >= (uint)Volatile.Read(ref _count))
        {
            record = default;
            return false;
        }

        record = _chunks.ChunkOf(index).Slot(index & FlatChunk.SlotMask);
        return true;
    }
}
