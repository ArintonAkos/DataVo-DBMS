namespace DataVo.Core.Memory.Flat;

/// <summary>
/// The object references that go with a slot sequence: a fixed number per slot, in chunks that line up with
/// the sequence's <see cref="FlatChunk"/>s. It is the managed counterpart of <see cref="ChunkDirectory"/>, for
/// the strings and arrays a flat record cannot hold. One writer at a time; readers take no lock. See
/// <see cref="ChunkTable{TChunk}"/> for what a reader may rely on.
/// </summary>
/// <example>
/// <code>
/// var references = new ReferenceDirectory(perSlot: 2, firstChunkSlots: 16);
/// references.EnsureSlots(slot + 1);
/// object?[] chunk = references.ChunkOf(slot);
/// chunk[references.BaseOf(slot) + 1] = "the slot's second reference";
/// </code>
/// </example>
internal sealed class ReferenceDirectory : ChunkTable<object?[]>
{
    /// <summary>Creates an empty directory.</summary>
    /// <param name="perSlot">The number of references each slot has.</param>
    /// <param name="firstChunkSlots">The number of slots the first chunk starts with, from 1 to <see cref="FlatChunk.Slots"/>.</param>
    public ReferenceDirectory(int perSlot, int firstChunkSlots)
        : base(firstChunkSlots)
    {
        if (perSlot <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(perSlot));
        }

        PerSlot = perSlot;
    }

    /// <summary>The number of references each slot has.</summary>
    public int PerSlot { get; }

    /// <summary>The index, in the chunk that holds a slot, of the slot's first reference.</summary>
    public int BaseOf(int slot) => (slot & FlatChunk.SlotMask) * PerSlot;

    /// <inheritdoc />
    protected override object?[] Allocate(int capacity) => new object?[capacity * PerSlot];

    /// <inheritdoc />
    protected override void Copy(object?[] from, object?[] to) => from.CopyTo(to, 0);
}
