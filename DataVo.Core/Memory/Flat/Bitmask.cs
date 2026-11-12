using System.Runtime.CompilerServices;

namespace DataVo.Core.Memory.Flat;

/// <summary>
/// Operations on a run of bits held in 64-bit words, bit 0 of word 0 first. One <see cref="FlatChunk"/> of slots
/// takes <see cref="WordsPerChunk"/> words. The same form serves as a validity mask (which slots are live), a
/// null mask (which values are missing) and a selection (which rows a condition accepted).
/// </summary>
internal static class Bitmask
{
    /// <summary>The words that cover one chunk of slots.</summary>
    public const int WordsPerChunk = FlatChunk.Slots / 64;

    /// <summary>The words needed to hold <paramref name="bits"/> bits.</summary>
    public static int WordsFor(int bits) => (bits + 63) >> 6;

    /// <summary>Sets one bit.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Set(Span<ulong> words, int bit) => words[bit >> 6] |= 1UL << (bit & 63);

    /// <summary>Clears one bit.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Clear(Span<ulong> words, int bit) => words[bit >> 6] &= ~(1UL << (bit & 63));

    /// <summary>Reads one bit.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool Test(ReadOnlySpan<ulong> words, int bit) => (words[bit >> 6] & (1UL << (bit & 63))) != 0;

    /// <summary>Counts the set bits.</summary>
    public static int Count(ReadOnlySpan<ulong> words)
    {
        int count = 0;
        for (int i = 0; i < words.Length; i++)
        {
            count += PopCount(words[i]);
        }

        return count;
    }

    /// <summary>Keeps in <paramref name="target"/> only the bits also set in <paramref name="other"/>.</summary>
    public static void And(Span<ulong> target, ReadOnlySpan<ulong> other)
    {
        for (int i = 0; i < target.Length; i++)
        {
            target[i] &= other[i];
        }
    }

    /// <summary>Adds to <paramref name="target"/> the bits set in <paramref name="other"/>.</summary>
    public static void Or(Span<ulong> target, ReadOnlySpan<ulong> other)
    {
        for (int i = 0; i < target.Length; i++)
        {
            target[i] |= other[i];
        }
    }

    /// <summary>Clears in <paramref name="target"/> the bits set in <paramref name="other"/>.</summary>
    public static void AndNot(Span<ulong> target, ReadOnlySpan<ulong> other)
    {
        for (int i = 0; i < target.Length; i++)
        {
            target[i] &= ~other[i];
        }
    }

    /// <summary>Flips every bit.</summary>
    public static void Not(Span<ulong> target)
    {
        for (int i = 0; i < target.Length; i++)
        {
            target[i] = ~target[i];
        }
    }

    /// <summary>Clears every bit from position <paramref name="bits"/> on.</summary>
    public static void TrimTo(Span<ulong> words, int bits)
    {
        int whole = bits >> 6;
        if (whole >= words.Length)
        {
            return;
        }

        int rest = bits & 63;
        if (rest != 0)
        {
            words[whole] &= (1UL << rest) - 1;
            whole++;
        }

        words.Slice(whole).Clear();
    }

    /// <summary>The number of set bits in one word.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int PopCount(ulong word)
    {
#if NET10_0_OR_GREATER
        return System.Numerics.BitOperations.PopCount(word);
#else
        word -= (word >> 1) & 0x5555_5555_5555_5555UL;
        word = (word & 0x3333_3333_3333_3333UL) + ((word >> 2) & 0x3333_3333_3333_3333UL);
        word = (word + (word >> 4)) & 0x0F0F_0F0F_0F0F_0F0FUL;
        return (int)((word * 0x0101_0101_0101_0101UL) >> 56);
#endif
    }

    /// <summary>The position of the lowest set bit of a word that is not zero.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int LowestSet(ulong word)
    {
#if NET10_0_OR_GREATER
        return System.Numerics.BitOperations.TrailingZeroCount(word);
#else
        return PopCount((word & (~word + 1)) - 1);
#endif
    }
}

/// <summary>
/// One bit per slot of a slot sequence, in flat chunks: bit <c>n</c> belongs to slot <c>n</c>. One writer at a
/// time; readers take no lock and look only at slots below a count they have read.
/// </summary>
internal sealed class BitmaskDirectory
{
    // A chunk of 4,096 words covers 64 chunks of slots.
    private readonly ChunkDirectory _words = new(sizeof(ulong));

    /// <summary>Makes sure the first <paramref name="slots"/> slots have a bit, initially clear. Called by the writer.</summary>
    public void EnsureSlots(int slots) => _words.EnsureSlots(Bitmask.WordsFor(slots));

    /// <summary>Sets a slot's bit. Called by the writer.</summary>
    public void Set(int slot) => Word(slot) |= 1UL << (slot & 63);

    /// <summary>Clears a slot's bit. Called by the writer.</summary>
    public void Clear(int slot) => Word(slot) &= ~(1UL << (slot & 63));

    /// <summary>Reads a slot's bit.</summary>
    public bool Test(int slot) => (Word(slot) & (1UL << (slot & 63))) != 0;

    /// <summary>The <see cref="Bitmask.WordsPerChunk"/> words for one chunk of slots.</summary>
    /// <param name="slotChunk">The slot chunk's number.</param>
    public Span<ulong> Words(int slotChunk) =>
        _words.Chunk(slotChunk >> 6).As<ulong>().Slice((slotChunk & 63) * Bitmask.WordsPerChunk, Bitmask.WordsPerChunk);

    private ref ulong Word(int slot)
    {
        int word = slot >> 6;
        return ref _words.ChunkOf(word).As<ulong>()[word & FlatChunk.SlotMask];
    }
}
