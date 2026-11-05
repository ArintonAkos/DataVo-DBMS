using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>
/// Cache-line-blocked Bloom filter over 64-bit row ids, operating directly on its serialized bytes so
/// that a memory-mapped SSTable can be probed without copying the filter into the heap.
/// </summary>
/// <remarks>
/// A classic Bloom filter spreads a key's k probe bits over the whole bit array, so a lookup touches up
/// to k random cache lines (the filter of a 1M-key file is 1.25 MB, larger than L1/L2). Here the key's
/// hash first selects one 64-byte block and all probes fall inside it: a lookup costs one cache miss.
/// The price is a slightly higher false-positive rate for the same memory (~1.2% at 10 bits/key, k=6).
/// Bits are stored as little-endian 64-bit words, so files are portable across architectures.
/// </remarks>
internal static class BlockedBloomFilter
{
    /// <summary>Bytes per block: one x64 cache line (half an Apple Silicon line).</summary>
    public const int BlockBytes = 64;

    /// <summary>Probes per key, all inside one block.</summary>
    public const int ProbeCount = 6;

    private const int BlockBits = BlockBytes * 8;

    /// <summary>Blocks needed for <paramref name="keyCount"/> keys at <paramref name="bitsPerKey"/>.</summary>
    public static int ComputeBlockCount(long keyCount, int bitsPerKey)
    {
        if (bitsPerKey <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bitsPerKey));
        }

        long bits = Math.Max(1, keyCount) * bitsPerKey;
        long blocks = (bits + BlockBits - 1) / BlockBits;
        return (int)Math.Min(Math.Max(1, blocks), int.MaxValue / BlockBytes);
    }

    /// <summary>Sets the probe bits of <paramref name="key"/> in <paramref name="blocks"/>.</summary>
    public static void Add(Span<byte> blocks, long key)
    {
        int blockCount = blocks.Length / BlockBytes;
        ulong hash = Mix(key);
        Span<byte> block = blocks.Slice(SelectBlock(hash, blockCount) * BlockBytes, BlockBytes);
        uint probe = (uint)hash;
        for (int i = 0; i < ProbeCount; i++)
        {
            int bit = (int)(probe >> 23); // top 9 bits: 0..511
            Span<byte> word = block.Slice((bit >> 6) * sizeof(ulong), sizeof(ulong));
            BinaryPrimitives.WriteUInt64LittleEndian(word, BinaryPrimitives.ReadUInt64LittleEndian(word) | (1UL << (bit & 63)));
            probe *= 0x9E3779B9u;
        }
    }

    /// <summary>False when <paramref name="key"/> is certainly absent; true when it may be present.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool MightContain(ReadOnlySpan<byte> blocks, long key)
    {
        int blockCount = blocks.Length / BlockBytes;
        if (blockCount == 0)
        {
            return true;
        }

        ulong hash = Mix(key);
        ReadOnlySpan<byte> block = blocks.Slice(SelectBlock(hash, blockCount) * BlockBytes, BlockBytes);
        uint probe = (uint)hash;
        for (int i = 0; i < ProbeCount; i++)
        {
            int bit = (int)(probe >> 23);
            ulong word = BinaryPrimitives.ReadUInt64LittleEndian(block.Slice((bit >> 6) * sizeof(ulong), sizeof(ulong)));
            if ((word & (1UL << (bit & 63))) == 0)
            {
                return false;
            }

            probe *= 0x9E3779B9u;
        }

        return true;
    }

    /// <summary>Maps the high hash bits onto [0, blockCount) without a division (Lemire's reduction).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SelectBlock(ulong hash, int blockCount) =>
        (int)(((hash >> 32) * (ulong)blockCount) >> 32);

    /// <summary>SplitMix64 finalizer: dense row ids map to well-spread 64-bit hashes.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Mix(long key)
    {
        ulong z = unchecked((ulong)key + 0x9E3779B97F4A7C15UL);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }
}
