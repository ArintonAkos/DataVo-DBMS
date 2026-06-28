using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// An allocation-free Bloom filter over user primary keys, used to skip SSTables that cannot contain a
/// key during a point lookup. Backed by a single <see cref="byte"/> bit array; the <c>k</c> probe
/// positions are synthesized from one 64-bit FNV-1a hash via double hashing
/// (Kirsch–Mitzenmacher: <c>g_i = h1 + i·h2</c>), so a membership test computes exactly one hash and
/// performs no heap allocation.
/// </summary>
public sealed class BloomFilter
{
    // Serialized layout: [int32 bitCount][byte numProbes][3 bytes reserved][bitset...].
    private const int HeaderSize = 8;

    private readonly byte[] _bits;
    private readonly int _bitCount;
    private readonly int _numProbes;

    private BloomFilter(byte[] bits, int bitCount, int numProbes)
    {
        _bits = bits;
        _bitCount = bitCount;
        _numProbes = numProbes;
    }

    /// <summary>Number of addressable bits in the filter.</summary>
    public int BitCount => _bitCount;

    /// <summary>Number of probe positions tested per key.</summary>
    public int NumProbes => _numProbes;

    /// <summary>Builds an empty filter sized for <paramref name="expectedKeys"/> at the given bits-per-key.</summary>
    public static BloomFilter Create(int expectedKeys, int bitsPerKey = 10)
    {
        if (expectedKeys < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(expectedKeys));
        }

        if (bitsPerKey <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bitsPerKey));
        }

        int bitCount = Math.Max(64, expectedKeys * bitsPerKey);
        // Optimal probe count k = bitsPerKey * ln 2, clamped to a sane range.
        int numProbes = Math.Clamp((int)Math.Round(bitsPerKey * 0.69314718), 1, 30);
        var bits = new byte[(bitCount + 7) / 8];
        return new BloomFilter(bits, bitCount, numProbes);
    }

    /// <summary>Wraps a persisted header + bitset for read-time probing (used by the SSTable reader).</summary>
    public static BloomFilter FromBytes(ReadOnlySpan<byte> serialized)
    {
        if (serialized.Length < HeaderSize)
        {
            throw new ArgumentException(
                $"Bloom filter blob too small: {serialized.Length} bytes is shorter than the {HeaderSize}-byte header.",
                nameof(serialized));
        }

        int bitCount = BinaryPrimitives.ReadInt32LittleEndian(serialized);
        int numProbes = serialized[4];
        byte[] bits = serialized[HeaderSize..].ToArray();

        if (bitCount <= 0 || numProbes < 1 || (long)bits.Length * 8 < bitCount)
        {
            throw new ArgumentException(
                $"Corrupt Bloom filter: bitCount={bitCount}, numProbes={numProbes}, bitset={bits.Length} bytes.",
                nameof(serialized));
        }

        return new BloomFilter(bits, bitCount, numProbes);
    }

    /// <summary>Serializes the header + bitset for storage in an SSTable filter block.</summary>
    public byte[] ToBytes()
    {
        var buffer = new byte[HeaderSize + _bits.Length];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, _bitCount);
        buffer[4] = (byte)_numProbes;
        _bits.CopyTo(buffer.AsSpan(HeaderSize));
        return buffer;
    }

    /// <summary>Records <paramref name="key"/> as present. Allocation-free.</summary>
    public void Add(ReadOnlySpan<byte> key)
    {
        (uint h1, uint h2) = DoubleHash(key);
        for (int i = 0; i < _numProbes; i++)
        {
            int bit = (int)(unchecked(h1 + (uint)i * h2) % (uint)_bitCount);
            _bits[bit >> 3] |= (byte)(1 << (bit & 7));
        }
    }

    /// <summary>Returns false only if <paramref name="key"/> is definitely absent. Allocation-free.</summary>
    public bool MightContain(ReadOnlySpan<byte> key)
    {
        (uint h1, uint h2) = DoubleHash(key);
        for (int i = 0; i < _numProbes; i++)
        {
            int bit = (int)(unchecked(h1 + (uint)i * h2) % (uint)_bitCount);
            if ((_bits[bit >> 3] & (byte)(1 << (bit & 7))) == 0)
            {
                return false;
            }
        }

        return true;
    }

    private static (uint H1, uint H2) DoubleHash(ReadOnlySpan<byte> key)
    {
        ulong hash = Fnv1a64(key);
        uint h1 = (uint)(hash & 0xFFFFFFFF);
        uint h2 = (uint)(hash >> 32);
        if (h2 == 0)
        {
            h2 = 0x9E3779B1; // avoid a zero stride collapsing every probe onto h1
        }

        return (h1, h2);
    }

    private static ulong Fnv1a64(ReadOnlySpan<byte> data)
    {
        const ulong offsetBasis = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        ulong hash = offsetBasis;
        for (int i = 0; i < data.Length; i++)
        {
            hash ^= data[i];
            hash *= prime;
        }

        return hash;
    }
}
