using System.Buffers.Binary;
using System.Text;

namespace DataVo.Core.Utils;

/// <summary>
/// Maps UTF-8 byte strings to integers without a <see cref="string"/> per lookup. Keys are copied once, into one
/// shared byte buffer, when they are added; a lookup hashes and compares the caller's span in place. It lets a
/// scan group or join stored text by its bytes instead of decoding every row's value.
/// </summary>
/// <remarks>
/// Valid UTF-8 is equal byte for byte exactly when the decoded strings are equal ordinally, which is how the
/// engine compares text keys, so a table keyed by stored bytes groups exactly as one keyed by strings. The hash is
/// seeded once per process, so key sets cannot be chosen in advance to collide.
/// </remarks>
/// <example>
/// <code>
/// var groups = new Utf8KeyTable();
/// if (!groups.TryGetValue(utf8, out int group))
/// {
///     group = groups.Count;
///     groups.Add(utf8, group);
/// }
/// </code>
/// </example>
internal sealed class Utf8KeyTable
{
    private static readonly ulong Seed = NewSeed();

    private int[] _buckets = new int[16];
    private Entry[] _entries = new Entry[16];
    private byte[] _bytes = new byte[256];
    private int _used;

    private struct Entry
    {
        public int Offset;
        public int Length;
        public int Hash;
        public int Next; // index + 1 of the next entry in the bucket, 0 at the end
        public int Value;
    }

    /// <summary>The number of keys.</summary>
    public int Count { get; private set; }

    /// <summary>Finds the value of <paramref name="key"/>.</summary>
    public bool TryGetValue(ReadOnlySpan<byte> key, out int value)
    {
        int hash = Hash(key);
        for (int at = _buckets[hash & (_buckets.Length - 1)]; at != 0; at = _entries[at - 1].Next)
        {
            ref Entry entry = ref _entries[at - 1];
            if (entry.Hash == hash && key.SequenceEqual(_bytes.AsSpan(entry.Offset, entry.Length)))
            {
                value = entry.Value;
                return true;
            }
        }

        value = 0;
        return false;
    }

    /// <summary>Adds a key the table does not hold yet.</summary>
    public void Add(ReadOnlySpan<byte> key, int value)
    {
        if (Count == _entries.Length)
        {
            Grow();
        }

        if (_used + key.Length > _bytes.Length)
        {
            Array.Resize(ref _bytes, Math.Max(_bytes.Length * 2, _used + key.Length));
        }

        key.CopyTo(_bytes.AsSpan(_used));
        int hash = Hash(key);
        int bucket = hash & (_buckets.Length - 1);
        _entries[Count] = new Entry { Offset = _used, Length = key.Length, Hash = hash, Next = _buckets[bucket], Value = value };
        _buckets[bucket] = ++Count;
        _used += key.Length;
    }

    /// <summary>
    /// Writes the strict UTF-8 form of <paramref name="text"/> into <paramref name="buffer"/> (grown as needed) and
    /// returns its length, or -1 when the text is not valid Unicode (a lone surrogate) and has no exact byte form.
    /// </summary>
    public static int TryEncode(string text, ref byte[] buffer)
    {
        int max = Encoding.UTF8.GetMaxByteCount(text.Length);
        if (buffer.Length < max)
        {
            buffer = new byte[Math.Max(max, buffer.Length * 2)];
        }

        try
        {
            return StrictUtf8.GetBytes(text.AsSpan(), buffer);
        }
        catch (EncoderFallbackException)
        {
            return -1;
        }
    }

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private void Grow()
    {
        Array.Resize(ref _entries, _entries.Length * 2);
        _buckets = new int[_entries.Length];
        for (int i = 0; i < Count; i++)
        {
            int bucket = _entries[i].Hash & (_buckets.Length - 1);
            _entries[i].Next = _buckets[bucket];
            _buckets[bucket] = i + 1;
        }
    }

    /// <summary>A seeded hash of the key, eight bytes at a time, finished with the SplitMix64 mixer.</summary>
    private static int Hash(ReadOnlySpan<byte> key)
    {
        ulong hash = Seed ^ ((ulong)key.Length * 0x9E3779B97F4A7C15UL);
        while (key.Length >= 8)
        {
            hash = Mix(hash ^ BinaryPrimitives.ReadUInt64LittleEndian(key));
            key = key[8..];
        }

        ulong tail = 0;
        for (int i = 0; i < key.Length; i++)
        {
            tail |= (ulong)key[i] << (8 * i);
        }

        hash = Mix(hash ^ tail);
        return (int)(hash ^ (hash >> 32));
    }

    private static ulong Mix(ulong value)
    {
        value *= 0xBF58476D1CE4E5B9UL;
        value ^= value >> 31;
        value *= 0x94D049BB133111EBUL;
        return value ^ (value >> 29);
    }

    private static ulong NewSeed()
    {
        byte[] bytes = Guid.NewGuid().ToByteArray();
        return BinaryPrimitives.ReadUInt64LittleEndian(bytes) ^ BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8));
    }
}
