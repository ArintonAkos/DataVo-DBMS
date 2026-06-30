using System.Buffers.Binary;
using System.Text;

namespace DataVo.Core.StorageEngine.Serialization;

/// <summary>
/// An allocation-free forward reader over a row's serialized bytes, decoding byte-identical to the format
/// <see cref="System.IO.BinaryWriter"/> writes: little-endian primitives, a 1-byte boolean, and a
/// 7-bit-length-prefixed UTF8 string. Replaces the per-row MemoryStream + BinaryReader in the hot read path.
/// </summary>
internal ref struct ByteSpanReader
{
    private readonly ReadOnlySpan<byte> _data;
    private int _position;

    public ByteSpanReader(ReadOnlySpan<byte> data)
    {
        _data = data;
        _position = 0;
    }

    public bool ReadBoolean() => _data[_position++] != 0;

    public int ReadInt32()
    {
        int value = BinaryPrimitives.ReadInt32LittleEndian(_data.Slice(_position, sizeof(int)));
        _position += sizeof(int);
        return value;
    }

    public long ReadInt64()
    {
        long value = BinaryPrimitives.ReadInt64LittleEndian(_data.Slice(_position, sizeof(long)));
        _position += sizeof(long);
        return value;
    }

    public Guid ReadGuid()
    {
        Guid value = new(_data.Slice(_position, 16));
        _position += 16;
        return value;
    }

    public string ReadString()
    {
        int length = Read7BitEncodedInt();
        string value = Encoding.UTF8.GetString(_data.Slice(_position, length));
        _position += length;
        return value;
    }

    public void SkipString()
    {
        // Two statements, not `_position += Read7BitEncodedInt()`: the compound assignment would read _position
        // before Read7BitEncodedInt advances it past the length prefix, losing the prefix advance.
        int length = Read7BitEncodedInt();
        _position += length;
    }

    public void Skip(int byteCount) => _position += byteCount;

    // Matches BinaryReader.Read7BitEncodedInt / BinaryWriter's length prefix (LEB128, max 5 bytes).
    private int Read7BitEncodedInt()
    {
        int result = 0;
        int shift = 0;
        byte current;
        do
        {
            current = _data[_position++];
            result |= (current & 0x7F) << shift;
            shift += 7;
        }
        while ((current & 0x80) != 0);

        return result;
    }
}
