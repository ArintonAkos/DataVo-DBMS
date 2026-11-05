using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using DataVo.Core.Transactions;

namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// Buffered little-endian writer that keeps a running CRC32C of everything written, so a file can end
/// with one checksum over its whole content.
/// </summary>
internal sealed class ChecksummedWriter
{
    private readonly Stream _stream;
    private readonly byte[] _buffer = new byte[64 * 1024];
    private int _used;
    private uint _crc = WalCrc32C.Seed;

    public ChecksummedWriter(Stream stream) => _stream = stream;

    public void WriteInt32(int value) => BinaryPrimitives.WriteInt32LittleEndian(Reserve(sizeof(int)), value);

    public void WriteInt64(long value) => BinaryPrimitives.WriteInt64LittleEndian(Reserve(sizeof(long)), value);

    public void WriteDouble(double value) => WriteInt64(BitConverter.DoubleToInt64Bits(value));

    public void WriteBoolean(bool value) => Reserve(1)[0] = value ? (byte)1 : (byte)0;

    public void WriteString(string value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        WriteInt32(bytes.Length);
        WriteBytes(bytes);
    }

    /// <summary>Writes the raw bytes of a primitive array (the host must be little-endian).</summary>
    public void WriteSpan<T>(ReadOnlySpan<T> values) where T : unmanaged => WriteBytes(MemoryMarshal.AsBytes(values));

    public void WriteBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length <= _buffer.Length - _used)
        {
            bytes.CopyTo(_buffer.AsSpan(_used));
            _used += bytes.Length;
            return;
        }

        FlushBuffer();
        _crc = WalCrc32C.Append(_crc, bytes);
        _stream.Write(bytes);
    }

    /// <summary>Writes the checksum of everything before it and flushes the buffer.</summary>
    public void Finish()
    {
        FlushBuffer(); // the buffered tail must be in the checksum
        uint checksum = ~_crc;
        Span<byte> trailer = stackalloc byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32LittleEndian(trailer, checksum);
        _stream.Write(trailer);
    }

    private Span<byte> Reserve(int length)
    {
        if (_buffer.Length - _used < length)
        {
            FlushBuffer();
        }

        Span<byte> slot = _buffer.AsSpan(_used, length);
        _used += length;
        return slot;
    }

    private void FlushBuffer()
    {
        if (_used == 0)
        {
            return;
        }

        _crc = WalCrc32C.Append(_crc, _buffer.AsSpan(0, _used));
        _stream.Write(_buffer, 0, _used);
        _used = 0;
    }
}

/// <summary>Reader for <see cref="ChecksummedWriter"/> output; <see cref="VerifyEnd"/> checks the trailer.</summary>
internal sealed class ChecksummedReader
{
    private readonly Stream _stream;
    private uint _crc = WalCrc32C.Seed;

    public ChecksummedReader(Stream stream) => _stream = stream;

    public int ReadInt32()
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        ReadBytes(bytes);
        return BinaryPrimitives.ReadInt32LittleEndian(bytes);
    }

    public long ReadInt64()
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        ReadBytes(bytes);
        return BinaryPrimitives.ReadInt64LittleEndian(bytes);
    }

    public double ReadDouble() => BitConverter.Int64BitsToDouble(ReadInt64());

    public bool ReadBoolean()
    {
        Span<byte> bytes = stackalloc byte[1];
        ReadBytes(bytes);
        return bytes[0] switch
        {
            0 => false,
            1 => true,
            _ => throw new InvalidDataException("Snapshot contains an invalid boolean.")
        };
    }

    public string ReadString(int maxBytes)
    {
        int length = ReadInt32();
        if (length < 0 || length > maxBytes)
        {
            throw new InvalidDataException("Snapshot contains an invalid string length.");
        }

        byte[] bytes = new byte[length];
        ReadBytes(bytes);
        return Encoding.UTF8.GetString(bytes);
    }

    public void ReadSpan<T>(Span<T> destination) where T : unmanaged => ReadBytes(MemoryMarshal.AsBytes(destination));

    public void ReadBytes(Span<byte> destination)
    {
        Span<byte> remaining = destination;
        while (remaining.Length > 0)
        {
            int read = _stream.Read(remaining);
            if (read <= 0)
            {
                throw new EndOfStreamException("Snapshot ends early.");
            }

            remaining = remaining[read..];
        }

        _crc = WalCrc32C.Append(_crc, destination);
    }

    /// <summary>Reads the trailer and throws unless it matches the content and ends the stream.</summary>
    public void VerifyEnd()
    {
        uint expected = ~_crc;
        Span<byte> trailer = stackalloc byte[sizeof(uint)];
        ReadBytes(trailer);
        if (BinaryPrimitives.ReadUInt32LittleEndian(trailer) != expected)
        {
            throw new InvalidDataException("Snapshot checksum mismatch.");
        }

        Span<byte> probe = stackalloc byte[1];
        if (_stream.Read(probe) != 0)
        {
            throw new InvalidDataException("Snapshot has trailing bytes.");
        }
    }
}
