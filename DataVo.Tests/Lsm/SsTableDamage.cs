using System.Buffers.Binary;
using System.Runtime.InteropServices;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

/// <summary>Finds the parts of a version 8 SSTable and damages them, for the corruption tests.</summary>
internal static class SsTableDamage
{
    /// <summary>Where the parts of a version 8 file are, read from its footer and its segment.</summary>
    public readonly record struct Layout(long FirstChunk, long Tombstones, long Directory, int DirectoryLength, long Footer, SsTableColumnarChunk[] Chunks, SsTableColumnarColumn[] Columns);

    /// <summary>Reads where the parts of an intact version 8 file are.</summary>
    public static Layout LayoutOf(byte[] file)
    {
        long footer = file.Length - SsTableV2Format.ColumnarFooterSize;
        ReadOnlySpan<byte> handle = file.AsSpan((int)footer + (3 * SsTableV2Format.BlockHandleSize));
        long directory = BinaryPrimitives.ReadInt64LittleEndian(handle);
        int directoryLength = BinaryPrimitives.ReadInt32LittleEndian(handle[8..]);
        long tombstones = BinaryPrimitives.ReadInt64LittleEndian(file.AsSpan((int)directory + 32));
        using SsTableV2Reader reader = SsTableV2Reader.Open(new MemoryBlockSource(file, mapped: false), ownsSource: true);
        SsTableColumnarSegment segment = reader.Columnar!;
        SsTableColumnarChunk[] chunks = [.. Enumerable.Range(0, segment.ChunkCount).Select(c => segment.Chunk(c))];
        SsTableColumnarColumn[] columns = [.. Enumerable.Range(0, segment.ChunkCount).SelectMany(c => Enumerable.Range(0, segment.Spec.ColumnCount).Select(s => segment.Column(c, s)))];
        return new Layout(chunks[0].Offset, tombstones, directory, directoryLength, footer, chunks, columns);
    }

    /// <summary>A copy of <paramref name="file"/> with the bits of <paramref name="mask"/> flipped at <paramref name="offset"/>.</summary>
    public static byte[] Flip(byte[] file, long offset, byte mask)
    {
        byte[] copy = (byte[])file.Clone();
        copy[offset] ^= mask;
        return copy;
    }

    /// <summary>
    /// An SSTable held in memory, so thousands of damaged copies are read without touching the disk. In mapped
    /// mode it hands out addresses into a pinned array, as a memory mapping does; otherwise it copies, as
    /// positional reads do.
    /// </summary>
    public sealed class MemoryBlockSource : SsTableBlockSource
    {
        private readonly byte[] _bytes;
        private readonly bool _mapped;

        public MemoryBlockSource(byte[] bytes, bool mapped)
        {
            _mapped = mapped;
            _bytes = mapped ? GC.AllocateArray<byte>(bytes.Length, pinned: true) : bytes;
            if (mapped)
            {
                bytes.CopyTo(_bytes, 0);
            }
        }

        public override long Length => _bytes.Length;

        public override bool IsMapped => _mapped;

        public override ReadOnlyMemory<byte> GetRegion(long offset, int length)
        {
            ThrowIfOutOfRange(offset, length, _bytes.Length);
            return new ReadOnlyMemory<byte>(_bytes, (int)offset, length);
        }

        public override bool TryGetRegionAddress(long offset, int length, out IntPtr address)
        {
            address = IntPtr.Zero;
            if (!_mapped)
            {
                return false;
            }

            ThrowIfOutOfRange(offset, length, _bytes.Length);
            address = Marshal.UnsafeAddrOfPinnedArrayElement(_bytes, (int)offset);
            return true;
        }

        public override TResult Read<TState, TResult>(long offset, int length, TState state, SpanFunc<TState, TResult> func)
        {
            ThrowIfOutOfRange(offset, length, _bytes.Length);
            return func(_bytes.AsSpan((int)offset, length), state);
        }

        public override SsTableBlockLease ReadBlock(long offset, int length, int block, ISsTableBlockVerifier verifier)
        {
            ThrowIfOutOfRange(offset, length, _bytes.Length);
            byte[] copy = System.Buffers.ArrayPool<byte>.Shared.Rent(length);
            _bytes.AsSpan((int)offset, length).CopyTo(copy);
            try
            {
                verifier.VerifyBlock(block, copy.AsSpan(0, length), physicalRead: !_mapped);
            }
            catch
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(copy);
                throw;
            }

            return SsTableBlockLease.Pooled(copy, length, pool: null);
        }

        public override void Dispose()
        {
        }
    }
}
