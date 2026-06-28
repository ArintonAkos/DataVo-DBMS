using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class SsTableFormatTests
{
    [Fact]
    public void Constants_DefineFixedFooterLayout()
    {
        Assert.Equal(0x44564C53U, SsTableFormat.Magic);
        Assert.Equal((ushort)1, SsTableFormat.Version);
        Assert.Equal(12, SsTableFormat.BlockHandleSize);
        Assert.Equal(30, SsTableFormat.FooterSize);
    }

    [Fact]
    public void WriteFooter_UsesLittleEndianFixedLayout()
    {
        Span<byte> footer = stackalloc byte[SsTableFormat.FooterSize];
        var indexBlock = new SsTableBlockHandle(0x0102030405060708L, 0x0A0B0C0D);
        var filterBlock = new SsTableBlockHandle(0x1112131415161718L, 0x1A1B1C1D);

        SsTableFormat.WriteFooter(footer, indexBlock, filterBlock);

        byte[] expected =
        [
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
            0x0D, 0x0C, 0x0B, 0x0A,
            0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11,
            0x1D, 0x1C, 0x1B, 0x1A,
            0x01, 0x00,
            0x53, 0x4C, 0x56, 0x44,
        ];
        Assert.Equal(expected, footer.ToArray());
    }

    [Fact]
    public void TryReadFooter_RoundTripsBlockHandles()
    {
        byte[] sstable = new byte[4480 + SsTableFormat.FooterSize];
        var indexBlock = new SsTableBlockHandle(128, 4096);
        var filterBlock = new SsTableBlockHandle(4224, 256);
        SsTableFormat.WriteFooter(sstable.AsSpan(4480), indexBlock, filterBlock);

        bool ok = SsTableFormat.TryReadFooter(
            sstable,
            out SsTableBlockHandle decodedIndexBlock,
            out SsTableBlockHandle decodedFilterBlock);

        Assert.True(ok);
        Assert.Equal(indexBlock, decodedIndexBlock);
        Assert.Equal(filterBlock, decodedFilterBlock);
    }

    [Fact]
    public void TryReadFooter_ReadsFooterFromEndOfSstableBytes()
    {
        byte[] sstable = new byte[17 + SsTableFormat.FooterSize];
        var indexBlock = new SsTableBlockHandle(4, 8);
        var filterBlock = new SsTableBlockHandle(12, 5);
        SsTableFormat.WriteFooter(sstable.AsSpan(17), indexBlock, filterBlock);

        bool ok = SsTableFormat.TryReadFooter(
            sstable,
            out SsTableBlockHandle decodedIndexBlock,
            out SsTableBlockHandle decodedFilterBlock);

        Assert.True(ok);
        Assert.Equal(indexBlock, decodedIndexBlock);
        Assert.Equal(filterBlock, decodedFilterBlock);
    }

    [Fact]
    public void TryReadFooter_RejectsTruncatedFooter()
    {
        Span<byte> footer = stackalloc byte[SsTableFormat.FooterSize];
        SsTableFormat.WriteFooter(footer, new SsTableBlockHandle(0, 1), new SsTableBlockHandle(1, 1));

        bool ok = SsTableFormat.TryReadFooter(
            footer[..^1],
            out SsTableBlockHandle indexBlock,
            out SsTableBlockHandle filterBlock);

        Assert.False(ok);
        Assert.Equal(default, indexBlock);
        Assert.Equal(default, filterBlock);
    }

    [Fact]
    public void TryReadFooter_RejectsWrongMagic()
    {
        Span<byte> footer = stackalloc byte[SsTableFormat.FooterSize];
        SsTableFormat.WriteFooter(footer, new SsTableBlockHandle(0, 1), new SsTableBlockHandle(1, 1));
        footer[^1] ^= 0xFF;

        bool ok = SsTableFormat.TryReadFooter(footer, out _, out _);

        Assert.False(ok);
    }

    [Fact]
    public void TryReadFooter_RejectsUnsupportedVersion()
    {
        Span<byte> footer = stackalloc byte[SsTableFormat.FooterSize];
        SsTableFormat.WriteFooter(footer, new SsTableBlockHandle(0, 1), new SsTableBlockHandle(1, 1));
        footer[24] = 2;
        footer[25] = 0;

        bool ok = SsTableFormat.TryReadFooter(footer, out _, out _);

        Assert.False(ok);
    }

    [Theory]
    [InlineData(0, -1, 1, 1)]
    [InlineData(0, 1, -1, 1)]
    [InlineData(0, 0, 1, 1)]
    [InlineData(0, 1, 1, 0)]
    [InlineData(long.MaxValue, 1, 1, 1)]
    [InlineData(0, 1, long.MaxValue, 1)]
    public void TryReadFooter_RejectsInvalidBlockHandles(
        long indexOffset,
        int indexLength,
        long filterOffset,
        int filterLength)
    {
        Span<byte> footer = stackalloc byte[SsTableFormat.FooterSize];
        SsTableFormat.WriteFooter(
            footer,
            new SsTableBlockHandle(indexOffset, indexLength),
            new SsTableBlockHandle(filterOffset, filterLength));

        bool ok = SsTableFormat.TryReadFooter(footer, out _, out _);

        Assert.False(ok);
    }

    [Theory]
    [InlineData(20, 1, 4, 8)]
    [InlineData(4, 8, 20, 1)]
    [InlineData(0, 21, 4, 8)]
    [InlineData(4, 8, 0, 21)]
    public void TryReadFooter_RejectsBlockHandlesOutsideDataRegion(
        long indexOffset,
        int indexLength,
        long filterOffset,
        int filterLength)
    {
        byte[] sstable = new byte[20 + SsTableFormat.FooterSize];
        SsTableFormat.WriteFooter(
            sstable.AsSpan(20),
            new SsTableBlockHandle(indexOffset, indexLength),
            new SsTableBlockHandle(filterOffset, filterLength));

        bool ok = SsTableFormat.TryReadFooter(sstable, out _, out _);

        Assert.False(ok);
    }
}
