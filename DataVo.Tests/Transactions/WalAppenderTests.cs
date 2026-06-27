using System.Buffers.Binary;
using DataVo.Core.Transactions;

namespace DataVo.Tests.Transactions;

public sealed class WalAppenderTests
{
    [Fact]
    public void Crc32C_MatchesKnownCastagnoliVector()
    {
        Assert.Equal(0xE3069283u, WalCrc32C.HashToUInt32("123456789"u8));
    }

    [Fact]
    public void Commit_WritesBinaryHeaderPayloadAndCrc32C()
    {
        var appender = new WalAppender(capacityBytes: 1024);
        var reservation = appender.Reserve(WalFrameOperationType.Insert, tableId: 17, rowId: 42, payloadLength: 5);

        "hello"u8.CopyTo(reservation.PayloadSpan);
        using WalFrame frame = reservation.Commit();

        ReadOnlySpan<byte> bytes = frame.Range.Span;
        Assert.Equal(WalAppender.FrameHeaderSize + 5, BinaryPrimitives.ReadInt32LittleEndian(bytes[..4]));
        Assert.Equal(frame.Lsn, BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(8, 8)));
        Assert.Equal((byte)WalFrameOperationType.Insert, bytes[16]);
        Assert.Equal(0, bytes[17]);
        Assert.Equal(42, BinaryPrimitives.ReadInt64LittleEndian(bytes.Slice(20, 8)));
        Assert.Equal(17, BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(28, 4)));
        Assert.True(WalAppender.TryReadFrameHeader(bytes, out WalFrameHeader header));
        Assert.True(WalAppender.ValidateFrame(bytes, header));
        Assert.Equal("hello"u8.ToArray(), bytes[WalAppender.FrameHeaderSize..].ToArray());
    }

    [Fact]
    public void Reserve_AssignsMonotonicLsns()
    {
        var appender = new WalAppender(capacityBytes: 1024);

        using WalFrame first = appender.Reserve(WalFrameOperationType.Insert, tableId: 1, rowId: 1, payloadLength: 0).Commit();
        using WalFrame second = appender.Reserve(WalFrameOperationType.Delete, tableId: 1, rowId: 1, payloadLength: 0).Commit();

        Assert.Equal(first.Lsn + 1, second.Lsn);
    }

    [Fact]
    public void Reserve_ThrowsWhenRingHasNoContiguousCapacity()
    {
        var appender = new WalAppender(capacityBytes: WalAppender.FrameHeaderSize + 4);
        using WalFrame frame = appender.Reserve(WalFrameOperationType.Insert, tableId: 1, rowId: 1, payloadLength: 4).Commit();

        Assert.Throws<InvalidOperationException>(() =>
            appender.Reserve(WalFrameOperationType.Insert, tableId: 1, rowId: 2, payloadLength: 1));
    }

    [Fact]
    public void Dispose_ReleasesRingCapacity()
    {
        var appender = new WalAppender(capacityBytes: WalAppender.FrameHeaderSize + 4);
        using WalFrame first = appender.Reserve(WalFrameOperationType.Insert, tableId: 1, rowId: 1, payloadLength: 4).Commit();

        first.Dispose();

        using WalFrame second = appender.Reserve(WalFrameOperationType.Insert, tableId: 1, rowId: 2, payloadLength: 4).Commit();
        Assert.Equal(2, second.Lsn);
    }

    [Fact]
    public void WalFileStore_AppendBinaryFrame_PersistsExactFrameBytes()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo_wal_appender_{Guid.NewGuid():N}");
        string walPath = Path.Combine(root, "datavo.walbin");

        try
        {
            var appender = new WalAppender(capacityBytes: 1024);
            var reservation = appender.Reserve(WalFrameOperationType.Update, tableId: 9, rowId: 123, payloadLength: 3);
            reservation.PayloadSpan[0] = 0xAA;
            reservation.PayloadSpan[1] = 0xBB;
            reservation.PayloadSpan[2] = 0xCC;
            using WalFrame frame = reservation.Commit();

            var store = new WalFileStore(walPath);
            store.AppendFrame(frame);

            byte[] persisted = File.ReadAllBytes(walPath);

            Assert.Equal(frame.Range.Span.ToArray(), persisted);
            Assert.True(WalAppender.TryReadFrameHeader(persisted, out WalFrameHeader header));
            Assert.True(WalAppender.ValidateFrame(persisted, header));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }
}
