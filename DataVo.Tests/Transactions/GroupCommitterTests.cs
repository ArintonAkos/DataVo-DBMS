using System.Buffers.Binary;
using DataVo.Core.Transactions;

namespace DataVo.Tests.Transactions;

public sealed class GroupCommitterTests
{
    [Fact]
    public async Task CommitAsync_DrainsCurrentlyPendingFramesIntoOneDurableBatch()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo_group_commit_{Guid.NewGuid():N}");
        string walPath = Path.Combine(root, "datavo.walbin");

        try
        {
            var appender = new WalAppender(capacityBytes: 4096);
            var store = new WalFileStore(walPath);
            await using var committer = new GroupCommitter(store, batchDelay: TimeSpan.FromMilliseconds(50));

            Task first = CommitPayloadAsync(appender, committer, WalFrameOperationType.Insert, tableId: 7, rowId: 1, "one"u8.ToArray());
            Task second = CommitPayloadAsync(appender, committer, WalFrameOperationType.Update, tableId: 7, rowId: 2, "two"u8.ToArray());
            Task third = CommitPayloadAsync(appender, committer, WalFrameOperationType.Delete, tableId: 7, rowId: 3, "three"u8.ToArray());

            await Task.WhenAll(first, second, third);

            Assert.Equal(3, committer.DurableLsn);
            Assert.Equal(1, store.DurableFlushCount);

            byte[] persisted = File.ReadAllBytes(walPath);
            Assert.Equal(3, CountValidFrames(persisted));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task CommitAsync_WritesFramesInLsnOrderEvenWhenTheyArriveOutOfOrder()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo_group_commit_order_{Guid.NewGuid():N}");
        string walPath = Path.Combine(root, "datavo.walbin");

        try
        {
            var appender = new WalAppender(capacityBytes: 4096);
            var store = new WalFileStore(walPath);
            await using var committer = new GroupCommitter(store, batchDelay: TimeSpan.FromMilliseconds(10));

            WalFrame first = CreateFrame(appender, WalFrameOperationType.Insert, tableId: 1, rowId: 10, "first"u8);
            WalFrame second = CreateFrame(appender, WalFrameOperationType.Insert, tableId: 1, rowId: 11, "second"u8);

            Task secondDurable = committer.CommitAsync(second).AsTask();
            Task firstDurable = committer.CommitAsync(first).AsTask();

            await Task.WhenAll(firstDurable, secondDurable);

            byte[] persisted = File.ReadAllBytes(walPath);
            Assert.True(WalAppender.TryReadFrameHeader(persisted, out WalFrameHeader firstHeader));
            Assert.Equal(1, firstHeader.Lsn);

            Assert.True(WalAppender.TryReadFrameHeader(persisted.AsSpan(firstHeader.FrameLength), out WalFrameHeader secondHeader));
            Assert.Equal(2, secondHeader.Lsn);
            Assert.Equal(2, committer.DurableLsn);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    private static async Task CommitPayloadAsync(
        WalAppender appender,
        GroupCommitter committer,
        WalFrameOperationType opType,
        int tableId,
        long rowId,
        ReadOnlyMemory<byte> payload)
    {
        WalFrame frame = CreateFrame(appender, opType, tableId, rowId, payload.Span);
        await committer.CommitAsync(frame);
    }

    private static WalFrame CreateFrame(
        WalAppender appender,
        WalFrameOperationType opType,
        int tableId,
        long rowId,
        ReadOnlySpan<byte> payload)
    {
        WalFrameReservation reservation = appender.Reserve(opType, tableId, rowId, payload.Length);
        payload.CopyTo(reservation.PayloadSpan);
        return reservation.Commit();
    }

    private static int CountValidFrames(ReadOnlySpan<byte> bytes)
    {
        int count = 0;
        int offset = 0;
        while (offset < bytes.Length)
        {
            Assert.True(WalAppender.TryReadFrameHeader(bytes[offset..], out WalFrameHeader header));
            Assert.True(WalAppender.ValidateFrame(bytes[offset..], header));
            Assert.Equal(header.FrameLength, BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(offset, sizeof(int))));
            count++;
            offset += header.FrameLength;
        }

        return count;
    }
}
