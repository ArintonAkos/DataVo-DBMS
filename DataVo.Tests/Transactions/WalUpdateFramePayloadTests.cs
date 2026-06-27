using DataVo.Core.Transactions;

namespace DataVo.Tests.Transactions;

/// <summary>
/// Unit coverage for the binary <see cref="WalFrameOperationType.Update"/> frame payload codec: the
/// allocation-free writer (used on the hot path) and the recovery-side reader must round-trip exactly.
/// </summary>
public class WalUpdateFramePayloadTests
{
    [Fact]
    public void Write_Then_Read_RoundTripsAllFields()
    {
        const string db = "BenchDb";
        const string table = "Records";
        const long oldRowId = 4242;
        byte[] newRowBytes = [0x00, 0x2A, 0xFF, 0x10, 0x11, 0x12, 0x13];

        int size = WalUpdateFramePayload.MeasureSize(db, table, newRowBytes.Length);
        byte[] buffer = new byte[size];
        WalUpdateFramePayload.Write(buffer, db, table, oldRowId, newRowBytes);

        bool ok = WalUpdateFramePayload.TryRead(
            buffer, out string readDb, out string readTable, out long readRowId, out byte[] readRow);

        Assert.True(ok);
        Assert.Equal(db, readDb);
        Assert.Equal(table, readTable);
        Assert.Equal(oldRowId, readRowId);
        Assert.Equal(newRowBytes, readRow);
    }

    [Fact]
    public void MeasureSize_MatchesBytesWritten()
    {
        const string db = "d";
        const string table = "Records";
        byte[] row = new byte[40];

        int size = WalUpdateFramePayload.MeasureSize(db, table, row.Length);
        byte[] buffer = new byte[size + 8]; // oversized to prove Write reports exactly `size`
        int written = WalUpdateFramePayload.Write(buffer, db, table, oldRowId: 7, row);

        Assert.Equal(size, written);
    }

    [Fact]
    public void TryRead_ReturnsFalse_OnTruncatedPayload()
    {
        const string db = "BenchDb";
        const string table = "Records";
        byte[] row = [1, 2, 3, 4];

        int size = WalUpdateFramePayload.MeasureSize(db, table, row.Length);
        byte[] buffer = new byte[size];
        WalUpdateFramePayload.Write(buffer, db, table, oldRowId: 1, row);

        // A torn tail: feed back fewer bytes than the encoded row claims.
        bool ok = WalUpdateFramePayload.TryRead(
            buffer.AsSpan(0, size - 2), out _, out _, out _, out _);

        Assert.False(ok);
    }

    [Fact]
    public void TryRead_ReturnsFalse_OnUnknownVersion()
    {
        const string db = "BenchDb";
        const string table = "Records";
        byte[] row = [9, 9, 9];

        int size = WalUpdateFramePayload.MeasureSize(db, table, row.Length);
        byte[] buffer = new byte[size];
        WalUpdateFramePayload.Write(buffer, db, table, oldRowId: 1, row);
        buffer[0] = 0xEE; // corrupt the version byte

        bool ok = WalUpdateFramePayload.TryRead(buffer, out _, out _, out _, out _);

        Assert.False(ok);
    }
}
