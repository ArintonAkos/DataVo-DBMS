using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Tests.Storage;

public class RowSerializerAllocationTests
{
    [Fact]
    public void SerializeCells_WarmCalls_AllocateAtMostPayloadPlusSlack()
    {
        IReadOnlyList<Column> columns =
        [
            new Column { Name = "Id", Type = "INT" },
            new Column { Name = "AccountId", Type = "INT" },
            new Column { Name = "MarketId", Type = "INT" },
            new Column { Name = "Stake", Type = "INT" },
        ];
        CellValue[] cells = [CellValue.From(1), CellValue.From(2), CellValue.From(3), CellValue.From(4)];

        for (int i = 0; i < 200; i++) _ = RowSerializer.SerializeCells(columns, cells);

        const int n = 5_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++) _ = RowSerializer.SerializeCells(columns, cells);
        long perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        // Payload is ~1 byte null-flag + 4 bytes per INT over 4 cols (~20B) + the byte[] header.
        Assert.True(perCall <= 96, $"SerializeCells per-call {perCall} B exceeds 96 B (stream churn not eliminated)");
    }
}
