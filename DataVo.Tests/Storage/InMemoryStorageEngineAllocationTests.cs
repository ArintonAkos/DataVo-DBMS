using DataVo.Core.StorageEngine.Memory;

namespace DataVo.Tests.Storage;

public class InMemoryStorageEngineAllocationTests
{
    [Fact]
    public void ReadRow_WarmCalls_DoesNotAllocate()
    {
        // In-memory ReadRow returns the stored byte[] by reference, so a warm read should allocate nothing.
        // The only per-call allocation was GetKey's "db.table" string interpolation, which scales with name
        // length (a value-tuple key removes it). Long names make the regression unmissable.
        var engine = new InMemoryStorageEngine();
        const string db = "Database_With_A_Reasonably_Descriptive_Name";
        const string table = "Customers_Transactions_Archive";
        long rowId = engine.InsertRow(db, table, [1, 2, 3, 4, 5, 6, 7, 8]);

        for (int i = 0; i < 200; i++) _ = engine.ReadRow(db, table, rowId);

        const int n = 10_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++) _ = engine.ReadRow(db, table, rowId);
        long perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        Assert.True(perCall <= 8, $"ReadRow per-call {perCall} B exceeds 8 B (per-row GetKey string not eliminated)");
    }
}
