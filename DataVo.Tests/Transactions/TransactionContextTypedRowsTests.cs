using DataVo.Core.Transactions;

namespace DataVo.Tests.Transactions;

public class TransactionContextTypedRowsTests
{
    [Fact]
    public void BufferInsert_ObjectRow_PreservesPrimitiveTypes()
    {
        var context = new TransactionContext();

        var row = new Dictionary<string, object?>
        {
            ["Id"] = 7,
            ["Name"] = "Alice",
            ["Score"] = 1.25f,
            ["IsActive"] = true
        };

        context.BufferInsert("Users", row);

        Assert.True(context.InsertedRows.TryGetValue("Users", out var rows));
        Assert.Single(rows!);
        Assert.IsType<int>(rows[0]["Id"]);
        Assert.IsType<string>(rows[0]["Name"]);
        Assert.IsType<float>(rows[0]["Score"]);
        Assert.IsType<bool>(rows[0]["IsActive"]);
    }

    [Fact]
    public void SavepointRollback_RestoresTypedBuffers()
    {
        var context = new TransactionContext();

        context.BufferInsert("Users", new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Alice" });
        context.CreateSavepoint("sp1");

        context.BufferInsert("Users", new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Bob" });
        context.BufferUpdate("Users", 1, new Dictionary<string, object?> { ["Name"] = "Alice Updated" });

        context.RollbackToSavepoint("sp1");

        Assert.True(context.InsertedRows.TryGetValue("Users", out var inserts));
        Assert.Single(inserts!);
        Assert.Equal(1, inserts[0]["Id"]);

        Assert.False(context.UpdatedRows.ContainsKey("Users"));
    }

    [Fact]
    public void WalEntry_FromTransactionContext_UsesVectorEnvelopeInOperationPayload()
    {
        var context = new TransactionContext { TransactionId = 42 };
        context.BufferInsert("Users", new Dictionary<string, object?>
        {
            ["Id"] = 10,
            ["Name"] = "Jane",
            ["Emb"] = new float[] { 0.1f, 0.2f }
        });

        WalEntry entry = WalEntry.FromTransactionContext("AppDb", context);

        Assert.Equal(42, entry.MvccTransactionId);
        Assert.Single(entry.Operations);

        Dictionary<string, object?> rowData = Assert.IsType<Dictionary<string, object?>>(entry.Operations[0].RowData);
        var embEnvelope = Assert.IsType<Dictionary<string, object>>(rowData["Emb"]);
        Assert.Equal("vector-f32b64-v1", embEnvelope["__dvType"]);
    }
}
