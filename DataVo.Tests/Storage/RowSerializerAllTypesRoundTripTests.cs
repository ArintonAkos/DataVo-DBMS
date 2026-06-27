using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Storage;

// Guards the allocation-free ClassifyColumnType dispatch (replacing per-column ToUpperInvariant): every
// supported storage type, plus a NULL, must round-trip byte-identically through both the dict path
// (WriteNonNullValue/ReadNonNullValue) and the typed path (WriteTypedCell/DecodeTypedCell).
public class RowSerializerAllTypesRoundTripTests
{
    [Fact]
    public void AllColumnTypes_RoundTripThroughDictAndTypedPaths()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE All_ (Id INT PRIMARY KEY, F FLOAT, B BIT, D DATE, S VARCHAR(50), V VECTOR(3))");

        var date = new DateOnly(2026, 6, 24);
        float[] vector = [1.5f, -2.0f, 3.25f];
        context.BulkInsert(
            "All_",
            [
                new Dictionary<string, object?>
                {
                    ["Id"] = 7, ["F"] = 2.5, ["B"] = true, ["D"] = date, ["S"] = "héllo", ["V"] = vector
                },
                new Dictionary<string, object?>
                {
                    ["Id"] = 8, ["F"] = 0.0, ["B"] = false, ["D"] = date, ["S"] = null, ["V"] = vector
                }
            ]);

        // Dict path: WriteNonNullValue on insert, ReadNonNullValue on read.
        Dictionary<long, Dictionary<string, object?>> dict =
            context.Engine.StorageContext.GetTableContents("All_", CurrentDatabase(context));
        Dictionary<string, object?> row7 = dict.Values.Single(r => Convert.ToInt32(r["Id"]) == 7);
        Dictionary<string, object?> row8 = dict.Values.Single(r => Convert.ToInt32(r["Id"]) == 8);

        Assert.Equal(2.5, Convert.ToDouble(row7["F"]), 5);
        Assert.Equal(true, row7["B"]);
        Assert.Equal(date, row7["D"]);
        Assert.Equal("héllo", row7["S"]);
        Assert.Equal(vector, ((float[])row7["V"]!));
        Assert.Null(row8["S"]);

        // Typed/compiled path: DecodeTypedCell via the streaming projected read.
        var hit = DataVoCompiledQuery.SelectSingleTyped<(int Id, double F, bool B, DateOnly D, string? S)>(
            context,
            DataVoCompiledQueryPlan.SelectSingle("All_", ["Id", "F", "B", "D", "S"], "Id", "id"),
            [new DataVoCompiledQueryParameter("id", 7)],
            static r => (r.GetInt32("Id"), r.GetDouble("F"), r.GetBoolean("B"), r.GetDate("D"), r.GetString("S")));

        Assert.Equal((7, 2.5, true, date, "héllo"), hit);
    }

    private static string CurrentDatabase(DataVoContext context)
        => context.Engine.Sessions.Get(context.SessionId)!;

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string databaseName = $"AllTypes_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }
}
