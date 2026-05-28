using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public sealed record PlayerProjection(int Id, string Name, int Level);

public class CompiledQueryRuntimeTests
{
    [Fact]
    public void CompiledSelectSingle_ByPrimaryKey_ReturnsTypedResultWithoutSqlExecute()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.BulkInsert(
            "Players",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 5 }
            ]);

        var plan = DataVoCompiledQueryPlan.SelectSingle(
            tableName: "Players",
            projectedColumns: ["Id", "Name", "Level"],
            whereColumn: "Id",
            parameterName: "id");

        PlayerProjection? player = DataVoCompiledQuery.SelectSingle(
            context,
            plan,
            [new DataVoCompiledQueryParameter("id", 1)],
            static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!));

        Assert.Equal(new PlayerProjection(1, "Ada", 5), player);
    }

    [Fact]
    public void CompiledInsert_InsertsRowAndReturnsRowId()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Telemetry (Id INT PRIMARY KEY, EventName VARCHAR(50), Frame INT)");

        IReadOnlyList<long> ids = DataVoCompiledQuery.Insert(
            context,
            DataVoCompiledQueryPlan.Insert(
                tableName: "Telemetry",
                columns: ["Id", "EventName", "Frame"],
                parameterNames: ["id", "eventName", "frame"]),
            [
                new DataVoCompiledQueryParameter("id", 1),
                new DataVoCompiledQueryParameter("eventName", "level_start"),
                new DataVoCompiledQueryParameter("frame", 10)
            ]);

        Assert.Equal([1L], ids);
    }

    [Fact]
    public void CompiledUpdate_UpdatesRowsAndReturnsAffectedCount()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Level INT)");
        context.Execute("INSERT INTO Players VALUES (1, 5)");

        var plan = DataVoCompiledQueryPlan.Update(
            tableName: "Players",
            assignments: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Level"] = "level"
            },
            whereColumn: "Id",
            whereParameterName: "id");

        int affected = DataVoCompiledQuery.Update(
            context,
            plan,
            [
                new DataVoCompiledQueryParameter("id", 1),
                new DataVoCompiledQueryParameter("level", 7)
            ]);

        Assert.Equal(1, affected);
        Assert.Equal(7, (int)context.Execute("SELECT Level FROM Players WHERE Id = 1").Single().Data.Single()["Level"]!);
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string databaseName = $"Compiled_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }
}
