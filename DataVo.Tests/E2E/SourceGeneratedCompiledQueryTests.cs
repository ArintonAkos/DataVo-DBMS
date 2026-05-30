using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public sealed record GeneratedPlayer(int Id, string Name, int Level);

public static partial class GeneratedGameQueries
{
    [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
    public static partial GeneratedPlayer? GetPlayer(DataVoContext db, int id);

    [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Name = @name")]
    public static partial IReadOnlyList<GeneratedPlayer> GetPlayersByName(DataVoContext db, string name);

    [DataVoQuery("INSERT INTO Telemetry (Id, EventName, Frame) VALUES (@id, @eventName, @frame)")]
    public static partial IReadOnlyList<long> InsertTelemetry(DataVoContext db, int id, string eventName, int frame);

    [DataVoQuery("UPDATE Players SET Level = @level WHERE Id = @id")]
    public static partial int SetPlayerLevel(DataVoContext db, int id, int level);
}

public class SourceGeneratedCompiledQueryTests
{
    [Fact]
    public void GeneratedSelect_ReturnsTypedRecord()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.Execute("INSERT INTO Players VALUES (1, 'Ada', 5)");

        GeneratedPlayer? player = GeneratedGameQueries.GetPlayer(context, 1);

        Assert.Equal(new GeneratedPlayer(1, "Ada", 5), player);
    }

    [Fact]
    public void GeneratedSelectMany_ReturnsTypedRecords()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.Execute("INSERT INTO Players VALUES (1, 'Ada', 5)");
        context.Execute("INSERT INTO Players VALUES (2, 'Grace', 8)");
        context.Execute("INSERT INTO Players VALUES (3, 'Ada', 9)");

        IReadOnlyList<GeneratedPlayer> players = GeneratedGameQueries.GetPlayersByName(context, "Ada");

        Assert.Equal(
            [new GeneratedPlayer(1, "Ada", 5), new GeneratedPlayer(3, "Ada", 9)],
            players);
    }

    [Fact]
    public void GeneratedInsert_InsertsTelemetry()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Telemetry (Id INT PRIMARY KEY, EventName VARCHAR(50), Frame INT)");

        IReadOnlyList<long> ids = GeneratedGameQueries.InsertTelemetry(context, 1, "level_start", 10);

        Assert.Equal([1L], ids);
        Assert.Single(context.Execute("SELECT * FROM Telemetry WHERE Id = 1").Single().Data);
    }

    [Fact]
    public void GeneratedUpdate_UpdatesPlayer()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.Execute("INSERT INTO Players VALUES (1, 'Ada', 5)");

        int affected = GeneratedGameQueries.SetPlayerLevel(context, 1, 9);

        Assert.Equal(1, affected);
        Assert.Equal(9, (int)context.Execute("SELECT Level FROM Players WHERE Id = 1").Single().Data.Single()["Level"]!);
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string databaseName = $"Generated_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }
}
