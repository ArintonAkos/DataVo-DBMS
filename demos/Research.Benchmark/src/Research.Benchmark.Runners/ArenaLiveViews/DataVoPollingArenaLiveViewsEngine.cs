using System.Globalization;
using System.Diagnostics;
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ArenaLiveViews;

public sealed class DataVoPollingArenaLiveViewsEngine : IArenaLiveViewsEngine
{
    private static readonly ReactiveRowSchema PlayersSchema = new("Id", "Team", "Zone", "Health", "Score");
    private static readonly ReactiveRowSchema InventorySchema = new("InventoryId", "PlayerId", "ItemId", "Slot");
    private static readonly DataVoCompiledQueryPlan PlayerMutationUpdatePlan = DataVoCompiledQueryPlan.Update(
        tableName: "Players",
        assignments: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Score"] = "score",
            ["Health"] = "health",
            ["Zone"] = "zone"
        },
        whereColumn: "Id",
        whereParameterName: "id");
    private static readonly DataVoCompiledQueryPlan InventoryMutationUpdatePlan = DataVoCompiledQueryPlan.Update(
        tableName: "Inventory",
        assignments: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["ItemId"] = "itemId",
            ["Slot"] = "slot"
        },
        whereColumn: "InventoryId",
        whereParameterName: "inventoryId");

    private DataVoContext? _context;
    private DataVoArenaMutationState? _mutationState;

    public string EngineId => "datavo-polling";

    public EngineArchitecture Architecture => EngineArchitecture.EmbeddedPollingRecompute;

    public ValueTask InitializeAsync(ArenaLiveViewsProfile profile, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(profile);

        DisposeDataVo();
        _context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk("CREATE DATABASE ArenaLiveViews");
        ExecuteOk("USE ArenaLiveViews");
        ExecuteOk(ArenaLiveViewsSql.CreatePlayers);
        ExecuteOk(ArenaLiveViewsSql.CreateInventory);
        Seed(EnsureContext(), profile);
        _mutationState = new DataVoArenaMutationState(profile);
        return ValueTask.CompletedTask;
    }

    public ValueTask ApplyBatchAsync(
        IReadOnlyList<ArenaLiveViewsMutation> batch,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(batch);

        foreach (ArenaLiveViewsMutation mutation in batch)
        {
            ApplyMutation(mutation);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<ArenaLiveViewsReadResult> ReadViewsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        long leaderboardStart = Stopwatch.GetTimestamp();
        IReadOnlyList<ArenaLeaderboardRow> leaderboard = ReadLeaderboard();
        long aliveByTeamStart = Stopwatch.GetTimestamp();
        IReadOnlyList<ArenaAliveByTeamRow> aliveByTeam = ReadAliveByTeam();
        long zoneInventoryStart = Stopwatch.GetTimestamp();
        IReadOnlyList<ArenaZoneInventoryRow> zoneInventory = ReadZoneInventory();
        long readEnd = Stopwatch.GetTimestamp();

        return ValueTask.FromResult(new ArenaLiveViewsReadResult(
            new ArenaLiveViewsViews(leaderboard, aliveByTeam, zoneInventory),
            ElapsedMilliseconds(leaderboardStart, aliveByTeamStart),
            ElapsedMilliseconds(aliveByTeamStart, zoneInventoryStart),
            ElapsedMilliseconds(zoneInventoryStart, readEnd)));
    }

    public ValueTask DisposeAsync()
    {
        DisposeDataVo();
        return ValueTask.CompletedTask;
    }

    private void ApplyMutation(ArenaLiveViewsMutation mutation)
    {
        DataVoArenaMutationState mutationState = EnsureMutationState();
        int updatedScore = mutationState.GetUpdatedScore(mutation);
        int playerRowsAffected = DataVoCompiledQuery.Update(
            EnsureContext(),
            PlayerMutationUpdatePlan,
            [
                new DataVoCompiledQueryParameter("score", updatedScore),
                new DataVoCompiledQueryParameter("health", mutation.ReplacementHealth),
                new DataVoCompiledQueryParameter("zone", mutation.ReplacementZone),
                new DataVoCompiledQueryParameter("id", mutation.PlayerId)
            ]);
        RequireSingleAffectedRow(playerRowsAffected, "Players", mutation.PlayerId);

        int inventoryRowsAffected = DataVoCompiledQuery.Update(
            EnsureContext(),
            InventoryMutationUpdatePlan,
            [
                new DataVoCompiledQueryParameter("itemId", mutation.InventoryItemId),
                new DataVoCompiledQueryParameter("slot", mutation.InventorySlot),
                new DataVoCompiledQueryParameter("inventoryId", mutation.PlayerId)
            ]);
        RequireSingleAffectedRow(inventoryRowsAffected, "Inventory", mutation.PlayerId);
        mutationState.SetScore(mutation, updatedScore);
    }

    private IReadOnlyList<ArenaLeaderboardRow> ReadLeaderboard() =>
        ReadRows(ArenaLiveViewsSql.Leaderboard)
            .Select(row => new ArenaLeaderboardRow(ToInt32(row["Id"]), ToInt32(row["Score"])))
            .ToArray();

    private IReadOnlyList<ArenaAliveByTeamRow> ReadAliveByTeam() =>
        ReadRows(ArenaLiveViewsSql.AliveByTeam)
            .Select(row => new ArenaAliveByTeamRow(ToRequiredString(row["Team"]), ToInt32(row["Alive"])))
            .ToArray();

    private IReadOnlyList<ArenaZoneInventoryRow> ReadZoneInventory() =>
        ReadRows(ArenaLiveViewsSql.ZoneInventory)
            .Select(row => new ArenaZoneInventoryRow(ToInt32(row["p.Id"]), ToInt32(row["i.ItemId"])))
            .ToArray();

    private IReadOnlyList<Dictionary<string, object?>> ReadRows(string sql)
    {
        QueryResult result = EnsureContext().Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }

        return result.Data;
    }

    private static void Seed(DataVoContext context, ArenaLiveViewsProfile profile)
    {
        ArenaLiveViewsPlayer[] players = ArenaLiveViewsFixture.CreateInitialPlayers(profile);
        var playerRows = new CellValue[players.Length][];
        for (int index = 0; index < players.Length; index++)
        {
            ArenaLiveViewsPlayer player = players[index];
            playerRows[index] = [
                CellValue.From(player.Id),
                CellValue.From(player.Team),
                CellValue.From(player.Zone),
                CellValue.From(player.Health),
                CellValue.From(player.Score)
            ];
        }

        ArenaLiveViewsInventory[] inventory = ArenaLiveViewsFixture.CreateInitialInventory(profile);
        var inventoryRows = new CellValue[inventory.Length][];
        for (int index = 0; index < inventory.Length; index++)
        {
            ArenaLiveViewsInventory item = inventory[index];
            inventoryRows[index] = [
                CellValue.From(item.InventoryId),
                CellValue.From(item.PlayerId),
                CellValue.From(item.ItemId),
                CellValue.From(item.Slot)
            ];
        }

        context.InsertTypedBatch("Players", PlayersSchema, playerRows, callerOwnsRowBuffers: true);
        context.InsertTypedBatch("Inventory", InventorySchema, inventoryRows, callerOwnsRowBuffers: true);
    }

    private static int ToInt32(object? value) => value switch
    {
        int int32 => int32,
        long int64 => checked((int)int64),
        decimal decimalValue => checked((int)decimalValue),
        double doubleValue => checked((int)doubleValue),
        _ => Convert.ToInt32(value, CultureInfo.InvariantCulture)
    };

    private static string ToRequiredString(object? value) =>
        value as string ?? throw new InvalidOperationException("Expected a non-null DataVo string value.");

    private static double ElapsedMilliseconds(long start, long end) =>
        (end - start) * 1000d / Stopwatch.Frequency;

    private DataVoContext EnsureContext() =>
        _context ?? throw new InvalidOperationException("DataVo polling arena engine has not been initialized.");

    private DataVoArenaMutationState EnsureMutationState() =>
        _mutationState ?? throw new InvalidOperationException("DataVo polling arena mutation state has not been initialized.");

    private static void RequireSingleAffectedRow(int rowsAffected, string tableName, int primaryKey)
    {
        if (rowsAffected != 1)
        {
            throw new InvalidOperationException(
                "Arena compiled update expected exactly one " + tableName + " row for primary key " +
                primaryKey.ToString(CultureInfo.InvariantCulture) + ", but affected " +
                rowsAffected.ToString(CultureInfo.InvariantCulture) + ".");
        }
    }

    private void ExecuteOk(string sql)
    {
        QueryResult result = EnsureContext().Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }

    private void DisposeDataVo()
    {
        _context?.Dispose();
        _context = null;
        _mutationState = null;
    }
}
