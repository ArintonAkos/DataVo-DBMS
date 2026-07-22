using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ArenaLiveViews;

public sealed class DataVoPollingArenaLiveViewsEngine : IArenaLiveViewsEngine
{
    private static readonly ReactiveRowSchema PlayersSchema = new("Id", "Team", "Zone", "Health", "Score");
    private static readonly ReactiveRowSchema InventorySchema = new("InventoryId", "PlayerId", "ItemId", "Slot");

    private DataVoContext? _context;

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

    public ValueTask<ArenaLiveViewsViews> ReadViewsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return ValueTask.FromResult(new ArenaLiveViewsViews(
            ReadLeaderboard(),
            ReadAliveByTeam(),
            ReadZoneInventory()));
    }

    public ValueTask DisposeAsync()
    {
        DisposeDataVo();
        return ValueTask.CompletedTask;
    }

    private void ApplyMutation(ArenaLiveViewsMutation mutation)
    {
        int playerId = mutation.PlayerId;
        ExecuteOk(
            "UPDATE Players SET Score = Score + " + mutation.ScoreDelta.ToString(CultureInfo.InvariantCulture) +
            ", Health = " + mutation.ReplacementHealth.ToString(CultureInfo.InvariantCulture) +
            ", Zone = " + SqlStringLiteral(mutation.ReplacementZone) +
            " WHERE Id = " + playerId.ToString(CultureInfo.InvariantCulture));
        ExecuteOk(
            "UPDATE Inventory SET ItemId = " + mutation.InventoryItemId.ToString(CultureInfo.InvariantCulture) +
            ", Slot = " + mutation.InventorySlot.ToString(CultureInfo.InvariantCulture) +
            " WHERE InventoryId = " + playerId.ToString(CultureInfo.InvariantCulture));
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

    private static string SqlStringLiteral(string value) =>
        "'" + (value ?? throw new ArgumentNullException(nameof(value))).Replace("'", "''", StringComparison.Ordinal) + "'";

    private DataVoContext EnsureContext() =>
        _context ?? throw new InvalidOperationException("DataVo polling arena engine has not been initialized.");

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
    }
}
