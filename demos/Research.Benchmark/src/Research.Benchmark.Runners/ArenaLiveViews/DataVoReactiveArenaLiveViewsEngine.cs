using System.Globalization;
using System.Diagnostics;
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ArenaLiveViews;

public sealed class DataVoReactiveArenaLiveViewsEngine : IArenaLiveViewsEngine
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
    // AggregateReactiveQuery intentionally does not implement ordering. The maintained projection
    // below applies the canonical ordinal ordering when it materializes the read model.
    private const string ReactiveAliveByTeamSql =
        "SELECT Team, COUNT(*) AS Alive FROM Players WHERE Health > 0 GROUP BY Team";
    // JoinReactiveQuery likewise maintains the join but deliberately leaves ordering to its consumer.
    private const string ReactiveZoneInventorySql =
        "SELECT p.Id, i.ItemId FROM Players p JOIN Inventory i ON p.Id = i.PlayerId WHERE p.Zone = 'arena-7'";

    private readonly object _gate = new();
    private readonly SortedDictionary<int, ArenaLeaderboardRow> _leaderboard = [];
    private readonly SortedDictionary<string, ArenaAliveByTeamRow> _aliveByTeam = new(StringComparer.Ordinal);
    private readonly SortedDictionary<(int PlayerId, int ItemId), ArenaZoneInventoryRow> _zoneInventory = [];
    private IReadOnlyList<ArenaLeaderboardRow> _leaderboardView = [];
    private IReadOnlyList<ArenaAliveByTeamRow> _aliveByTeamView = [];
    private IReadOnlyList<ArenaZoneInventoryRow> _zoneInventoryView = [];
    private DataVoContext? _context;
    private DataVoArenaMutationState? _mutationState;
    private IDisposable? _leaderboardSubscription;
    private IDisposable? _aliveByTeamSubscription;
    private IDisposable? _zoneInventorySubscription;

    public string EngineId => "datavo-reactive";

    public EngineArchitecture Architecture => EngineArchitecture.EmbeddedReactiveIvm;

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

        DataVoContext context = EnsureContext();
        _leaderboardSubscription = context.SubscribeZeroAlloc(ArenaLiveViewsSql.Leaderboard, ApplyLeaderboardChange);
        _aliveByTeamSubscription = context.SubscribeZeroAlloc(ReactiveAliveByTeamSql, ApplyAliveByTeamChange);
        _zoneInventorySubscription = context.SubscribeZeroAlloc(ReactiveZoneInventorySql, ApplyZoneInventoryChange);

        Seed(context, profile);
        _mutationState = new DataVoArenaMutationState(profile);
        context.DispatchPendingNotifications();
        return ValueTask.CompletedTask;
    }

    public ValueTask ApplyBatchAsync(
        IReadOnlyList<ArenaLiveViewsMutation> batch,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(batch);

        DataVoContext context = EnsureContext();
        foreach (ArenaLiveViewsMutation mutation in batch)
        {
            ApplyMutation(mutation);
        }

        context.DispatchPendingNotifications();
        return ValueTask.CompletedTask;
    }

    public ValueTask<ArenaLiveViewsReadResult> ReadViewsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            long leaderboardStart = Stopwatch.GetTimestamp();
            IReadOnlyList<ArenaLeaderboardRow> leaderboard = _leaderboardView
                .OrderByDescending(row => row.Score)
                .ThenBy(row => row.Id)
                .ToArray();
            long aliveByTeamStart = Stopwatch.GetTimestamp();
            IReadOnlyList<ArenaAliveByTeamRow> aliveByTeam = _aliveByTeamView
                .OrderBy(row => row.Team, StringComparer.Ordinal)
                .ToArray();
            long zoneInventoryStart = Stopwatch.GetTimestamp();
            IReadOnlyList<ArenaZoneInventoryRow> zoneInventory = _zoneInventoryView
                .OrderBy(row => row.PlayerId)
                .ThenBy(row => row.ItemId)
                .ToArray();
            long readEnd = Stopwatch.GetTimestamp();

            return ValueTask.FromResult(new ArenaLiveViewsReadResult(
                new ArenaLiveViewsViews(leaderboard, aliveByTeam, zoneInventory),
                ElapsedMilliseconds(leaderboardStart, aliveByTeamStart),
                ElapsedMilliseconds(aliveByTeamStart, zoneInventoryStart),
                ElapsedMilliseconds(zoneInventoryStart, readEnd)));
        }
    }

    public ValueTask DisposeAsync()
    {
        DisposeDataVo();
        return ValueTask.CompletedTask;
    }

    private void ApplyLeaderboardChange(in QueryChangeRef change)
    {
        lock (_gate)
        {
            RemoveLeaderboardRows(change.Removed);
            UpsertLeaderboardRows(change.Added);
            UpsertLeaderboardRows(change.Updated);
            _leaderboardView = _leaderboard.Values
                .OrderByDescending(row => row.Score)
                .ThenBy(row => row.Id)
                .ToArray();
        }
    }

    private void ApplyAliveByTeamChange(in QueryChangeRef change)
    {
        lock (_gate)
        {
            RemoveAliveByTeamRows(change.Removed);
            UpsertAliveByTeamRows(change.Added);
            UpsertAliveByTeamRows(change.Updated);
            _aliveByTeamView = _aliveByTeam.Values.ToArray();
        }
    }

    private void ApplyZoneInventoryChange(in QueryChangeRef change)
    {
        lock (_gate)
        {
            RemoveZoneInventoryRows(change.Removed);
            RemoveZoneInventoryRows(change.UpdatedBefore);
            UpsertZoneInventoryRows(change.Added);
            UpsertZoneInventoryRows(change.Updated);
            _zoneInventoryView = _zoneInventory.Values.ToArray();
        }
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

    private void RemoveLeaderboardRows(RowSet rows)
    {
        for (int index = 0; index < rows.Count; index++)
        {
            _leaderboard.Remove(ToInt32(rows[index]["Id"]));
        }
    }

    private void UpsertLeaderboardRows(RowSet rows)
    {
        for (int index = 0; index < rows.Count; index++)
        {
            RowRef row = rows[index];
            int id = ToInt32(row["Id"]);
            _leaderboard[id] = new ArenaLeaderboardRow(id, ToInt32(row["Score"]));
        }
    }

    private void RemoveAliveByTeamRows(RowSet rows)
    {
        for (int index = 0; index < rows.Count; index++)
        {
            _aliveByTeam.Remove(ToRequiredString(rows[index]["Team"]));
        }
    }

    private void UpsertAliveByTeamRows(RowSet rows)
    {
        for (int index = 0; index < rows.Count; index++)
        {
            RowRef row = rows[index];
            string team = ToRequiredString(row["Team"]);
            _aliveByTeam[team] = new ArenaAliveByTeamRow(team, ToInt32(row["Alive"]));
        }
    }

    private void RemoveZoneInventoryRows(RowSet rows)
    {
        for (int index = 0; index < rows.Count; index++)
        {
            RowRef row = rows[index];
            _zoneInventory.Remove((ToInt32(row["p.Id"]), ToInt32(row["i.ItemId"])));
        }
    }

    private void UpsertZoneInventoryRows(RowSet rows)
    {
        for (int index = 0; index < rows.Count; index++)
        {
            RowRef row = rows[index];
            int playerId = ToInt32(row["p.Id"]);
            int itemId = ToInt32(row["i.ItemId"]);
            _zoneInventory[(playerId, itemId)] = new ArenaZoneInventoryRow(playerId, itemId);
        }
    }

    private static int ToInt32(CellValue value) => value.Type switch
    {
        CellType.Int32 => value.AsInt32(),
        CellType.Int64 => checked((int)value.AsInt64()),
        CellType.Decimal => checked((int)value.AsDecimal()),
        CellType.Double => checked((int)value.AsDouble()),
        _ => throw new InvalidOperationException($"Expected numeric DataVo cell but received {value.Type}.")
    };

    private static string ToRequiredString(CellValue value) =>
        value.AsString() ?? throw new InvalidOperationException("Expected a non-null DataVo string cell.");

    private static double ElapsedMilliseconds(long start, long end) =>
        (end - start) * 1000d / Stopwatch.Frequency;

    private DataVoContext EnsureContext() =>
        _context ?? throw new InvalidOperationException("DataVo reactive arena engine has not been initialized.");

    private DataVoArenaMutationState EnsureMutationState() =>
        _mutationState ?? throw new InvalidOperationException("DataVo reactive arena mutation state has not been initialized.");

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
        _leaderboardSubscription?.Dispose();
        _leaderboardSubscription = null;
        _aliveByTeamSubscription?.Dispose();
        _aliveByTeamSubscription = null;
        _zoneInventorySubscription?.Dispose();
        _zoneInventorySubscription = null;
        _context?.Dispose();
        _context = null;
        _mutationState = null;

        lock (_gate)
        {
            _leaderboard.Clear();
            _aliveByTeam.Clear();
            _zoneInventory.Clear();
            _leaderboardView = [];
            _aliveByTeamView = [];
            _zoneInventoryView = [];
        }
    }
}
