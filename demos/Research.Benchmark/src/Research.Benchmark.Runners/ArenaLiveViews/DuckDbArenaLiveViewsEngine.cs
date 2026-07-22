using DuckDB.NET.Data;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ArenaLiveViews;

public sealed class DuckDbArenaLiveViewsEngine : IArenaLiveViewsEngine
{
    private DuckDBConnection? _connection;
    private DuckDBCommand? _updatePlayerCommand;
    private DuckDBCommand? _replaceInventoryCommand;

    public string EngineId => "duckdb";

    public EngineArchitecture Architecture => EngineArchitecture.EmbeddedPollingRecompute;

    public ValueTask InitializeAsync(ArenaLiveViewsProfile profile, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(profile);

        DisposeDuckDb();
        _connection = new DuckDBConnection("Data Source=:memory:");
        _connection.Open();

        using (DuckDBCommand schema = _connection.CreateCommand())
        {
            schema.CommandText = $"""
                {ArenaLiveViewsSql.CreatePlayers};
                {ArenaLiveViewsSql.CreateInventory};
                CREATE INDEX IX_Players_Zone ON Players (Zone);
                CREATE INDEX IX_Inventory_PlayerId ON Inventory (PlayerId);
                """;
            schema.ExecuteNonQuery();
        }

        Seed(profile, cancellationToken);
        _updatePlayerCommand = CreateUpdatePlayerCommand(EnsureConnection());
        _replaceInventoryCommand = CreateReplaceInventoryCommand(EnsureConnection());
        return ValueTask.CompletedTask;
    }

    public ValueTask ApplyBatchAsync(
        IReadOnlyList<ArenaLiveViewsMutation> batch,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(batch);

        DuckDBConnection connection = EnsureConnection();
        DuckDBCommand updatePlayer = EnsureUpdatePlayerCommand();
        DuckDBCommand replaceInventory = EnsureReplaceInventoryCommand();
        using DuckDBTransaction transaction = connection.BeginTransaction();
        updatePlayer.Transaction = transaction;
        replaceInventory.Transaction = transaction;

        try
        {
            foreach (ArenaLiveViewsMutation mutation in batch)
            {
                cancellationToken.ThrowIfCancellationRequested();
                BindPlayerUpdate(updatePlayer, mutation);
                updatePlayer.ExecuteNonQuery();
                BindInventoryReplacement(replaceInventory, mutation);
                replaceInventory.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        finally
        {
            updatePlayer.Transaction = null;
            replaceInventory.Transaction = null;
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
        DisposeDuckDb();
        return ValueTask.CompletedTask;
    }

    private void Seed(ArenaLiveViewsProfile profile, CancellationToken cancellationToken)
    {
        DuckDBConnection connection = EnsureConnection();
        using DuckDBTransaction transaction = connection.BeginTransaction();

        using DuckDBCommand player = connection.CreateCommand();
        player.Transaction = transaction;
        player.CommandText = "INSERT INTO Players (Id, Team, Zone, Health, Score) VALUES (?, ?, ?, ?, ?)";
        player.Parameters.Add(new DuckDBParameter { Value = 0 });
        player.Parameters.Add(new DuckDBParameter { Value = string.Empty });
        player.Parameters.Add(new DuckDBParameter { Value = string.Empty });
        player.Parameters.Add(new DuckDBParameter { Value = 0 });
        player.Parameters.Add(new DuckDBParameter { Value = 0 });
        player.Prepare();

        foreach (ArenaLiveViewsPlayer row in ArenaLiveViewsFixture.CreateInitialPlayers(profile))
        {
            cancellationToken.ThrowIfCancellationRequested();
            player.Parameters[0].Value = row.Id;
            player.Parameters[1].Value = row.Team;
            player.Parameters[2].Value = row.Zone;
            player.Parameters[3].Value = row.Health;
            player.Parameters[4].Value = row.Score;
            player.ExecuteNonQuery();
        }

        using DuckDBCommand inventory = connection.CreateCommand();
        inventory.Transaction = transaction;
        inventory.CommandText = "INSERT INTO Inventory (InventoryId, PlayerId, ItemId, Slot) VALUES (?, ?, ?, ?)";
        inventory.Parameters.Add(new DuckDBParameter { Value = 0 });
        inventory.Parameters.Add(new DuckDBParameter { Value = 0 });
        inventory.Parameters.Add(new DuckDBParameter { Value = 0 });
        inventory.Parameters.Add(new DuckDBParameter { Value = 0 });
        inventory.Prepare();

        foreach (ArenaLiveViewsInventory row in ArenaLiveViewsFixture.CreateInitialInventory(profile))
        {
            cancellationToken.ThrowIfCancellationRequested();
            inventory.Parameters[0].Value = row.InventoryId;
            inventory.Parameters[1].Value = row.PlayerId;
            inventory.Parameters[2].Value = row.ItemId;
            inventory.Parameters[3].Value = row.Slot;
            inventory.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    private IReadOnlyList<ArenaLeaderboardRow> ReadLeaderboard()
    {
        DuckDBConnection connection = EnsureConnection();
        var rows = new List<ArenaLeaderboardRow>();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = ArenaLiveViewsSql.Leaderboard;
        using DuckDBDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new ArenaLeaderboardRow(reader.GetInt32(0), reader.GetInt32(1)));
        }

        return rows
            .OrderByDescending(row => row.Score)
            .ThenBy(row => row.Id)
            .ToArray();
    }

    private IReadOnlyList<ArenaAliveByTeamRow> ReadAliveByTeam()
    {
        DuckDBConnection connection = EnsureConnection();
        var rows = new List<ArenaAliveByTeamRow>();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = ArenaLiveViewsSql.AliveByTeam;
        using DuckDBDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new ArenaAliveByTeamRow(reader.GetString(0), checked((int)reader.GetInt64(1))));
        }

        return rows
            .OrderBy(row => row.Team, StringComparer.Ordinal)
            .ToArray();
    }

    private IReadOnlyList<ArenaZoneInventoryRow> ReadZoneInventory()
    {
        DuckDBConnection connection = EnsureConnection();
        var rows = new List<ArenaZoneInventoryRow>();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = ArenaLiveViewsSql.ZoneInventory;
        using DuckDBDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new ArenaZoneInventoryRow(reader.GetInt32(0), reader.GetInt32(1)));
        }

        return rows
            .OrderBy(row => row.PlayerId)
            .ThenBy(row => row.ItemId)
            .ToArray();
    }

    private static DuckDBCommand CreateUpdatePlayerCommand(DuckDBConnection connection)
    {
        DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "UPDATE Players SET Score = Score + ?, Health = ?, Zone = ? WHERE Id = ?";
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = string.Empty });
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Prepare();
        return command;
    }

    private static DuckDBCommand CreateReplaceInventoryCommand(DuckDBConnection connection)
    {
        DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "UPDATE Inventory SET ItemId = ?, Slot = ? WHERE PlayerId = ?";
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Prepare();
        return command;
    }

    private static void BindPlayerUpdate(DuckDBCommand command, ArenaLiveViewsMutation mutation)
    {
        command.Parameters[0].Value = mutation.ScoreDelta;
        command.Parameters[1].Value = mutation.ReplacementHealth;
        command.Parameters[2].Value = mutation.ReplacementZone;
        command.Parameters[3].Value = mutation.PlayerId;
    }

    private static void BindInventoryReplacement(DuckDBCommand command, ArenaLiveViewsMutation mutation)
    {
        command.Parameters[0].Value = mutation.InventoryItemId;
        command.Parameters[1].Value = mutation.InventorySlot;
        command.Parameters[2].Value = mutation.PlayerId;
    }

    private DuckDBConnection EnsureConnection() =>
        _connection ?? throw new InvalidOperationException("DuckDB arena live-views engine has not been initialized.");

    private DuckDBCommand EnsureUpdatePlayerCommand() =>
        _updatePlayerCommand ?? throw new InvalidOperationException("DuckDB arena live-views engine has not been initialized.");

    private DuckDBCommand EnsureReplaceInventoryCommand() =>
        _replaceInventoryCommand ?? throw new InvalidOperationException("DuckDB arena live-views engine has not been initialized.");

    private void DisposeDuckDb()
    {
        _updatePlayerCommand?.Dispose();
        _updatePlayerCommand = null;
        _replaceInventoryCommand?.Dispose();
        _replaceInventoryCommand = null;
        _connection?.Dispose();
        _connection = null;
    }
}
