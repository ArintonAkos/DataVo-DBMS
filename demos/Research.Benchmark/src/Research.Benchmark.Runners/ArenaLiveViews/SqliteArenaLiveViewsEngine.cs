using Microsoft.Data.Sqlite;
using System.Diagnostics;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ArenaLiveViews;

public sealed class SqliteArenaLiveViewsEngine : IArenaLiveViewsEngine
{
    private SqliteConnection? _connection;
    private SqliteCommand? _updatePlayerCommand;
    private SqliteCommand? _replaceInventoryCommand;

    public string EngineId => "sqlite";

    public EngineArchitecture Architecture => EngineArchitecture.EmbeddedPollingRecompute;

    public async ValueTask InitializeAsync(ArenaLiveViewsProfile profile, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(profile);

        await DisposeSqliteAsync();
        _connection = new SqliteConnection(CreateConnectionString());
        await _connection.OpenAsync(cancellationToken);

        await using (SqliteCommand schema = _connection.CreateCommand())
        {
            schema.CommandText = $"""
                {ArenaLiveViewsSql.CreatePlayers};
                {ArenaLiveViewsSql.CreateInventory};
                CREATE INDEX IX_Players_Zone ON Players (Zone);
                CREATE INDEX IX_Inventory_PlayerId ON Inventory (PlayerId);
                """;
            await schema.ExecuteNonQueryAsync(cancellationToken);
        }

        await SeedAsync(profile, cancellationToken);
        _updatePlayerCommand = await CreateUpdatePlayerCommandAsync(EnsureConnection(), cancellationToken);
        _replaceInventoryCommand = await CreateReplaceInventoryCommandAsync(EnsureConnection(), cancellationToken);
    }

    public async ValueTask ApplyBatchAsync(
        IReadOnlyList<ArenaLiveViewsMutation> batch,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(batch);

        SqliteConnection connection = EnsureConnection();
        SqliteCommand updatePlayer = EnsureUpdatePlayerCommand();
        SqliteCommand replaceInventory = EnsureReplaceInventoryCommand();
        await using SqliteTransaction transaction =
            (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        updatePlayer.Transaction = transaction;
        replaceInventory.Transaction = transaction;

        try
        {
            foreach (ArenaLiveViewsMutation mutation in batch)
            {
                cancellationToken.ThrowIfCancellationRequested();
                BindPlayerUpdate(updatePlayer, mutation);
                await updatePlayer.ExecuteNonQueryAsync(cancellationToken);
                BindInventoryReplacement(replaceInventory, mutation);
                await replaceInventory.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            updatePlayer.Transaction = null;
            replaceInventory.Transaction = null;
        }
    }

    public async ValueTask<ArenaLiveViewsReadResult> ReadViewsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        long leaderboardStart = Stopwatch.GetTimestamp();
        IReadOnlyList<ArenaLeaderboardRow> leaderboard = await ReadLeaderboardAsync(cancellationToken);
        long aliveByTeamStart = Stopwatch.GetTimestamp();
        IReadOnlyList<ArenaAliveByTeamRow> aliveByTeam = await ReadAliveByTeamAsync(cancellationToken);
        long zoneInventoryStart = Stopwatch.GetTimestamp();
        IReadOnlyList<ArenaZoneInventoryRow> zoneInventory = await ReadZoneInventoryAsync(cancellationToken);
        long readEnd = Stopwatch.GetTimestamp();

        return new ArenaLiveViewsReadResult(
            new ArenaLiveViewsViews(leaderboard, aliveByTeam, zoneInventory),
            ElapsedMilliseconds(leaderboardStart, aliveByTeamStart),
            ElapsedMilliseconds(aliveByTeamStart, zoneInventoryStart),
            ElapsedMilliseconds(zoneInventoryStart, readEnd));
    }

    public async ValueTask DisposeAsync() => await DisposeSqliteAsync();

    private async Task SeedAsync(ArenaLiveViewsProfile profile, CancellationToken cancellationToken)
    {
        SqliteConnection connection = EnsureConnection();
        await using SqliteTransaction transaction =
            (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);

        await using SqliteCommand player = connection.CreateCommand();
        player.Transaction = transaction;
        player.CommandText = "INSERT INTO Players (Id, Team, Zone, Health, Score) VALUES (@Id, @Team, @Zone, @Health, @Score)";
        player.Parameters.Add("@Id", SqliteType.Integer);
        player.Parameters.Add("@Team", SqliteType.Text);
        player.Parameters.Add("@Zone", SqliteType.Text);
        player.Parameters.Add("@Health", SqliteType.Integer);
        player.Parameters.Add("@Score", SqliteType.Integer);
        await player.PrepareAsync(cancellationToken);

        foreach (ArenaLiveViewsPlayer row in ArenaLiveViewsFixture.CreateInitialPlayers(profile))
        {
            player.Parameters["@Id"].Value = row.Id;
            player.Parameters["@Team"].Value = row.Team;
            player.Parameters["@Zone"].Value = row.Zone;
            player.Parameters["@Health"].Value = row.Health;
            player.Parameters["@Score"].Value = row.Score;
            await player.ExecuteNonQueryAsync(cancellationToken);
        }

        await using SqliteCommand inventory = connection.CreateCommand();
        inventory.Transaction = transaction;
        inventory.CommandText = "INSERT INTO Inventory (InventoryId, PlayerId, ItemId, Slot) VALUES (@InventoryId, @PlayerId, @ItemId, @Slot)";
        inventory.Parameters.Add("@InventoryId", SqliteType.Integer);
        inventory.Parameters.Add("@PlayerId", SqliteType.Integer);
        inventory.Parameters.Add("@ItemId", SqliteType.Integer);
        inventory.Parameters.Add("@Slot", SqliteType.Integer);
        await inventory.PrepareAsync(cancellationToken);

        foreach (ArenaLiveViewsInventory row in ArenaLiveViewsFixture.CreateInitialInventory(profile))
        {
            inventory.Parameters["@InventoryId"].Value = row.InventoryId;
            inventory.Parameters["@PlayerId"].Value = row.PlayerId;
            inventory.Parameters["@ItemId"].Value = row.ItemId;
            inventory.Parameters["@Slot"].Value = row.Slot;
            await inventory.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    private async Task<IReadOnlyList<ArenaLeaderboardRow>> ReadLeaderboardAsync(CancellationToken cancellationToken)
    {
        SqliteConnection connection = EnsureConnection();
        var rows = new List<ArenaLeaderboardRow>();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = ArenaLiveViewsSql.Leaderboard;
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ArenaLeaderboardRow(reader.GetInt32(0), reader.GetInt32(1)));
        }

        return rows
            .OrderByDescending(row => row.Score)
            .ThenBy(row => row.Id)
            .ToArray();
    }

    private async Task<IReadOnlyList<ArenaAliveByTeamRow>> ReadAliveByTeamAsync(CancellationToken cancellationToken)
    {
        SqliteConnection connection = EnsureConnection();
        var rows = new List<ArenaAliveByTeamRow>();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = ArenaLiveViewsSql.AliveByTeam;
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ArenaAliveByTeamRow(reader.GetString(0), checked((int)reader.GetInt64(1))));
        }

        return rows
            .OrderBy(row => row.Team, StringComparer.Ordinal)
            .ToArray();
    }

    private async Task<IReadOnlyList<ArenaZoneInventoryRow>> ReadZoneInventoryAsync(CancellationToken cancellationToken)
    {
        SqliteConnection connection = EnsureConnection();
        var rows = new List<ArenaZoneInventoryRow>();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = ArenaLiveViewsSql.ZoneInventory;
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ArenaZoneInventoryRow(reader.GetInt32(0), reader.GetInt32(1)));
        }

        return rows
            .OrderBy(row => row.PlayerId)
            .ThenBy(row => row.ItemId)
            .ToArray();
    }

    private static async Task<SqliteCommand> CreateUpdatePlayerCommandAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "UPDATE Players SET Score = Score + @ScoreDelta, Health = @Health, Zone = @Zone WHERE Id = @PlayerId";
        command.Parameters.Add("@ScoreDelta", SqliteType.Integer);
        command.Parameters.Add("@Health", SqliteType.Integer);
        command.Parameters.Add("@Zone", SqliteType.Text);
        command.Parameters.Add("@PlayerId", SqliteType.Integer);
        await command.PrepareAsync(cancellationToken);
        return command;
    }

    private static async Task<SqliteCommand> CreateReplaceInventoryCommandAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "UPDATE Inventory SET ItemId = @ItemId, Slot = @Slot WHERE PlayerId = @PlayerId";
        command.Parameters.Add("@ItemId", SqliteType.Integer);
        command.Parameters.Add("@Slot", SqliteType.Integer);
        command.Parameters.Add("@PlayerId", SqliteType.Integer);
        await command.PrepareAsync(cancellationToken);
        return command;
    }

    private static void BindPlayerUpdate(SqliteCommand command, ArenaLiveViewsMutation mutation)
    {
        command.Parameters["@ScoreDelta"].Value = mutation.ScoreDelta;
        command.Parameters["@Health"].Value = mutation.ReplacementHealth;
        command.Parameters["@Zone"].Value = mutation.ReplacementZone;
        command.Parameters["@PlayerId"].Value = mutation.PlayerId;
    }

    private static void BindInventoryReplacement(SqliteCommand command, ArenaLiveViewsMutation mutation)
    {
        command.Parameters["@ItemId"].Value = mutation.InventoryItemId;
        command.Parameters["@Slot"].Value = mutation.InventorySlot;
        command.Parameters["@PlayerId"].Value = mutation.PlayerId;
    }

    private static string CreateConnectionString() =>
        $"Data Source=ResearchBenchmarkArenaLiveViews-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";

    private static double ElapsedMilliseconds(long start, long end) =>
        (end - start) * 1000d / Stopwatch.Frequency;

    private SqliteConnection EnsureConnection() =>
        _connection ?? throw new InvalidOperationException("SQLite arena live-views engine has not been initialized.");

    private SqliteCommand EnsureUpdatePlayerCommand() =>
        _updatePlayerCommand ?? throw new InvalidOperationException("SQLite arena live-views engine has not been initialized.");

    private SqliteCommand EnsureReplaceInventoryCommand() =>
        _replaceInventoryCommand ?? throw new InvalidOperationException("SQLite arena live-views engine has not been initialized.");

    private async ValueTask DisposeSqliteAsync()
    {
        if (_updatePlayerCommand is not null)
        {
            await _updatePlayerCommand.DisposeAsync();
            _updatePlayerCommand = null;
        }

        if (_replaceInventoryCommand is not null)
        {
            await _replaceInventoryCommand.DisposeAsync();
            _replaceInventoryCommand = null;
        }

        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}
