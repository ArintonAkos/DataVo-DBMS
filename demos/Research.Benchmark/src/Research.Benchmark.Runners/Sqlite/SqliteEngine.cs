using Microsoft.Data.Sqlite;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.Sqlite;

public sealed class SqliteEngine : IBettingRiskEngine
{
    private SqliteConnection? _connection;
    private SqliteCommand? _insertCommand;
    private SqliteCommand? _upsertExposureCommand;
    private SqliteCommand? _pointQueryCommand;
    private BettingRiskScenario _scenario = new(0, 0, 0, 0, 0);
    private long _nextOrderId = 1;

    public string Name => "SQLite";

    public EngineArchitecture Architecture => EngineArchitecture.EmbeddedPollingRecompute;

    public async ValueTask InitializeAsync(BettingRiskScenario scenario, CancellationToken cancellationToken = default)
    {
        await DisposeSqliteAsync();

        _scenario = scenario;
        _nextOrderId = 1;
        _connection = new SqliteConnection(CreateConnectionString());
        await _connection.OpenAsync(cancellationToken);

        await using (SqliteCommand schema = _connection.CreateCommand())
        {
            schema.CommandText = """
                CREATE TABLE Orders (
                    OrderId INTEGER NOT NULL,
                    MarketId INTEGER NOT NULL,
                    RunnerId INTEGER NOT NULL,
                    AccountId INTEGER NOT NULL,
                    Side TEXT NOT NULL,
                    Price INTEGER NOT NULL,
                    Stake INTEGER NOT NULL,
                    Status TEXT NOT NULL
                );
                CREATE INDEX IX_Orders_Status ON Orders (Status);
                CREATE INDEX IX_Orders_MarketId ON Orders (MarketId);
                CREATE INDEX IX_Orders_RunnerId ON Orders (RunnerId);
                CREATE INDEX IX_Orders_AccountId ON Orders (AccountId);
                CREATE INDEX IX_Orders_RunnerRisk ON Orders (Status, MarketId, RunnerId);
                CREATE INDEX IX_Orders_AccountRisk ON Orders (Status, AccountId, MarketId);
                CREATE TABLE RiskExposure (
                    RunnerId INTEGER NOT NULL,
                    AccountId INTEGER NOT NULL,
                    TotalExposure REAL NOT NULL,
                    PRIMARY KEY (RunnerId, AccountId)
                );
                """;
            await schema.ExecuteNonQueryAsync(cancellationToken);
        }

        _insertCommand = CreateInsertCommand(_connection);
        _upsertExposureCommand = CreateUpsertExposureCommand(_connection);
        _pointQueryCommand = CreatePointQueryCommand(_connection);

        List<MarketTick> baseline = Enumerable
            .Range(0, scenario.InitialOrderCount)
            .Select(i => Research.Benchmark.Runners.BenchmarkTickFactory.CreateBaselineOrder(_nextOrderId + i, scenario))
            .ToList();

        await IngestBatchAsync(baseline, cancellationToken);
    }

    public async ValueTask IngestTickAsync(MarketTick tick, CancellationToken cancellationToken = default)
    {
        SqliteConnection connection = EnsureConnection();
        SqliteCommand command = EnsureInsertCommand();
        SqliteCommand upsert = EnsureUpsertExposureCommand();
        await using SqliteTransaction transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        command.Transaction = transaction;
        upsert.Transaction = transaction;

        BindInsert(command, tick);
        await command.ExecuteNonQueryAsync(cancellationToken);
        BindExposure(upsert, tick);
        await upsert.ExecuteNonQueryAsync(cancellationToken);

        await transaction.CommitAsync(cancellationToken);
        command.Transaction = null;
        upsert.Transaction = null;
        _nextOrderId = Math.Max(_nextOrderId, tick.Sequence + 1);
    }

    public async ValueTask IngestBatchAsync(IReadOnlyList<MarketTick> ticks, CancellationToken cancellationToken = default)
    {
        if (ticks.Count == 0)
        {
            return;
        }

        SqliteConnection connection = EnsureConnection();
        SqliteCommand command = EnsureInsertCommand();
        SqliteCommand upsert = EnsureUpsertExposureCommand();
        await using SqliteTransaction transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        command.Transaction = transaction;
        upsert.Transaction = transaction;

        foreach (MarketTick tick in ticks)
        {
            cancellationToken.ThrowIfCancellationRequested();
            BindInsert(command, tick);
            await command.ExecuteNonQueryAsync(cancellationToken);
            BindExposure(upsert, tick);
            await upsert.ExecuteNonQueryAsync(cancellationToken);
            _nextOrderId = Math.Max(_nextOrderId, tick.Sequence + 1);
        }

        await transaction.CommitAsync(cancellationToken);
        command.Transaction = null;
        upsert.Transaction = null;
    }

    public async ValueTask<RiskReadModel> QueryRiskAsync(RiskQuery query, CancellationToken cancellationToken = default)
    {
        SqliteConnection connection = EnsureConnection();
        if (query.RunnerId is not null && query.AccountId is not null)
        {
            SqliteCommand command = EnsurePointQueryCommand();
            command.Parameters["@RunnerId"].Value = query.RunnerId.Value;
            command.Parameters["@AccountId"].Value = query.AccountId.Value;
            object? scalar = await command.ExecuteScalarAsync(cancellationToken);
            decimal totalExposure = scalar is null || scalar is DBNull ? 0m : RiskModelProjection.ToDecimal(scalar);
            return RiskModelProjection.BuildPoint(
                query.RunnerId.Value,
                query.AccountId.Value,
                totalExposure,
                _scenario.SubscriberCount);
        }

        List<RunnerExposure> runners = [];
        List<AccountExposure> accounts = [];

        await using (SqliteCommand command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT MarketId,
                       RunnerId,
                       MAX(Price) AS BestBack,
                       MIN(Price) AS BestLay,
                       SUM(Stake) AS OpenExposure
                FROM Orders
                WHERE Status = 'OPEN'
                GROUP BY MarketId, RunnerId
                """;

            await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                runners.Add(new RunnerExposure(
                    reader.GetInt32(0),
                    reader.GetInt32(1),
                    reader.IsDBNull(2) ? 0m : reader.GetDecimal(2),
                    reader.IsDBNull(3) ? 0m : reader.GetDecimal(3),
                    0m,
                    reader.IsDBNull(4) ? 0m : reader.GetDecimal(4)));
            }
        }

        await using (SqliteCommand command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT AccountId,
                       MarketId,
                       SUM(Stake) AS OpenExposure
                FROM Orders
                WHERE Status = 'OPEN'
                GROUP BY AccountId, MarketId
                """;

            await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                accounts.Add(new AccountExposure(
                    reader.GetInt32(0),
                    reader.GetInt32(1),
                    reader.IsDBNull(2) ? 0m : reader.GetDecimal(2),
                    0m));
            }
        }

        return RiskModelProjection.Build(runners, accounts, query, _scenario.SubscriberCount);
    }

    public async ValueTask ResetAsync(CancellationToken cancellationToken = default)
    {
        SqliteConnection connection = EnsureConnection();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "DELETE FROM Orders; DELETE FROM RiskExposure;";
        await command.ExecuteNonQueryAsync(cancellationToken);
        _nextOrderId = 1;
    }

    public async ValueTask DisposeAsync() => await DisposeSqliteAsync();

    private SqliteConnection EnsureConnection() =>
        _connection ?? throw new InvalidOperationException("SQLite engine has not been initialized.");

    private SqliteCommand EnsureInsertCommand() =>
        _insertCommand ?? throw new InvalidOperationException("SQLite engine has not been initialized.");

    private SqliteCommand EnsureUpsertExposureCommand() =>
        _upsertExposureCommand ?? throw new InvalidOperationException("SQLite engine has not been initialized.");

    private SqliteCommand EnsurePointQueryCommand() =>
        _pointQueryCommand ?? throw new InvalidOperationException("SQLite engine has not been initialized.");

    private static SqliteCommand CreateInsertCommand(SqliteConnection connection)
    {
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO Orders (OrderId, MarketId, RunnerId, AccountId, Side, Price, Stake, Status)
            VALUES (@OrderId, @MarketId, @RunnerId, @AccountId, @Side, @Price, @Stake, @Status)
            """;

        command.Parameters.Add("@OrderId", SqliteType.Integer);
        command.Parameters.Add("@MarketId", SqliteType.Integer);
        command.Parameters.Add("@RunnerId", SqliteType.Integer);
        command.Parameters.Add("@AccountId", SqliteType.Integer);
        command.Parameters.Add("@Side", SqliteType.Text);
        command.Parameters.Add("@Price", SqliteType.Integer);
        command.Parameters.Add("@Stake", SqliteType.Integer);
        command.Parameters.Add("@Status", SqliteType.Text);
        return command;
    }

    private static SqliteCommand CreateUpsertExposureCommand(SqliteConnection connection)
    {
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO RiskExposure (RunnerId, AccountId, TotalExposure)
            VALUES (@RiskRunnerId, @RiskAccountId, @RiskExposure)
            ON CONFLICT(RunnerId, AccountId) DO UPDATE
            SET TotalExposure = RiskExposure.TotalExposure + excluded.TotalExposure
            """;

        command.Parameters.Add("@RiskRunnerId", SqliteType.Integer);
        command.Parameters.Add("@RiskAccountId", SqliteType.Integer);
        command.Parameters.Add("@RiskExposure", SqliteType.Real);
        return command;
    }

    private static SqliteCommand CreatePointQueryCommand(SqliteConnection connection)
    {
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT TotalExposure
            FROM RiskExposure
            WHERE RunnerId = @RunnerId AND AccountId = @AccountId
            """;

        command.Parameters.Add("@RunnerId", SqliteType.Integer);
        command.Parameters.Add("@AccountId", SqliteType.Integer);
        return command;
    }

    private void BindInsert(SqliteCommand command, MarketTick tick)
    {
        long orderId = tick.Sequence > 0 ? tick.Sequence : _nextOrderId;
        command.Parameters["@OrderId"].Value = orderId;
        command.Parameters["@MarketId"].Value = tick.MarketId;
        command.Parameters["@RunnerId"].Value = tick.RunnerId;
        command.Parameters["@AccountId"].Value = tick.AccountId;
        command.Parameters["@Side"].Value = tick.Side;
        command.Parameters["@Price"].Value = Decimal.ToInt64(decimal.Round(tick.Price, 0));
        command.Parameters["@Stake"].Value = Decimal.ToInt64(decimal.Round(tick.Stake, 0));
        command.Parameters["@Status"].Value = tick.Kind == TickKind.OrderCancelled ? "CANCELLED" : "OPEN";
    }

    private static void BindExposure(SqliteCommand command, MarketTick tick)
    {
        command.Parameters["@RiskRunnerId"].Value = tick.RunnerId;
        command.Parameters["@RiskAccountId"].Value = tick.AccountId;
        command.Parameters["@RiskExposure"].Value = tick.Kind == TickKind.OrderCancelled ? 0m : tick.Stake;
    }

    private static string CreateConnectionString() =>
        $"Data Source=ResearchBenchmark-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";

    private async ValueTask DisposeSqliteAsync()
    {
        if (_pointQueryCommand is not null)
        {
            await _pointQueryCommand.DisposeAsync();
            _pointQueryCommand = null;
        }

        if (_upsertExposureCommand is not null)
        {
            await _upsertExposureCommand.DisposeAsync();
            _upsertExposureCommand = null;
        }

        if (_insertCommand is not null)
        {
            await _insertCommand.DisposeAsync();
            _insertCommand = null;
        }

        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}
