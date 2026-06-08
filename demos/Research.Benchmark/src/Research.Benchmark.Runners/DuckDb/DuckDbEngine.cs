using DuckDB.NET.Data;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.DuckDb;

public sealed class DuckDbEngine : IBettingRiskEngine
{
    private const string ConnectionString = "Data Source=:memory:";
    private DuckDBConnection? _connection;
    private DuckDBCommand? _insertCommand;
    private DuckDBCommand? _upsertExposureCommand;
    private DuckDBCommand? _pointQueryCommand;
    private BettingRiskScenario _scenario = new(0, 0, 0, 0, 0);
    private long _nextOrderId = 1;

    public string Name => "DuckDB";

    public EngineArchitecture Architecture => EngineArchitecture.EmbeddedPollingRecompute;

    public async ValueTask InitializeAsync(BettingRiskScenario scenario, CancellationToken cancellationToken = default)
    {
        await DisposeDuckDbAsync();

        _scenario = scenario;
        _nextOrderId = 1;
        _connection = new DuckDBConnection(ConnectionString);
        _connection.Open();

        using (DuckDBCommand schema = _connection.CreateCommand())
        {
            schema.CommandText = """
                CREATE TABLE Orders (
                    OrderId BIGINT NOT NULL,
                    MarketId INTEGER NOT NULL,
                    RunnerId INTEGER NOT NULL,
                    AccountId INTEGER NOT NULL,
                    Side VARCHAR NOT NULL,
                    Price DECIMAL(18, 2) NOT NULL,
                    Stake DECIMAL(18, 2) NOT NULL,
                    Status VARCHAR NOT NULL
                );
                CREATE TABLE RiskExposure (
                    RunnerId INTEGER NOT NULL,
                    AccountId INTEGER NOT NULL,
                    TotalExposure DOUBLE NOT NULL,
                    PRIMARY KEY (RunnerId, AccountId)
                );
                """;
            schema.ExecuteNonQuery();
        }

        _insertCommand = CreateInsertCommand(_connection);
        _upsertExposureCommand = CreateUpsertExposureCommand(_connection);
        _pointQueryCommand = CreatePointQueryCommand(_connection);

        List<MarketTick> baseline = Enumerable
            .Range(0, scenario.InitialOrderCount)
            .Select(i => BenchmarkTickFactory.CreateBaselineOrder(_nextOrderId + i, scenario))
            .ToList();

        await IngestBatchAsync(baseline, cancellationToken);
    }

    public ValueTask IngestTickAsync(MarketTick tick, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        DuckDBConnection connection = EnsureConnection();
        DuckDBCommand command = EnsureInsertCommand();
        DuckDBCommand upsert = EnsureUpsertExposureCommand();
        using DuckDBTransaction transaction = connection.BeginTransaction();
        command.Transaction = transaction;
        upsert.Transaction = transaction;

        BindInsert(command, tick);
        command.ExecuteNonQuery();
        BindExposure(upsert, tick);
        upsert.ExecuteNonQuery();

        transaction.Commit();
        command.Transaction = null;
        upsert.Transaction = null;
        _nextOrderId = Math.Max(_nextOrderId, tick.Sequence + 1);
        return ValueTask.CompletedTask;
    }

    public ValueTask IngestBatchAsync(IReadOnlyList<MarketTick> ticks, CancellationToken cancellationToken = default)
    {
        if (ticks.Count == 0)
        {
            return ValueTask.CompletedTask;
        }

        DuckDBConnection connection = EnsureConnection();
        DuckDBCommand command = EnsureInsertCommand();
        DuckDBCommand upsert = EnsureUpsertExposureCommand();
        using DuckDBTransaction transaction = connection.BeginTransaction();
        command.Transaction = transaction;
        upsert.Transaction = transaction;

        foreach (MarketTick tick in ticks)
        {
            cancellationToken.ThrowIfCancellationRequested();
            BindInsert(command, tick);
            command.ExecuteNonQuery();
            BindExposure(upsert, tick);
            upsert.ExecuteNonQuery();
            _nextOrderId = Math.Max(_nextOrderId, tick.Sequence + 1);
        }

        transaction.Commit();
        command.Transaction = null;
        upsert.Transaction = null;
        return ValueTask.CompletedTask;
    }

    public ValueTask<RiskReadModel> QueryRiskAsync(RiskQuery query, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        DuckDBConnection connection = EnsureConnection();
        if (query.RunnerId is not null && query.AccountId is not null)
        {
            DuckDBCommand command = EnsurePointQueryCommand();
            command.Parameters[0].Value = query.RunnerId.Value;
            command.Parameters[1].Value = query.AccountId.Value;
            object? scalar = command.ExecuteScalar();
            decimal totalExposure = scalar is null || scalar is DBNull ? 0m : RiskModelProjection.ToDecimal(scalar);
            return ValueTask.FromResult(RiskModelProjection.BuildPoint(
                query.RunnerId.Value,
                query.AccountId.Value,
                totalExposure,
                _scenario.SubscriberCount));
        }

        List<RunnerExposure> runners = [];
        List<AccountExposure> accounts = [];

        using (DuckDBCommand command = connection.CreateCommand())
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

            using DuckDBDataReader reader = command.ExecuteReader();
            while (reader.Read())
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

        using (DuckDBCommand command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT AccountId,
                       MarketId,
                       SUM(Stake) AS OpenExposure
                FROM Orders
                WHERE Status = 'OPEN'
                GROUP BY AccountId, MarketId
                """;

            using DuckDBDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                accounts.Add(new AccountExposure(
                    reader.GetInt32(0),
                    reader.GetInt32(1),
                    reader.IsDBNull(2) ? 0m : reader.GetDecimal(2),
                    0m));
            }
        }

        return ValueTask.FromResult(RiskModelProjection.Build(runners, accounts, query, _scenario.SubscriberCount));
    }

    public ValueTask ResetAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        DuckDBConnection connection = EnsureConnection();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "DELETE FROM Orders; DELETE FROM RiskExposure;";
        command.ExecuteNonQuery();
        _nextOrderId = 1;
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await DisposeDuckDbAsync();

    private DuckDBConnection EnsureConnection() =>
        _connection ?? throw new InvalidOperationException("DuckDB engine has not been initialized.");

    private DuckDBCommand EnsureInsertCommand() =>
        _insertCommand ?? throw new InvalidOperationException("DuckDB engine has not been initialized.");

    private DuckDBCommand EnsureUpsertExposureCommand() =>
        _upsertExposureCommand ?? throw new InvalidOperationException("DuckDB engine has not been initialized.");

    private DuckDBCommand EnsurePointQueryCommand() =>
        _pointQueryCommand ?? throw new InvalidOperationException("DuckDB engine has not been initialized.");

    private static DuckDBCommand CreateInsertCommand(DuckDBConnection connection)
    {
        DuckDBCommand command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO Orders (OrderId, MarketId, RunnerId, AccountId, Side, Price, Stake, Status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

        command.Parameters.Add(new DuckDBParameter { Value = 0L });
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = string.Empty });
        command.Parameters.Add(new DuckDBParameter { Value = 0m });
        command.Parameters.Add(new DuckDBParameter { Value = 0m });
        command.Parameters.Add(new DuckDBParameter { Value = string.Empty });
        return command;
    }

    private static DuckDBCommand CreateUpsertExposureCommand(DuckDBConnection connection)
    {
        DuckDBCommand command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO RiskExposure (RunnerId, AccountId, TotalExposure)
            VALUES (?, ?, ?)
            ON CONFLICT (RunnerId, AccountId) DO UPDATE
            SET TotalExposure = RiskExposure.TotalExposure + excluded.TotalExposure
            """;

        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = 0d });
        return command;
    }

    private static DuckDBCommand CreatePointQueryCommand(DuckDBConnection connection)
    {
        DuckDBCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT TotalExposure
            FROM RiskExposure
            WHERE RunnerId = ? AND AccountId = ?
            """;

        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        return command;
    }

    private void BindInsert(DuckDBCommand command, MarketTick tick)
    {
        long orderId = tick.Sequence > 0 ? tick.Sequence : _nextOrderId;
        command.Parameters[0].Value = orderId;
        command.Parameters[1].Value = tick.MarketId;
        command.Parameters[2].Value = tick.RunnerId;
        command.Parameters[3].Value = tick.AccountId;
        command.Parameters[4].Value = tick.Side;
        command.Parameters[5].Value = tick.Price;
        command.Parameters[6].Value = tick.Stake;
        command.Parameters[7].Value = tick.Kind == TickKind.OrderCancelled ? "CANCELLED" : "OPEN";
    }

    private static void BindExposure(DuckDBCommand command, MarketTick tick)
    {
        command.Parameters[0].Value = tick.RunnerId;
        command.Parameters[1].Value = tick.AccountId;
        command.Parameters[2].Value = tick.Kind == TickKind.OrderCancelled ? 0d : Convert.ToDouble(tick.Stake);
    }

    private ValueTask DisposeDuckDbAsync()
    {
        _pointQueryCommand?.Dispose();
        _pointQueryCommand = null;
        _upsertExposureCommand?.Dispose();
        _upsertExposureCommand = null;
        _insertCommand?.Dispose();
        _insertCommand = null;
        _connection?.Dispose();
        _connection = null;
        return ValueTask.CompletedTask;
    }
}
