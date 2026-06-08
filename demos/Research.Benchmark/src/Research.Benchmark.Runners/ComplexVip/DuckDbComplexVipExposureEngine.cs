using DuckDB.NET.Data;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ComplexVip;

public sealed class DuckDbComplexVipExposureEngine : IComplexVipExposureEngine
{
    private DuckDBConnection? _connection;
    private DuckDBCommand? _insertOrderCommand;
    private long _nextOrderId = 1;

    public string Name => "DuckDB";

    public EngineArchitecture Architecture => EngineArchitecture.EmbeddedPollingRecompute;

    public ValueTask InitializeAsync(ComplexVipExposureScenario scenario, CancellationToken cancellationToken = default)
    {
        DisposeDuckDb();
        _nextOrderId = 1;
        _connection = new DuckDBConnection("Data Source=:memory:");
        _connection.Open();

        using (DuckDBCommand schema = _connection.CreateCommand())
        {
            schema.CommandText = """
                CREATE TABLE Accounts (Id INTEGER NOT NULL, IsVip BOOLEAN NOT NULL);
                CREATE TABLE Markets (Id INTEGER NOT NULL, Category VARCHAR NOT NULL);
                CREATE TABLE Orders (Id BIGINT NOT NULL, AccountId INTEGER NOT NULL, MarketId INTEGER NOT NULL, Stake DOUBLE NOT NULL);
                CREATE INDEX IX_Accounts_Id ON Accounts (Id);
                CREATE INDEX IX_Accounts_IsVip ON Accounts (IsVip);
                CREATE INDEX IX_Markets_Id ON Markets (Id);
                CREATE INDEX IX_Orders_AccountId ON Orders (AccountId);
                CREATE INDEX IX_Orders_MarketId ON Orders (MarketId);
                """;
            schema.ExecuteNonQuery();
        }

        SeedStatic(scenario);
        _insertOrderCommand = CreateInsertOrderCommand(_connection);
        IngestBatch(scenario);
        return ValueTask.CompletedTask;
    }

    public ValueTask IngestOrderAsync(ComplexOrderTick order, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        DuckDBCommand command = EnsureInsertOrderCommand();
        BindOrder(command, order);
        command.ExecuteNonQuery();
        _nextOrderId = Math.Max(_nextOrderId, order.Id + 1);
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<CategoryExposure>> QueryExposureAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        DuckDBConnection connection = EnsureConnection();
        var rows = new List<CategoryExposure>();
        using DuckDBCommand command = connection.CreateCommand();
        command.CommandText = ComplexVipSql.Query;
        using DuckDBDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new CategoryExposure(reader.GetString(0), Convert.ToDecimal(reader.GetDouble(1))));
        }

        return ValueTask.FromResult((IReadOnlyList<CategoryExposure>)rows.OrderBy(row => row.Category).ToList());
    }

    public ValueTask DisposeAsync()
    {
        DisposeDuckDb();
        return ValueTask.CompletedTask;
    }

    private void SeedStatic(ComplexVipExposureScenario scenario)
    {
        DuckDBConnection connection = EnsureConnection();
        using DuckDBTransaction transaction = connection.BeginTransaction();

        using DuckDBCommand account = connection.CreateCommand();
        account.Transaction = transaction;
        account.CommandText = "INSERT INTO Accounts (Id, IsVip) VALUES (?, ?)";
        account.Parameters.Add(new DuckDBParameter { Value = 0 });
        account.Parameters.Add(new DuckDBParameter { Value = false });
        for (int id = 1; id <= scenario.AccountCount; id++)
        {
            account.Parameters[0].Value = id;
            account.Parameters[1].Value = ComplexVipTickFactory.IsVipAccount(id, scenario);
            account.ExecuteNonQuery();
        }

        using DuckDBCommand market = connection.CreateCommand();
        market.Transaction = transaction;
        market.CommandText = "INSERT INTO Markets (Id, Category) VALUES (?, ?)";
        market.Parameters.Add(new DuckDBParameter { Value = 0 });
        market.Parameters.Add(new DuckDBParameter { Value = string.Empty });
        for (int id = 1; id <= scenario.MarketCount; id++)
        {
            market.Parameters[0].Value = id;
            market.Parameters[1].Value = ComplexVipTickFactory.CategoryForMarket(id);
            market.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    private void IngestBatch(ComplexVipExposureScenario scenario)
    {
        DuckDBConnection connection = EnsureConnection();
        DuckDBCommand command = EnsureInsertOrderCommand();
        using DuckDBTransaction transaction = connection.BeginTransaction();
        command.Transaction = transaction;
        for (int i = 0; i < scenario.InitialOrderCount; i++)
        {
            ComplexOrderTick order = ComplexVipTickFactory.CreateOrder(_nextOrderId + i, scenario);
            BindOrder(command, order);
            command.ExecuteNonQuery();
        }

        transaction.Commit();
        command.Transaction = null;
        _nextOrderId += scenario.InitialOrderCount;
    }

    private static DuckDBCommand CreateInsertOrderCommand(DuckDBConnection connection)
    {
        DuckDBCommand command = connection.CreateCommand();
        command.CommandText = "INSERT INTO Orders (Id, AccountId, MarketId, Stake) VALUES (?, ?, ?, ?)";
        command.Parameters.Add(new DuckDBParameter { Value = 0L });
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = 0 });
        command.Parameters.Add(new DuckDBParameter { Value = 0d });
        return command;
    }

    private static void BindOrder(DuckDBCommand command, ComplexOrderTick order)
    {
        command.Parameters[0].Value = order.Id;
        command.Parameters[1].Value = order.AccountId;
        command.Parameters[2].Value = order.MarketId;
        command.Parameters[3].Value = Convert.ToDouble(order.Stake);
    }

    private DuckDBConnection EnsureConnection() =>
        _connection ?? throw new InvalidOperationException("DuckDB complex VIP engine has not been initialized.");

    private DuckDBCommand EnsureInsertOrderCommand() =>
        _insertOrderCommand ?? throw new InvalidOperationException("DuckDB complex VIP engine has not been initialized.");

    private void DisposeDuckDb()
    {
        _insertOrderCommand?.Dispose();
        _insertOrderCommand = null;
        _connection?.Dispose();
        _connection = null;
    }
}
