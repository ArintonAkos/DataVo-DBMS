using Microsoft.Data.Sqlite;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ComplexVip;

public sealed class SqliteComplexVipExposureEngine : IComplexVipExposureEngine
{
    private SqliteConnection? _connection;
    private SqliteCommand? _insertOrderCommand;
    private long _nextOrderId = 1;

    public string Name => "SQLite";

    public EngineArchitecture Architecture => EngineArchitecture.EmbeddedPollingRecompute;

    public async ValueTask InitializeAsync(ComplexVipExposureScenario scenario, CancellationToken cancellationToken = default)
    {
        await DisposeSqliteAsync();
        _nextOrderId = 1;
        _connection = new SqliteConnection(CreateConnectionString());
        await _connection.OpenAsync(cancellationToken);

        await using (SqliteCommand schema = _connection.CreateCommand())
        {
            schema.CommandText = """
                CREATE TABLE Accounts (Id INTEGER NOT NULL, IsVip INTEGER NOT NULL);
                CREATE TABLE Markets (Id INTEGER NOT NULL, Category TEXT NOT NULL);
                CREATE TABLE Orders (Id INTEGER NOT NULL, AccountId INTEGER NOT NULL, MarketId INTEGER NOT NULL, Stake REAL NOT NULL);
                CREATE INDEX IX_Accounts_Id ON Accounts (Id);
                CREATE INDEX IX_Accounts_IsVip ON Accounts (IsVip);
                CREATE INDEX IX_Markets_Id ON Markets (Id);
                CREATE INDEX IX_Orders_AccountId ON Orders (AccountId);
                CREATE INDEX IX_Orders_MarketId ON Orders (MarketId);
                """;
            await schema.ExecuteNonQueryAsync(cancellationToken);
        }

        await SeedStaticAsync(scenario, cancellationToken);
        _insertOrderCommand = CreateInsertOrderCommand(_connection);
        await IngestBatchAsync(scenario, cancellationToken);
    }

    public async ValueTask IngestOrderAsync(ComplexOrderTick order, CancellationToken cancellationToken = default)
    {
        SqliteCommand command = EnsureInsertOrderCommand();
        BindOrder(command, order);
        await command.ExecuteNonQueryAsync(cancellationToken);
        _nextOrderId = Math.Max(_nextOrderId, order.Id + 1);
    }

    public async ValueTask<IReadOnlyList<CategoryExposure>> QueryExposureAsync(CancellationToken cancellationToken = default)
    {
        SqliteConnection connection = EnsureConnection();
        var rows = new List<CategoryExposure>();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = ComplexVipSql.Query;
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new CategoryExposure(reader.GetString(0), reader.GetDecimal(1)));
        }

        return rows.OrderBy(row => row.Category).ToList();
    }

    public async ValueTask DisposeAsync() => await DisposeSqliteAsync();

    private async Task SeedStaticAsync(ComplexVipExposureScenario scenario, CancellationToken cancellationToken)
    {
        SqliteConnection connection = EnsureConnection();
        await using SqliteTransaction transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);

        await using SqliteCommand account = connection.CreateCommand();
        account.Transaction = transaction;
        account.CommandText = "INSERT INTO Accounts (Id, IsVip) VALUES (@Id, @IsVip)";
        account.Parameters.Add("@Id", SqliteType.Integer);
        account.Parameters.Add("@IsVip", SqliteType.Integer);

        for (int id = 1; id <= scenario.AccountCount; id++)
        {
            account.Parameters["@Id"].Value = id;
            account.Parameters["@IsVip"].Value = ComplexVipTickFactory.IsVipAccount(id, scenario) ? 1 : 0;
            await account.ExecuteNonQueryAsync(cancellationToken);
        }

        await using SqliteCommand market = connection.CreateCommand();
        market.Transaction = transaction;
        market.CommandText = "INSERT INTO Markets (Id, Category) VALUES (@Id, @Category)";
        market.Parameters.Add("@Id", SqliteType.Integer);
        market.Parameters.Add("@Category", SqliteType.Text);

        for (int id = 1; id <= scenario.MarketCount; id++)
        {
            market.Parameters["@Id"].Value = id;
            market.Parameters["@Category"].Value = ComplexVipTickFactory.CategoryForMarket(id);
            await market.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    private async Task IngestBatchAsync(ComplexVipExposureScenario scenario, CancellationToken cancellationToken)
    {
        SqliteConnection connection = EnsureConnection();
        SqliteCommand command = EnsureInsertOrderCommand();
        await using SqliteTransaction transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        command.Transaction = transaction;
        for (int i = 0; i < scenario.InitialOrderCount; i++)
        {
            ComplexOrderTick order = ComplexVipTickFactory.CreateOrder(_nextOrderId + i, scenario);
            BindOrder(command, order);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        command.Transaction = null;
        _nextOrderId += scenario.InitialOrderCount;
    }

    private static SqliteCommand CreateInsertOrderCommand(SqliteConnection connection)
    {
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "INSERT INTO Orders (Id, AccountId, MarketId, Stake) VALUES (@Id, @AccountId, @MarketId, @Stake)";
        command.Parameters.Add("@Id", SqliteType.Integer);
        command.Parameters.Add("@AccountId", SqliteType.Integer);
        command.Parameters.Add("@MarketId", SqliteType.Integer);
        command.Parameters.Add("@Stake", SqliteType.Real);
        return command;
    }

    private static void BindOrder(SqliteCommand command, ComplexOrderTick order)
    {
        command.Parameters["@Id"].Value = order.Id;
        command.Parameters["@AccountId"].Value = order.AccountId;
        command.Parameters["@MarketId"].Value = order.MarketId;
        command.Parameters["@Stake"].Value = order.Stake;
    }

    private static string CreateConnectionString() =>
        $"Data Source=ResearchBenchmarkComplexVip-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";

    private SqliteConnection EnsureConnection() =>
        _connection ?? throw new InvalidOperationException("SQLite complex VIP engine has not been initialized.");

    private SqliteCommand EnsureInsertOrderCommand() =>
        _insertOrderCommand ?? throw new InvalidOperationException("SQLite complex VIP engine has not been initialized.");

    private async ValueTask DisposeSqliteAsync()
    {
        if (_insertOrderCommand is not null)
        {
            await _insertOrderCommand.DisposeAsync();
            _insertOrderCommand = null;
        }

        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}
