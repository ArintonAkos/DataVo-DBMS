using System.Data;
using System.Data.Common;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;
using DataVo.Data;

namespace DataVo.Tests.ADO;

/// <summary>
/// Regression tests for the ADO.NET provider contract fixes (audit items BC-19, LE-17 to LE-22): connection-string
/// handling, provider factory, exception base type, transactions, command behaviors and data-reader edge cases.
/// </summary>
public sealed class AdoNetProviderConformanceTests : IDisposable
{
    private readonly DataVoConnection _connection;

    public AdoNetProviderConformanceTests()
    {
        _connection = new DataVoConnection($"StorageMode=InMemory;DataSource=Conformance_{Guid.NewGuid():N}");
        _connection.Open();
        Execute("CREATE TABLE People (Id INT PRIMARY KEY, Name VARCHAR(50), Nick VARCHAR(50));");
        Execute("INSERT INTO People (Id, Name, Nick) VALUES (1, 'Alice', NULL);");
        Execute("INSERT INTO People (Id, Name, Nick) VALUES (2, 'Bob', 'bobby');");
        Execute("INSERT INTO People (Id, Name, Nick) VALUES (3, 'Carol', 'cc');");
    }

    public void Dispose() => _connection.Dispose();

    // ---------------------------------------------------------------- BC-19

    [Fact]
    public void ServerVersion_ComesFromAssemblyVersion()
    {
        Assert.NotEqual("DataVo 1.0", _connection.ServerVersion);
        Assert.Matches(@"^\d+\.\d+\.\d+", _connection.ServerVersion);
        Assert.DoesNotContain("+", _connection.ServerVersion, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------- LE-17

    [Fact]
    public void ParameterlessConstructor_ThenConnectionString_Opens()
    {
        using var connection = new DataVoConnection();
        Assert.Equal(string.Empty, connection.ConnectionString);

        connection.ConnectionString = $"StorageMode=InMemory;DataSource=Late_{Guid.NewGuid():N}";
        connection.Open();

        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.StartsWith("Late_", connection.Database, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectionStringSetter_ReparsesBuilder()
    {
        using var connection = new DataVoConnection("StorageMode=InMemory;DataSource=first");
        connection.ConnectionString = "StorageMode=InMemory;DataSource=second";

        Assert.Equal("second", connection.DataSource);
        Assert.Equal("second", connection.Database);
    }

    [Fact]
    public void ConnectionStringSetter_RejectsChangeWhileOpen()
    {
        Assert.Throws<InvalidOperationException>(() => _connection.ConnectionString = "DataSource=other");
    }

    [Fact]
    public void ChangeDatabase_UpdatesDatabaseProperty()
    {
        Execute("CREATE DATABASE IF NOT EXISTS Secondary;");

        _connection.ChangeDatabase("Secondary");

        Assert.Equal("Secondary", _connection.Database);
        Execute("CREATE TABLE OnlyInSecondary (Id INT PRIMARY KEY);");
        Assert.Equal(1, ExecuteNonQuery("INSERT INTO OnlyInSecondary (Id) VALUES (1);"));
    }

    [Fact]
    public void PathDataSource_OpensWithDerivedDatabaseName()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo-ado-{Guid.NewGuid():N}");
        string dataSource = Path.Combine(root, "my-db.v1") + Path.DirectorySeparatorChar;
        try
        {
            using (var connection = new DataVoConnection($"StorageMode=Lsm;DataSource={dataSource}"))
            {
                connection.Open();
                Assert.Equal("my_db_v1", connection.Database);
                using DbCommand create = connection.CreateCommand();
                create.CommandText = "CREATE TABLE T (Id INT PRIMARY KEY); INSERT INTO T (Id) VALUES (7);";
                create.ExecuteNonQuery();
            }

            using (var reopened = new DataVoConnection($"StorageMode=Lsm;DataSource={dataSource}"))
            {
                reopened.Open();
                using DbCommand read = reopened.CreateCommand();
                read.CommandText = "SELECT Id FROM T;";
                Assert.Equal(7, Convert.ToInt32(read.ExecuteScalar()));
            }
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public void ExplicitDatabaseKey_OverridesDerivedName()
    {
        using var connection = new DataVoConnection($"StorageMode=InMemory;DataSource=./data/{Guid.NewGuid():N};Database=AppDb");
        connection.Open();
        Assert.Equal("AppDb", connection.Database);
    }

    [Fact]
    public void ExplicitDatabaseKey_RejectsNonIdentifier()
    {
        var builder = new DataVoConnectionStringBuilder("DataSource=x;Database=bad name");
        Assert.Throws<ArgumentException>(() => builder.Database);
    }

    // ---------------------------------------------------------------- LE-18

    [Fact]
    public void ConnectionStringBuilder_IsDbConnectionStringBuilder_WithWritableProperties()
    {
        var builder = new DataVoConnectionStringBuilder();
        DbConnectionStringBuilder asBase = builder;

        builder.StorageMode = StorageMode.Lsm;
        builder.DataSource = "./store";

        Assert.Equal(StorageMode.Lsm, builder.StorageMode);
        Assert.Equal("./store", builder.DataSource);
        Assert.Equal("store", builder.Database);
        Assert.Equal("./store", asBase["DataSource"]);

        var roundTrip = new DataVoConnectionStringBuilder(builder.ConnectionString);
        Assert.Equal(StorageMode.Lsm, roundTrip.StorageMode);
        Assert.Equal("./store", roundTrip.DataSource);
    }

    [Fact]
    public void ConnectionStringBuilder_KeysAreCaseInsensitive()
    {
        var builder = new DataVoConnectionStringBuilder("storagemode=lsm;datasource=abc");
        Assert.Equal(StorageMode.Lsm, builder.StorageMode);
        Assert.Equal("abc", builder.DataSource);
    }

    [Fact]
    public void ConnectionStringBuilder_RejectsUnsupportedStorageModeOnSet()
    {
        var builder = new DataVoConnectionStringBuilder();
        Assert.Throws<ArgumentException>(() => builder.StorageMode = StorageMode.Wasm);
    }

    [Fact]
    public void ProviderFactory_CreatesProviderObjects()
    {
        DbProviderFactory factory = DbProviderFactories.GetFactory(_connection)!;

        Assert.Same(DataVoFactory.Instance, factory);
        Assert.IsType<DataVoConnection>(factory.CreateConnection());
        Assert.IsType<DataVoCommand>(factory.CreateCommand());
        Assert.IsType<DataVoParameter>(factory.CreateParameter());
        Assert.IsType<DataVoConnectionStringBuilder>(factory.CreateConnectionStringBuilder());

        using DbConnection connection = factory.CreateConnection()!;
        connection.ConnectionString = $"DataSource=Factory_{Guid.NewGuid():N}";
        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    // ---------------------------------------------------------------- LE-19

    [Fact]
    public void DataVoException_IsDbException_WithErrorMetadata()
    {
        DbException ex = Assert.ThrowsAny<DbException>(() => Execute("INSERT INTO People (Id, Name) VALUES (1, 'Dup');"));

        DataVoException dataVo = Assert.IsType<DataVoException>(ex);
        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, dataVo.ErrorCode);
    }

    [Fact]
    public void ConnectionInternalErrors_CarryErrorCode()
    {
        DataVoException ex = Assert.Throws<DataVoException>(() => _connection.ChangeDatabase("DoesNotExist"));
        Assert.False(string.IsNullOrEmpty(ex.ErrorCode));
    }

    // ---------------------------------------------------------------- LE-20

    [Fact]
    public void Transaction_SupportsSavepoints_AndReportsSnapshot()
    {
        using DbTransaction tx = _connection.BeginTransaction();
        Assert.True(tx.SupportsSavepoints);
        Assert.Equal(IsolationLevel.Snapshot, tx.IsolationLevel);
        tx.Rollback();
    }

    [Theory]
    [InlineData(IsolationLevel.Unspecified)]
    [InlineData(IsolationLevel.ReadCommitted)]
    [InlineData(IsolationLevel.RepeatableRead)]
    [InlineData(IsolationLevel.Snapshot)]
    public void BeginTransaction_AcceptsLevelsSnapshotSatisfies(IsolationLevel level)
    {
        using DbTransaction tx = _connection.BeginTransaction(level);
        Assert.Equal(IsolationLevel.Snapshot, tx.IsolationLevel);
    }

    [Theory]
    [InlineData(IsolationLevel.Serializable)]
    [InlineData(IsolationLevel.Chaos)]
    public void BeginTransaction_RejectsLevelsSnapshotCannotHonour(IsolationLevel level)
    {
        Assert.Throws<NotSupportedException>(() => _connection.BeginTransaction(level));

        // The rejected request must not leave a transaction open.
        using DbTransaction tx = _connection.BeginTransaction();
        tx.Rollback();
    }

    [Fact]
    public void DisposingUncommittedTransaction_RollsBack()
    {
        try
        {
            using DbTransaction tx = _connection.BeginTransaction();
            Execute("INSERT INTO People (Id, Name) VALUES (10, 'Ghost');");
            throw new InvalidOperationException("boom");
        }
        catch (InvalidOperationException)
        {
        }

        Assert.Equal(0L, Convert.ToInt64(Scalar("SELECT COUNT(*) FROM People WHERE Id = 10;")));

        // A fresh transaction can start: the disposed one did not leak.
        using DbTransaction next = _connection.BeginTransaction();
        next.Commit();
    }

    [Fact]
    public void DisposingCommittedTransaction_KeepsData()
    {
        using (DbTransaction tx = _connection.BeginTransaction())
        {
            Execute("INSERT INTO People (Id, Name) VALUES (11, 'Kept');");
            tx.Commit();
        }

        Assert.Equal(1L, Convert.ToInt64(Scalar("SELECT COUNT(*) FROM People WHERE Id = 11;")));
    }

    // ---------------------------------------------------------------- LE-21

    [Fact]
    public void QueryResult_RowsAffected_IsStructured()
    {
        Assert.Equal(-1, QueryResult.Default().RowsAffected);
        Assert.Equal(2, ExecuteNonQuery("UPDATE People SET Nick = 'x' WHERE Id >= 2;"));
        Assert.Equal(0, ExecuteNonQuery("DELETE FROM People WHERE Id = 99;"));
        Assert.Equal(0, ExecuteNonQuery("CREATE TABLE Empty (Id INT PRIMARY KEY);"));
    }

    [Fact]
    public void RowsAffected_DoesNotDependOnMessageText()
    {
        // A VARCHAR value that looks like the old scraped message must not be counted.
        Assert.Equal(1, ExecuteNonQuery("INSERT INTO People (Id, Name) VALUES (20, 'Rows affected: 500');"));
    }

    [Fact]
    public void PreparedNonQuery_ReportsRowsAffected()
    {
        using DataVoCommand cmd = _connection.CreateCommand();
        cmd.CommandText = "UPDATE People SET Nick = @nick WHERE Id = @id;";
        cmd.Parameters.AddWithValue("@nick", "n");
        cmd.Parameters.AddWithValue("@id", 1);
        cmd.Prepare();

        Assert.Equal(1, cmd.ExecuteNonQuery());
        cmd.Parameters[1].Value = 404;
        Assert.Equal(0, cmd.ExecuteNonQuery());
    }

    [Theory]
    [InlineData(CommandType.StoredProcedure)]
    [InlineData(CommandType.TableDirect)]
    public void CommandType_RejectsNonText(CommandType type)
    {
        using DataVoCommand cmd = _connection.CreateCommand();
        Assert.Throws<NotSupportedException>(() => cmd.CommandType = type);
        cmd.CommandType = CommandType.Text;
        Assert.Equal(CommandType.Text, cmd.CommandType);
    }

    [Fact]
    public void CommandBehavior_CloseConnection_ClosesWithReader()
    {
        using var connection = new DataVoConnection($"DataSource=Close_{Guid.NewGuid():N}");
        connection.Open();
        using DataVoCommand cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1;";

        using (DbDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
        {
            Assert.Equal(ConnectionState.Open, connection.State);
        }

        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public void CommandBehavior_SingleRow_StopsAfterFirstRow()
    {
        using DataVoCommand cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT Id FROM People ORDER BY Id;";
        using DbDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleRow);

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetInt32(0));
        Assert.False(reader.Read());
    }

    [Fact]
    public void CommandBehavior_SingleResult_HidesLaterResultSets()
    {
        using DataVoCommand cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT Id FROM People WHERE Id = 1; SELECT Name FROM People WHERE Id = 2;";

        using (DbDataReader all = cmd.ExecuteReader())
        {
            Assert.True(all.NextResult());
        }

        using DbDataReader single = cmd.ExecuteReader(CommandBehavior.SingleResult);
        Assert.False(single.NextResult());
    }

    [Fact]
    public void Command_RejectsTransactionFromAnotherConnection()
    {
        using var other = new DataVoConnection($"DataSource=Other_{Guid.NewGuid():N}");
        other.Open();
        using DbTransaction foreign = other.BeginTransaction();

        using DataVoCommand cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 1;";
        cmd.Transaction = foreign;

        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
    }

    [Fact]
    public void Command_RejectsCompletedTransaction()
    {
        DbTransaction tx = _connection.BeginTransaction();
        tx.Commit();

        using DataVoCommand cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT 1;";
        cmd.Transaction = tx;

        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
    }

    // ---------------------------------------------------------------- LE-22

    [Fact]
    public void Reader_IsClosed_AfterCloseAndDispose()
    {
        using DataVoCommand cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT Id FROM People;";
        DbDataReader reader = cmd.ExecuteReader();

        Assert.False(reader.IsClosed);
        reader.Close();
        Assert.True(reader.IsClosed);
        Assert.Throws<InvalidOperationException>(() => reader.Read());

        DbDataReader disposed = cmd.ExecuteReader();
        disposed.Dispose();
        Assert.True(disposed.IsClosed);
    }

    [Fact]
    public void Reader_NextResultPastEnd_KeepsMetadataSafe()
    {
        using DataVoCommand cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT Id FROM People;";
        using DbDataReader reader = cmd.ExecuteReader();

        Assert.False(reader.NextResult());
        Assert.False(reader.NextResult());
        Assert.Equal(0, reader.FieldCount);
        Assert.False(reader.HasRows);
        Assert.False(reader.Read());
    }

    [Fact]
    public void Reader_GetString_OnNull_ThrowsInvalidCast()
    {
        using DbDataReader reader = Reader("SELECT Nick FROM People WHERE Id = 1;");
        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(0));
        Assert.Throws<InvalidCastException>(() => reader.GetString(0));
    }

    [Fact]
    public void Reader_GetOrdinal_FallsBackToCaseInsensitive()
    {
        using DbDataReader reader = Reader("SELECT Id, Name FROM People WHERE Id = 2;");
        Assert.Equal(1, reader.GetOrdinal("name"));
        Assert.Equal(0, reader.GetOrdinal("ID"));
        Assert.Throws<IndexOutOfRangeException>(() => reader.GetOrdinal("missing"));
    }

    [Fact]
    public void Reader_GetChars_CopiesText()
    {
        using DbDataReader reader = Reader("SELECT Name FROM People WHERE Id = 3;");
        Assert.True(reader.Read());

        Assert.Equal(5, reader.GetChars(0, 0, null, 0, 0));
        var buffer = new char[4];
        long copied = reader.GetChars(0, 1, buffer, 1, 3);
        Assert.Equal(3, copied);
        Assert.Equal(new[] { '\0', 'a', 'r', 'o' }, buffer);
    }

    [Fact]
    public void Reader_GetBytes_RejectsNonBinary()
    {
        using DbDataReader reader = Reader("SELECT Name FROM People WHERE Id = 3;");
        Assert.True(reader.Read());
        Assert.Throws<InvalidCastException>(() => reader.GetBytes(0, 0, null, 0, 0));
    }

    [Fact]
    public void Reader_GetSchemaTable_DescribesColumns()
    {
        using DbDataReader reader = Reader("SELECT Id, Name FROM People;");
        DataTable schema = reader.GetSchemaTable()!;

        Assert.Equal(2, schema.Rows.Count);
        Assert.Equal("Id", schema.Rows[0][SchemaTableColumn.ColumnName]);
        Assert.Equal(0, schema.Rows[0][SchemaTableColumn.ColumnOrdinal]);
        Assert.Equal(typeof(int), schema.Rows[0][SchemaTableColumn.DataType]);
        Assert.Equal("Name", schema.Rows[1][SchemaTableColumn.ColumnName]);
        Assert.Equal(typeof(string), schema.Rows[1][SchemaTableColumn.DataType]);
    }

    [Fact]
    public void Reader_RecordsAffected_ReportsDml()
    {
        using (DbDataReader select = Reader("SELECT Id FROM People;"))
        {
            Assert.Equal(-1, select.RecordsAffected);
        }

        using DbDataReader update = Reader("UPDATE People SET Nick = 'z' WHERE Id = 1;");
        Assert.Equal(1, update.RecordsAffected);
    }

    [Fact]
    public void DataTableLoad_UsesSchemaTable()
    {
        using DbDataReader reader = Reader("SELECT Id, Name FROM People ORDER BY Id;");
        var table = new DataTable();
        table.Load(reader);

        Assert.Equal(3, table.Rows.Count);
        Assert.Equal("Alice", table.Rows[0]["Name"]);
    }

    [Fact]
    public void ParameterCollection_FollowsAdoNetContracts()
    {
        using DataVoCommand cmd = _connection.CreateCommand();
        cmd.Parameters.AddWithValue("@a", 1);
        DbParameterCollection parameters = cmd.Parameters;

        Assert.False(parameters.Contains((object)"not a parameter"));
        Assert.Equal(-1, parameters.IndexOf((object)42));
        Assert.Throws<IndexOutOfRangeException>(() => parameters["@missing"]);
        Assert.Throws<IndexOutOfRangeException>(() => parameters["@missing"] = new DataVoParameter("@missing", 1));
        Assert.Equal(1, parameters["@a"].Value);
    }

    private void Execute(string sql) => ExecuteNonQuery(sql);

    private int ExecuteNonQuery(string sql)
    {
        using DataVoCommand cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteNonQuery();
    }

    private object? Scalar(string sql)
    {
        using DataVoCommand cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    private DbDataReader Reader(string sql)
    {
        DataVoCommand cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteReader();
    }
}
