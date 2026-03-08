using DataVo.Data;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.ADO;

/// <summary>
/// Integration tests verifying the DataVo ADO.NET provider surface (connection, command, reader, transaction, parameters).
/// </summary>
[Collection("SequentialStorageTests")]
public class AdoNetTests : IDisposable
{
    private readonly DataVoConnection _connection;
    private readonly string _databaseName = $"AdoTestDb_{Guid.NewGuid():N}";

    public AdoNetTests()
    {
        TestEngineLock.Instance.Wait();
        _connection = new DataVoConnection($"StorageMode=InMemory;DataSource={_databaseName}");
        _connection.Open();
    }

    public void Dispose()
    {
        _connection.Close();
        _connection.Dispose();
        TestEngineLock.Instance.Release();
    }

    [Fact]
    public void ExecuteReader_ReturnsRows()
    {
        CreateAndSeedTable("Readers");

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT * FROM Readers;";

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.HasRows);

        int count = 0;
        while (reader.Read())
        {
            count++;
            int id = Convert.ToInt32(reader["Id"]);
            string name = reader["Name"]?.ToString()!;
            Assert.True(id > 0);
            Assert.False(string.IsNullOrEmpty(name));
        }

        Assert.Equal(3, count);
    }

    [Fact]
    public void ExecuteNonQuery_ReturnsAffectedRows()
    {
        CreateAndSeedTable("NonQuery");

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "DELETE FROM NonQuery WHERE Id = 1;";
        int affected = cmd.ExecuteNonQuery();

        Assert.Equal(1, affected);
    }

    [Fact]
    public void ExecuteScalar_ReturnsFirstColumnFirstRow()
    {
        CreateAndSeedTable("Scalar");

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT Name FROM Scalar WHERE Id = 1;";
        object? result = cmd.ExecuteScalar();

        Assert.NotNull(result);
        Assert.Equal("Alice", result!.ToString());
    }

    [Fact]
    public void ParameterizedQuery_SubstitutesCorrectly()
    {
        CreateAndSeedTable("Params");

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT * FROM Params WHERE Id = @id;";
        cmd.Parameters.AddWithValue("@id", 2);

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Bob", reader["Name"]?.ToString());
        Assert.False(reader.Read());
    }

    [Fact]
    public void Transaction_Commit_PersistsData()
    {
        Execute("CREATE TABLE IF NOT EXISTS TxCommit (Id INT PRIMARY KEY, Name VARCHAR(50));");

        using var tx = _connection.BeginTransaction();
        Execute("INSERT INTO TxCommit (Id, Name) VALUES (1, 'Alice');");
        tx.Commit();

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT * FROM TxCommit;";
        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Alice", reader["Name"]?.ToString());
    }

    [Fact]
    public void Transaction_Rollback_DiscardsData()
    {
        Execute("CREATE TABLE IF NOT EXISTS TxRollback (Id INT PRIMARY KEY, Name VARCHAR(50));");

        using var tx = _connection.BeginTransaction();
        Execute("INSERT INTO TxRollback (Id, Name) VALUES (1, 'Alice');");
        tx.Rollback();

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT * FROM TxRollback;";
        using var reader = cmd.ExecuteReader();

        Assert.False(reader.Read());
    }

    [Fact]
    public void ConnectionStringBuilder_ParsesCorrectly()
    {
        var builder = new DataVoConnectionStringBuilder("StorageMode=Disk;DataSource=./mydb;WalEnabled=false");
        Assert.Equal(StorageMode.Disk, builder.StorageMode);
        Assert.Equal("./mydb", builder.DataSource);
        Assert.False(builder.WalEnabled);
    }

    [Fact]
    public void GetOrdinal_ThrowsForUnknownColumn()
    {
        CreateAndSeedTable("Ordinal");

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT * FROM Ordinal;";
        using var reader = cmd.ExecuteReader();
        reader.Read();

        Assert.Throws<IndexOutOfRangeException>(() => reader.GetOrdinal("NonExistent"));
    }

    [Fact]
    public void MultipleStringParameters_SubstituteCorrectly()
    {
        CreateAndSeedTable("MultiParam");

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT * FROM MultiParam WHERE Name = @name;";
        cmd.Parameters.AddWithValue("@name", "Charlie");

        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(3, Convert.ToInt32(reader["Id"]));
        Assert.False(reader.Read());
    }

    private void CreateAndSeedTable(string tableName)
    {
        Execute($"CREATE TABLE IF NOT EXISTS {tableName} (Id INT PRIMARY KEY, Name VARCHAR(50));");
        Execute($"INSERT INTO {tableName} (Id, Name) VALUES (1, 'Alice');");
        Execute($"INSERT INTO {tableName} (Id, Name) VALUES (2, 'Bob');");
        Execute($"INSERT INTO {tableName} (Id, Name) VALUES (3, 'Charlie');");
    }

    private void Execute(string sql)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
