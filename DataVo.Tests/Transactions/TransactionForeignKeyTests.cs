using DataVo.Data;

namespace DataVo.Tests.Transactions;

/// <summary>
/// Foreign-key checks on INSERTs buffered in a transaction see the parent rows the same transaction inserted.
/// </summary>
public class TransactionForeignKeyTests
{
    [Theory]
    [InlineData("InMemory")]
    [InlineData("Lsm")]
    public void Insert_ChildOfParentInsertedInSameTransaction_Commits(string storageMode)
    {
        using var connection = Open(storageMode);
        Execute(connection, "CREATE TABLE Parents (Id INT PRIMARY KEY, Name VARCHAR(20));");
        Execute(connection, "CREATE TABLE Children (Id INT PRIMARY KEY, ParentId INT NOT NULL REFERENCES Parents(Id));");

        using (var transaction = connection.BeginTransaction())
        {
            Execute(connection, "INSERT INTO Parents (Id, Name) VALUES (1, 'p');");
            Execute(connection, "INSERT INTO Children (Id, ParentId) VALUES (10, 1);");
            transaction.Commit();
        }

        Assert.Equal(1L, Scalar(connection, "SELECT COUNT(*) FROM Children WHERE ParentId = 1;"));
    }

    [Theory]
    [InlineData("InMemory")]
    [InlineData("Lsm")]
    public void Insert_ChildWithoutAnyParent_IsStillRejectedInTransaction(string storageMode)
    {
        using var connection = Open(storageMode);
        Execute(connection, "CREATE TABLE Parents (Id INT PRIMARY KEY, Name VARCHAR(20));");
        Execute(connection, "CREATE TABLE Children (Id INT PRIMARY KEY, ParentId INT NOT NULL REFERENCES Parents(Id));");

        using var transaction = connection.BeginTransaction();
        Execute(connection, "INSERT INTO Parents (Id, Name) VALUES (1, 'p');");

        var ex = Assert.Throws<DataVoException>(() => Execute(connection, "INSERT INTO Children (Id, ParentId) VALUES (10, 2);"));
        Assert.Contains("Foreign key violation", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("InMemory")]
    [InlineData("Lsm")]
    public void Rollback_DiscardsParentAndChildInsertedTogether(string storageMode)
    {
        using var connection = Open(storageMode);
        Execute(connection, "CREATE TABLE Parents (Id INT PRIMARY KEY, Name VARCHAR(20));");
        Execute(connection, "CREATE TABLE Children (Id INT PRIMARY KEY, ParentId INT NOT NULL REFERENCES Parents(Id));");

        using (var transaction = connection.BeginTransaction())
        {
            Execute(connection, "INSERT INTO Parents (Id, Name) VALUES (1, 'p');");
            Execute(connection, "INSERT INTO Children (Id, ParentId) VALUES (10, 1);");
            transaction.Rollback();
        }

        Assert.Equal(0L, Scalar(connection, "SELECT COUNT(*) FROM Children;"));
        Assert.Equal(0L, Scalar(connection, "SELECT COUNT(*) FROM Parents;"));
    }

    private static DataVoConnection Open(string storageMode)
    {
        string dataSource = storageMode == "Lsm"
            ? TestStorageDirectories.Create("tx_fk")
            : $"tx_fk_{Guid.NewGuid():N}";
        var connection = new DataVoConnection($"StorageMode={storageMode};DataSource={dataSource}");
        connection.Open();
        return connection;
    }

    private static void Execute(DataVoConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static long Scalar(DataVoConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(command.ExecuteScalar());
    }
}
