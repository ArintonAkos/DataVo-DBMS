using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// Table-level key constraints (PRIMARY KEY (A, B), UNIQUE (C), optional CONSTRAINT names) and the key
/// checks of UPDATE: a composite key clashes only when every column matches another row, a clash reports
/// a primary-key violation, and UNIQUE columns stay independent constraints.
/// </summary>
public sealed class SqlKeyConstraintTests : IDisposable
{
    private readonly List<string> _directories = [];

    public void Dispose()
    {
        foreach (string directory in _directories)
        {
            try { Directory.Delete(directory, recursive: true); } catch (IOException) { }
        }
    }

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    [Theory]
    [MemberData(nameof(Modes))]
    public void CompositePrimaryKey_RejectsOnlyAFullDuplicate(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "CREATE TABLE K (A INT, B INT, V VARCHAR(10), CONSTRAINT pk_k PRIMARY KEY (A, B))");
        Run(db, "INSERT INTO K (A, B, V) VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c')");

        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, Error(db, "INSERT INTO K (A, B, V) VALUES (1, 2, 'dup')"));
        Run(db, "INSERT INTO K (A, B, V) VALUES (2, 2, 'd')");
        Assert.Equal(["1,1,a", "1,2,b", "2,1,c", "2,2,d"], Rows(db, "SELECT A, B, V FROM K ORDER BY A, B"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Update_OfACompositeKeyColumn_ChecksTheWholeKey(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "CREATE TABLE K (A INT, B INT, V VARCHAR(10), PRIMARY KEY (A, B))");
        Run(db, "INSERT INTO K (A, B, V) VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c')");

        // B = 2 already exists (in row (1, 2)), but (2, 2) does not: allowed.
        Run(db, "UPDATE K SET B = 2 WHERE A = 2 AND B = 1");
        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, Error(db, "UPDATE K SET B = 1 WHERE A = 1 AND B = 2"));
        Assert.Equal(["1,1,a", "1,2,b", "2,2,c"], Rows(db, "SELECT A, B, V FROM K ORDER BY A, B"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Update_ThatTakesAnExistingKey_IsAPrimaryKeyViolation(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "CREATE TABLE T (Id INT PRIMARY KEY, V INT)");
        Run(db, "INSERT INTO T (Id, V) VALUES (1, 1), (2, 2)");

        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, Error(db, "UPDATE T SET Id = 2 WHERE Id = 1"));
        Run(db, "UPDATE T SET Id = 5 WHERE Id = 1");
        Assert.Equal(["2,2", "5,1"], Rows(db, "SELECT Id, V FROM T ORDER BY Id"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void TableLevelUnique_IsEnforcedOnInsertAndUpdate(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, "CREATE TABLE U (Id INT, E VARCHAR(10), PRIMARY KEY (Id), UNIQUE (E))");
        Run(db, "INSERT INTO U (Id, E) VALUES (1, 'x'), (2, 'y')");

        Assert.Equal(DataVoErrorCodes.UniqueViolation, Error(db, "INSERT INTO U (Id, E) VALUES (3, 'x')"));
        Assert.Equal(DataVoErrorCodes.UniqueViolation, Error(db, "UPDATE U SET E = 'x' WHERE Id = 2"));
        Run(db, "UPDATE U SET E = 'z' WHERE Id = 2");
        Run(db, "UPDATE U SET E = 'y' WHERE Id = 1");
        Assert.Equal(["1,y", "2,z"], Rows(db, "SELECT Id, E FROM U ORDER BY Id"));
    }

    [Theory]
    [InlineData("CREATE TABLE Bad (A INT PRIMARY KEY, B INT, PRIMARY KEY (B))")]
    [InlineData("CREATE TABLE Bad (A INT, PRIMARY KEY (Z))")]
    [InlineData("CREATE TABLE Bad (A INT, B INT, PRIMARY KEY (A), PRIMARY KEY (B))")]
    [InlineData("CREATE TABLE Bad (A INT, B INT, PRIMARY KEY (A, A))")]
    [InlineData("CREATE TABLE Bad (A INT, B INT, UNIQUE (A, B))")]
    public void InvalidTableConstraints_AreRejected(string sql)
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        Assert.True(db.Execute(sql).Last().IsError);
    }

    [Fact]
    public void AConstraintMayNameAColumnDeclaredAfterIt()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        Run(db, "CREATE TABLE L (PRIMARY KEY (Id), Id INT, V INT)");
        Run(db, "INSERT INTO L (Id, V) VALUES (1, 1)");
        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, Error(db, "INSERT INTO L (Id, V) VALUES (1, 2)"));
    }

    private DataVoContext Open(StorageMode mode)
    {
        string directory = Path.Combine(Path.GetTempPath(), "datavo-keys-" + Guid.NewGuid().ToString("N"));
        _directories.Add(directory);
        var db = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory, LsmStrictFsync = false });
        Run(db, "CREATE DATABASE D");
        Run(db, "USE D");
        return db;
    }

    private static string? Error(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql).Last();
        Assert.True(result.IsError, $"{sql} should fail");
        return result.ErrorCode;
    }

    private static string[] Rows(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql).Last();
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return result.Data.Select(row => string.Join(",", row.Values)).ToArray();
    }

    private static void Run(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql).Last();
        Assert.False(result.IsError, $"{sql}: {string.Join("; ", result.Messages)}");
    }
}
