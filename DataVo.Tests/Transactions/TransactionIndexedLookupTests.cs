using System.Diagnostics;
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.StorageEngine.Config;
using Xunit.Abstractions;

namespace DataVo.Tests.Transactions;

/// <summary>
/// Regression, MVCC snapshot-isolation, and scale tests verifying that primary-key and secondary-index
/// equality lookups inside explicit transactions remain index-driven (<c>O(log N)</c> / <c>O(1)</c>)
/// even when the transaction has buffered writes or the table has retained MVCC version images.
/// </summary>
public sealed class TransactionIndexedLookupTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly List<string> _directories = [];

    public TransactionIndexedLookupTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public void Dispose()
    {
        foreach (string directory in _directories)
        {
            try
            {
                if (Directory.Exists(directory))
                {
                    Directory.Delete(directory, recursive: true);
                }
            }
            catch (IOException)
            {
            }
        }
    }

    public static TheoryData<StorageMode> StorageModes => new()
    {
        StorageMode.InMemory,
        StorageMode.Lsm
    };

    [Theory]
    [MemberData(nameof(StorageModes))]
    public void PointLookups_WithBufferedInsertsUpdatesDeletesAndSavepoints_UseIndexesWithoutTableScan(StorageMode mode)
    {
        using DataVoContext db = OpenContext(mode);
        SeedAccounts(db, count: 50);
        db.Engine.Diagnostics.Enabled = true;

        Run(db, "BEGIN TRANSACTION");

        // 1. Buffer an insert and look up both a committed row and the buffered row by PK and secondary index.
        Run(db, "INSERT INTO Accounts (Id, Code, Balance) VALUES (101, 'C101', 500)");

        AssertSingleBalanceNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Id = 25", expectedId: 25, expectedBalance: 250, expectedIndex: "_PK_Accounts");
        AssertSingleBalanceNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Id = 101", expectedId: 101, expectedBalance: 500, expectedIndex: "_PK_Accounts");
        AssertSingleBalanceNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Code = 'C025'", expectedId: 25, expectedBalance: 250, expectedIndex: "idx_accounts_code");
        AssertSingleBalanceNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Code = 'C101'", expectedId: 101, expectedBalance: 500, expectedIndex: "idx_accounts_code");

        // 2. Update a committed row without changing keys, and update the buffered insert.
        Run(db, "UPDATE Accounts SET Balance = 999 WHERE Id = 25");
        AssertNoTableScan(db.Engine.Diagnostics.LastQuery, "_PK_Accounts");
        Run(db, "UPDATE Accounts SET Balance = 777 WHERE Code = 'C101'");
        AssertNoTableScan(db.Engine.Diagnostics.LastQuery, "idx_accounts_code");

        AssertSingleBalanceNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Id = 25", expectedId: 25, expectedBalance: 999, expectedIndex: "_PK_Accounts");
        AssertSingleBalanceNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Id = 101", expectedId: 101, expectedBalance: 777, expectedIndex: "_PK_Accounts");

        // 3. Create a savepoint, change indexed keys and delete a row, verify lookups, then roll back to savepoint.
        Run(db, "SAVEPOINT sp_before_key_move");
        Run(db, "UPDATE Accounts SET Code = 'C025_NEW' WHERE Id = 25");
        AssertNoTableScan(db.Engine.Diagnostics.LastQuery, "_PK_Accounts");
        Run(db, "DELETE FROM Accounts WHERE Id = 30");
        AssertNoTableScan(db.Engine.Diagnostics.LastQuery, "_PK_Accounts");

        AssertEmptyLookupNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Code = 'C025'", expectedIndex: "idx_accounts_code");
        AssertSingleBalanceNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Code = 'C025_NEW'", expectedId: 25, expectedBalance: 999, expectedIndex: "idx_accounts_code");
        AssertEmptyLookupNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Id = 30", expectedIndex: "_PK_Accounts");

        Run(db, "ROLLBACK TO SAVEPOINT sp_before_key_move");

        // After savepoint rollback, Code='C025' and Id=30 must be visible again via index lookup.
        AssertSingleBalanceNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Code = 'C025'", expectedId: 25, expectedBalance: 999, expectedIndex: "idx_accounts_code");
        AssertEmptyLookupNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Code = 'C025_NEW'", expectedIndex: "idx_accounts_code");
        AssertSingleBalanceNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Id = 30", expectedId: 30, expectedBalance: 300, expectedIndex: "_PK_Accounts");

        Run(db, "COMMIT");

        AssertSingleBalanceNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Id = 25", expectedId: 25, expectedBalance: 999, expectedIndex: "_PK_Accounts");
        AssertSingleBalanceNoScan(db, "SELECT Id, Balance FROM Accounts WHERE Id = 101", expectedId: 101, expectedBalance: 777, expectedIndex: "_PK_Accounts");
    }

    [Theory]
    [MemberData(nameof(StorageModes))]
    public void SnapshotIsolation_WithRetainedImagesAndBufferedWrites_PreservesVisibilityWithoutTableScan(StorageMode mode)
    {
        using DataVoContext txSession = OpenContext(mode);
        SeedAccounts(txSession, count: 40);
        Guid concurrentSession = Guid.NewGuid();
        Run(txSession, "USE TxIdxDb", concurrentSession);

        txSession.Engine.Diagnostics.Enabled = true;

        // Open an explicit transaction on txSession and buffer a write so HasBufferedWrites is true.
        Run(txSession, "BEGIN TRANSACTION");
        Run(txSession, "INSERT INTO Accounts (Id, Code, Balance) VALUES (500, 'C500', 5000)");
        Run(txSession, "UPDATE Accounts SET Balance = 111 WHERE Id = 1");

        // Concurrent session commits updates, key changes, inserts, and deletes while txSession's snapshot is open.
        // This populates retained MVCC row images (HasRetainedImages == true).
        Run(txSession, "UPDATE Accounts SET Balance = 8888, Code = 'C010_CONCURRENT' WHERE Id = 10", concurrentSession);
        Run(txSession, "DELETE FROM Accounts WHERE Id = 20", concurrentSession);
        Run(txSession, "INSERT INTO Accounts (Id, Code, Balance) VALUES (600, 'C600', 6000)", concurrentSession);

        // txSession must still see pre-snapshot versions of Id=10 (Code='C010', Balance=100) and Id=20 via index,
        // must NOT see Id=600 or Code='C010_CONCURRENT', and must see its own buffered writes (Id=1, Id=500).
        AssertSingleBalanceNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Id = 10", expectedId: 10, expectedBalance: 100, expectedIndex: "_PK_Accounts");
        AssertSingleBalanceNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Code = 'C010'", expectedId: 10, expectedBalance: 100, expectedIndex: "idx_accounts_code");
        AssertEmptyLookupNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Code = 'C010_CONCURRENT'", expectedIndex: "idx_accounts_code");
        AssertSingleBalanceNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Id = 20", expectedId: 20, expectedBalance: 200, expectedIndex: "_PK_Accounts");
        AssertSingleBalanceNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Code = 'C020'", expectedId: 20, expectedBalance: 200, expectedIndex: "idx_accounts_code");
        AssertEmptyLookupNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Id = 600", expectedIndex: "_PK_Accounts");
        AssertSingleBalanceNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Id = 1", expectedId: 1, expectedBalance: 111, expectedIndex: "_PK_Accounts");
        AssertSingleBalanceNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Id = 500", expectedId: 500, expectedBalance: 5000, expectedIndex: "_PK_Accounts");

        // Concurrent session must not see txSession's uncommitted writes before COMMIT.
        AssertSingleBalanceNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Id = 1", expectedId: 1, expectedBalance: 10, expectedIndex: "_PK_Accounts", concurrentSession);
        AssertEmptyLookupNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Id = 500", expectedIndex: "_PK_Accounts", concurrentSession);

        Run(txSession, "ROLLBACK");

        // After ROLLBACK, txSession sees the committed state from concurrentSession.
        AssertSingleBalanceNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Id = 10", expectedId: 10, expectedBalance: 8888, expectedIndex: "_PK_Accounts");
        AssertSingleBalanceNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Code = 'C010_CONCURRENT'", expectedId: 10, expectedBalance: 8888, expectedIndex: "idx_accounts_code");
        AssertEmptyLookupNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Id = 20", expectedIndex: "_PK_Accounts");
        AssertEmptyLookupNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Id = 500", expectedIndex: "_PK_Accounts");
        AssertSingleBalanceNoScan(txSession, "SELECT Id, Balance FROM Accounts WHERE Id = 600", expectedId: 600, expectedBalance: 6000, expectedIndex: "_PK_Accounts");
    }

    [Theory]
    [MemberData(nameof(StorageModes))]
    public void CompiledAndPreparedQueries_InsideTransactionWithBufferedWrites_ObserveSnapshotAndOwnWrites(StorageMode mode)
    {
        using DataVoContext db = OpenContext(mode);
        SeedAccounts(db, count: 30);

        DataVoCompiledQueryPlan singleByIdPlan = DataVoCompiledQueryPlan.SelectSingle(
            "Accounts", ["Id", "Code", "Balance"], "Id", "id");
        DataVoCompiledQueryPlan manyByCodePlan = DataVoCompiledQueryPlan.SelectMany(
            "Accounts", ["Id", "Code", "Balance"], "Code", "code",
            accessPath: CompiledAccessPath.SingleColumnIndex,
            resolvedIndexName: "idx_accounts_code");

        DataVoPreparedSelectSingle<(int Id, string Code, int Balance)> preparedSingleById =
            DataVoCompiledQuery.PrepareSelectSingleTyped(
                db,
                singleByIdPlan,
                static r => (r.GetInt32("Id"), r.GetString("Code")!, r.GetInt32("Balance")));

        DataVoPreparedSelectMany<(int Id, string Code, int Balance)> preparedManyByCode =
            DataVoCompiledQuery.PrepareSelectManyTyped(
                db,
                manyByCodePlan,
                static r => (r.GetInt32("Id"), r.GetString("Code")!, r.GetInt32("Balance")));

        DataVoPreparedStatement preparedSqlSelectById = db.Prepare("SELECT Id, Code, Balance FROM Accounts WHERE Id = @id");
        DataVoPreparedStatement preparedSqlUpdateById = db.Prepare("UPDATE Accounts SET Balance = @balance WHERE Id = @id");
        DataVoPreparedStatement preparedSqlDeleteByCode = db.Prepare("DELETE FROM Accounts WHERE Code = @code");

        db.Engine.Diagnostics.Enabled = true;

        Run(db, "BEGIN TRANSACTION");
        Run(db, "INSERT INTO Accounts (Id, Code, Balance) VALUES (90, 'C090', 900)");
        Run(db, "UPDATE Accounts SET Balance = 555, Code = 'C005_UPD' WHERE Id = 5");
        Run(db, "DELETE FROM Accounts WHERE Id = 6");

        // DataVoPreparedStatement SELECT, UPDATE, DELETE inside a transaction use indexes without full scans.
        var prepRow90 = preparedSqlSelectById.Execute(("id", 90));
        Assert.False(prepRow90.IsError);
        Assert.Single(prepRow90.Data);
        Assert.Equal(900, Convert.ToInt32(prepRow90.Data[0]["Balance"]));
        AssertNoTableScan(db.Engine.Diagnostics.LastQuery, "_PK_Accounts");

        var prepUpd = preparedSqlUpdateById.Execute(("balance", 950), ("id", 90));
        Assert.False(prepUpd.IsError);
        AssertNoTableScan(db.Engine.Diagnostics.LastQuery, "_PK_Accounts");

        var prepDel = preparedSqlDeleteByCode.Execute(("code", "C007"));
        Assert.False(prepDel.IsError);
        AssertNoTableScan(db.Engine.Diagnostics.LastQuery, "idx_accounts_code");
        Assert.Equal(default, preparedSingleById.Execute(7));

        // Restore Balance=900 for the rest of the compiled query assertions.
        preparedSqlUpdateById.Execute(("balance", 900), ("id", 90));
        AssertNoTableScan(db.Engine.Diagnostics.LastQuery, "_PK_Accounts");

        // Prepared typed single lookup sees buffered insert, buffered update, and buffered delete.
        Assert.Equal((90, "C090", 900), preparedSingleById.Execute(90));
        Assert.Equal((5, "C005_UPD", 555), preparedSingleById.Execute(5));
        Assert.Equal(default, preparedSingleById.Execute(6));

        // Prepared typed multi-row lookup by secondary index sees updated code and buffered insert.
        Assert.Empty(preparedManyByCode.Execute("C005"));
        Assert.Equal([(5, "C005_UPD", 555)], preparedManyByCode.Execute("C005_UPD"));
        Assert.Equal([(90, "C090", 900)], preparedManyByCode.Execute("C090"));

        // Typed DataVoCompiledQuery.SelectSingleTyped and SelectManyTyped also observe transaction overlay.
        (int Id, int Balance) dictHit = DataVoCompiledQuery.SelectSingleTyped(
            db,
            singleByIdPlan,
            [new DataVoCompiledQueryParameter("id", 90)],
            static row => (row.GetInt32("Id"), row.GetInt32("Balance")));
        Assert.Equal((90, 900), dictHit);

        IReadOnlyList<(int Id, int Balance)> dictDeleted = DataVoCompiledQuery.SelectManyTyped(
            db,
            manyByCodePlan,
            [new DataVoCompiledQueryParameter("code", "C006")],
            static row => (row.GetInt32("Id"), row.GetInt32("Balance")));
        Assert.Empty(dictDeleted);

        Run(db, "COMMIT");

        Assert.Equal((90, "C090", 900), preparedSingleById.Execute(90));
        Assert.Equal((5, "C005_UPD", 555), preparedSingleById.Execute(5));
        Assert.Equal(default, preparedSingleById.Execute(6));
    }

    [Theory]
    [MemberData(nameof(StorageModes))]
    public void JoinWithIndexedWherePredicate_InsideTransactionWithBufferedWrites_UsesIndexWithoutFullScanOnFilteredTable(StorageMode mode)
    {
        using DataVoContext db = OpenContext(mode);
        SeedAccounts(db, count: 40);
        Run(db, "CREATE TABLE Badges (AccountId INT PRIMARY KEY, Label VARCHAR(20))");
        Run(db, "INSERT INTO Badges (AccountId, Label) VALUES (10, 'Gold'), (99, 'Diamond')");

        db.Engine.Diagnostics.Enabled = true;

        Run(db, "BEGIN TRANSACTION");
        Run(db, "INSERT INTO Accounts (Id, Code, Balance) VALUES (99, 'C099', 990)");
        Run(db, "UPDATE Accounts SET Balance = 1050 WHERE Id = 10");

        var joinedCommitted = db.Execute(
            "SELECT Accounts.Id, Accounts.Balance, Badges.Label FROM Accounts JOIN Badges ON Accounts.Id = Badges.AccountId WHERE Accounts.Id = 10").Last();
        Assert.False(joinedCommitted.IsError, string.Join("; ", joinedCommitted.Messages));
        Assert.Single(joinedCommitted.Data);
        Assert.Equal(1050, Convert.ToInt32(joinedCommitted.Data[0]["Accounts.Balance"] ?? joinedCommitted.Data[0]["Balance"]));
        Assert.Contains("_PK_Accounts", db.Engine.Diagnostics.LastQuery!.IndexesUsed);

        var joinedBuffered = db.Execute(
            "SELECT Accounts.Id, Accounts.Balance, Badges.Label FROM Accounts JOIN Badges ON Accounts.Id = Badges.AccountId WHERE Accounts.Id = 99").Last();
        Assert.False(joinedBuffered.IsError, string.Join("; ", joinedBuffered.Messages));
        Assert.Single(joinedBuffered.Data);
        Assert.Equal(990, Convert.ToInt32(joinedBuffered.Data[0]["Accounts.Balance"] ?? joinedBuffered.Data[0]["Balance"]));
        Assert.Contains("_PK_Accounts", db.Engine.Diagnostics.LastQuery!.IndexesUsed);

        Run(db, "COMMIT");
    }

    [Theory]
    [MemberData(nameof(StorageModes))]
    public void Scale_ManyIndexedSelectsAndUpdatesInSingleTransaction_StayFastAndNeverFullScan(StorageMode mode)
    {
        const int initialRows = 2_000;
        const int txOperations = 1_000;

        using DataVoContext db = OpenContext(mode);
        SeedAccounts(db, count: initialRows);

        db.Engine.Diagnostics.Enabled = true;
        db.Engine.Diagnostics.RecentQueryCapacity = 16;

        var sw = Stopwatch.StartNew();
        Run(db, "BEGIN TRANSACTION");

        for (int i = 1; i <= txOperations; i++)
        {
            int targetId = ((i - 1) % initialRows) + 1;
            var selectResult = db.Execute($"SELECT Id, Balance FROM Accounts WHERE Id = {targetId}").Last();
            Assert.False(selectResult.IsError);
            Assert.Single(selectResult.Data);
            AssertNoTableScan(db.Engine.Diagnostics.LastQuery, "_PK_Accounts");

            if (i % 5 == 0)
            {
                int newId = initialRows + i;
                Run(db, $"INSERT INTO Accounts (Id, Code, Balance) VALUES ({newId}, 'C{newId:D4}', {newId * 10})");
                var verifyInsert = db.Execute($"SELECT Id, Balance FROM Accounts WHERE Id = {newId}").Last();
                Assert.False(verifyInsert.IsError);
                Assert.Single(verifyInsert.Data);
                AssertNoTableScan(db.Engine.Diagnostics.LastQuery, "_PK_Accounts");
            }
            else
            {
                Run(db, $"UPDATE Accounts SET Balance = {i * 100} WHERE Id = {targetId}");
                AssertNoTableScan(db.Engine.Diagnostics.LastQuery, "_PK_Accounts");
            }
        }

        Run(db, "COMMIT");
        sw.Stop();

        _output.WriteLine($"[{mode}] {txOperations} in-transaction SELECT + UPDATE/INSERT pairs on {initialRows}-row table completed in {sw.Elapsed.TotalMilliseconds:F1} ms ({sw.Elapsed.TotalMicroseconds / txOperations:F1} us/pair).");

        Assert.True(
            sw.Elapsed < TimeSpan.FromSeconds(4),
            $"Expected {txOperations} in-transaction indexed operations on {mode} to complete in < 4s, but took {sw.Elapsed.TotalMilliseconds:F1} ms.");
    }

    private DataVoContext OpenContext(StorageMode mode)
    {
        string directory = Path.Combine(Path.GetTempPath(), "datavo-tx-idx-" + Guid.NewGuid().ToString("N"));
        _directories.Add(directory);
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = directory,
            LsmStrictFsync = false
        });
        Run(db, "CREATE DATABASE TxIdxDb");
        Run(db, "USE TxIdxDb");
        Run(db, "CREATE TABLE Accounts (Id INT PRIMARY KEY, Code VARCHAR(24), Balance INT)");
        Run(db, "CREATE INDEX idx_accounts_code ON Accounts (Code)");
        return db;
    }

    private static void SeedAccounts(DataVoContext db, int count)
    {
        const int batchSize = 250;
        for (int offset = 0; offset < count; offset += batchSize)
        {
            int limit = Math.Min(count, offset + batchSize);
            var values = new List<string>(limit - offset);
            for (int id = offset + 1; id <= limit; id++)
            {
                values.Add($"({id}, 'C{id:D3}', {id * 10})");
            }

            Run(db, $"INSERT INTO Accounts (Id, Code, Balance) VALUES {string.Join(", ", values)}");
        }
    }

    private static void AssertSingleBalanceNoScan(
        DataVoContext db,
        string sql,
        int expectedId,
        int expectedBalance,
        string expectedIndex,
        Guid? sessionId = null)
    {
        var result = (sessionId.HasValue ? db.Execute(sql, sessionId.Value) : db.Execute(sql)).Last();
        Assert.False(result.IsError, $"{sql}: {string.Join("; ", result.Messages)}");
        Dictionary<string, object?> row = Assert.Single(result.Data);
        Assert.Equal(expectedId, Convert.ToInt32(row["Id"]));
        Assert.Equal(expectedBalance, Convert.ToInt32(row["Balance"]));

        if (db.Engine.Diagnostics.Enabled)
        {
            AssertNoTableScan(db.Engine.Diagnostics.LastQuery, expectedIndex);
        }
    }

    private static void AssertEmptyLookupNoScan(
        DataVoContext db,
        string sql,
        string expectedIndex,
        Guid? sessionId = null)
    {
        var result = (sessionId.HasValue ? db.Execute(sql, sessionId.Value) : db.Execute(sql)).Last();
        Assert.False(result.IsError, $"{sql}: {string.Join("; ", result.Messages)}");
        Assert.Empty(result.Data);

        if (db.Engine.Diagnostics.Enabled)
        {
            AssertNoTableScan(db.Engine.Diagnostics.LastQuery, expectedIndex);
        }
    }

    private static void AssertNoTableScan(RuntimeQueryStats? stats, string expectedIndex)
    {
        Assert.NotNull(stats);
        Assert.False(stats.FullTableScan, $"Expected indexed execution without full table scan for '{stats.QueryText}', but FullTableScan was true (RowsScanned={stats.RowsScanned}).");
        Assert.Equal(0, stats.RowsScanned);
        Assert.Contains(expectedIndex, stats.IndexesUsed, StringComparer.OrdinalIgnoreCase);
    }

    private static void Run(DataVoContext db, string sql, Guid? sessionId = null)
    {
        var result = (sessionId.HasValue ? db.Execute(sql, sessionId.Value) : db.Execute(sql)).Last();
        Assert.False(result.IsError, $"{sql}: {string.Join("; ", result.Messages)}");
    }
}
