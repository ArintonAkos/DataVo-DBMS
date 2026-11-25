using System.Collections.Concurrent;
using DataVo.Core;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;

namespace DataVo.Tests.Transactions;

/// <summary>
/// A single-row INSERT holds the table's intent lock and locks on its own primary-key and unique values instead of
/// the table's write lock. Inserts and updates of other rows no longer wait for each other, while writers giving rows
/// the same key value still check it one after the other, and DDL still waits for every insert.
/// </summary>
public sealed class InsertKeyLockTests : IDisposable
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Email", "Value");
    private readonly List<string> _directories = [];

    public void Dispose()
    {
        foreach (string directory in _directories)
        {
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public async Task ConcurrentTypedInserts_OfTheSamePrimaryKeys_InsertEachKeyOnce(StorageMode mode)
    {
        using DataVoContext context = CreateContext(mode);
        const int Keys = 300;
        var winners = new ConcurrentDictionary<int, int>();
        var unexpected = new ConcurrentQueue<Exception>();

        await RunThreads(6, writer =>
        {
            foreach (int id in Shuffled(Keys, seed: writer))
            {
                try
                {
                    context.InsertTyped("Records", Schema, Row(id, $"w{writer}-{id}@x"));
                    winners.AddOrUpdate(id, 1, static (_, count) => count + 1);
                }
                catch (Exception ex) when (IsKeyViolation(ex))
                {
                }
                catch (Exception ex)
                {
                    unexpected.Enqueue(ex);
                }
            }
        });

        Assert.Empty(unexpected);
        Assert.Equal(Keys, winners.Count);
        Assert.All(winners.Values, count => Assert.Equal(1, count));
        AssertDistinctIds(context, Keys);
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public async Task ConcurrentTypedInserts_OfTheSameUniqueValues_InsertEachValueOnce(StorageMode mode)
    {
        using DataVoContext context = CreateContext(mode);
        const int Values = 200;
        int nextId = 0;
        var winners = new ConcurrentDictionary<int, int>();
        var unexpected = new ConcurrentQueue<Exception>();

        await RunThreads(6, writer =>
        {
            foreach (int value in Shuffled(Values, seed: writer))
            {
                try
                {
                    context.InsertTyped("Records", Schema, Row(Interlocked.Increment(ref nextId), $"user{value}@x"));
                    winners.AddOrUpdate(value, 1, static (_, count) => count + 1);
                }
                catch (Exception ex) when (IsKeyViolation(ex))
                {
                }
                catch (Exception ex)
                {
                    unexpected.Enqueue(ex);
                }
            }
        });

        Assert.Empty(unexpected);
        Assert.Equal(Values, winners.Count);
        Assert.All(winners.Values, count => Assert.Equal(1, count));
        List<Dictionary<string, object?>> rows = context.Execute("SELECT Email FROM Records")[0].Data;
        Assert.Equal(Values, rows.Count);
        Assert.Equal(Values, rows.Select(row => (string?)row["Email"]).Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task ConcurrentSqlInserts_OfTheSameTextKeys_InsertEachKeyOnce()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        Execute(context, "CREATE DATABASE Keys");
        Execute(context, "USE Keys");
        Execute(context, "CREATE TABLE Tags (Name VARCHAR(20) PRIMARY KEY, Score INT)");
        const int Keys = 100;
        var winners = new ConcurrentDictionary<int, int>();
        var unexpected = new ConcurrentQueue<string>();

        await RunThreads(4, writer =>
        {
            Guid session = Guid.NewGuid();
            Execute(context, "USE Keys", session);
            foreach (int key in Shuffled(Keys, seed: writer))
            {
                QueryResultLike result = Run(context, $"INSERT INTO Tags (Name, Score) VALUES ('tag{key}', {writer})", session);
                if (!result.IsError)
                {
                    winners.AddOrUpdate(key, 1, static (_, count) => count + 1);
                }
                else if (!result.Message.Contains("violation", StringComparison.OrdinalIgnoreCase))
                {
                    unexpected.Enqueue(result.Message);
                }
            }
        });

        Assert.Empty(unexpected);
        Assert.Equal(Keys, winners.Count);
        Assert.All(winners.Values, count => Assert.Equal(1, count));
        Assert.Equal(Keys, context.Execute("SELECT Name FROM Tags")[0].Data.Count);
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public async Task InsertsAndKeyChangingUpdates_OfTheSameValues_NeverDuplicateAKey(StorageMode mode)
    {
        using DataVoContext context = CreateContext(mode);
        const int Rows = 50;
        context.InsertTypedBatch("Records", Schema, [.. Enumerable.Range(1, Rows).Select(id => Row(id, $"seed{id}@x"))]);
        var unexpected = new ConcurrentQueue<string>();

        // Updaters move rows 1..50 onto keys 1001..1100 while inserters insert those same keys: before an UPDATE
        // reserved its new key value, only the INSERT's table write lock kept the two apart.
        await RunThreads(4, writer =>
        {
            Guid session = Guid.NewGuid();
            Execute(context, "USE Keys", session);
            var random = new Random(writer);
            for (int i = 0; i < 150; i++)
            {
                int target = 1_001 + random.Next(100);
                if (writer % 2 == 0)
                {
                    QueryResultLike result = Run(context, $"UPDATE Records SET Id = {target} WHERE Id = {1 + random.Next(Rows)}", session);
                    if (result.IsError && !result.Message.Contains("violation", StringComparison.OrdinalIgnoreCase))
                    {
                        unexpected.Enqueue(result.Message);
                    }
                }
                else
                {
                    try
                    {
                        context.InsertTyped("Records", Schema, Row(target, null));
                    }
                    catch (Exception ex) when (IsKeyViolation(ex))
                    {
                    }
                    catch (Exception ex)
                    {
                        unexpected.Enqueue(ex.ToString());
                    }
                }
            }
        });

        Assert.Empty(unexpected);
        List<int> ids = [.. context.Execute("SELECT Id FROM Records")[0].Data.Select(row => Convert.ToInt32(row["Id"]))];
        Assert.Equal(ids.Count, ids.Distinct().Count());
        foreach (int id in ids)
        {
            Assert.Single(context.Execute($"SELECT Id FROM Records WHERE Id = {id}")[0].Data);
        }
    }

    [Fact]
    public async Task CommittedKeyChange_AndAnInsertOfTheSameKey_NeverBothSucceed()
    {
        using DataVoContext context = CreateContext(StorageMode.InMemory);
        context.InsertTypedBatch("Records", Schema, [.. Enumerable.Range(1, 40).Select(id => Row(id, $"seed{id}@x"))]);
        Guid session = Guid.NewGuid();
        Execute(context, "USE Keys", session);

        for (int round = 0; round < 40; round++)
        {
            int target = 5_000 + round;
            Execute(context, "BEGIN", session);
            Execute(context, $"UPDATE Records SET Id = {target} WHERE Id = {round + 1}", session);

            using var start = new Barrier(2);
            bool committed = false;
            bool inserted = false;
            Task commit = Task.Run(() =>
            {
                start.SignalAndWait();
                committed = !Run(context, "COMMIT", session).IsError;
            });
            Task insert = Task.Run(() =>
            {
                start.SignalAndWait();
                try
                {
                    context.InsertTyped("Records", Schema, Row(target, null));
                    inserted = true;
                }
                catch (Exception ex) when (IsKeyViolation(ex))
                {
                }
            });
            await Task.WhenAll(commit, insert);

            Assert.False(committed && inserted, $"round {round}: both the COMMIT and the INSERT gave a row key {target}");
            Assert.Single(context.Execute($"SELECT Id FROM Records WHERE Id = {target}")[0].Data);
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public async Task Insert_DoesNotWaitForAnUpdateOfAnotherRow(StorageMode mode)
    {
        using DataVoContext context = CreateContext(mode);
        context.InsertTyped("Records", Schema, Row(1, "one@x"));
        RowKey heldRow = context.Engine.IndexManager.FilterUsingIndex("[1]", "_PK_Records", "Records", "Keys")[0];
        using var held = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);

        // A row writer in the middle of its statement: it holds the row's write lock and the table's intent lock.
        Task updater = Task.Run(() =>
        {
            context.Engine.LockManager.AcquireRowWriteLock("Keys", "Records", heldRow);
            held.Set();
            release.Wait();
            context.Engine.LockManager.ReleaseRowWriteLock("Keys", "Records", heldRow);
        });

        Assert.True(held.Wait(5_000));
        try
        {
            Task insert = Task.Run(() => context.InsertTyped("Records", Schema, Row(2, "two@x")));
            Task sqlInsert = Task.Run(() => Execute(context, "INSERT INTO Records (Id, Email, Value) VALUES (3, 'three@x', 0)"));
            Assert.True(Task.WaitAll([insert, sqlInsert], TimeSpan.FromSeconds(5)), "an insert waited for an update of another row");
        }
        finally
        {
            release.Set();
            await updater;
        }

        Assert.Equal(3, context.Execute("SELECT Id FROM Records")[0].Data.Count);
    }

    [Fact]
    public async Task AlterTable_WaitsForAnInsertHoldingItsKeys()
    {
        using DataVoContext context = CreateContext(StorageMode.InMemory);
        using var held = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);

        // An insert between reserving its key and publishing its row.
        Task insert = Task.Run(() =>
        {
            KeyLockSet keys = KeyLockSet.Rent(context.Engine.LockManager, "Keys", "Records", takeTableIntent: true);
            try
            {
                keys.ReserveInsertKeys(context.Engine.Catalog, Schema, Row(7, "seven@x"));
                held.Set();
                release.Wait();
            }
            finally
            {
                keys.Release();
            }
        });

        Assert.True(held.Wait(5_000));
        Task alter = Task.Run(() => Execute(context, "ALTER TABLE Records ADD COLUMN Note VARCHAR(10)"));
        Assert.False(alter.Wait(300), "ALTER TABLE ran while an insert held the table's intent lock");

        release.Set();
        await insert;
        await alter.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Contains("Note", context.Engine.Catalog.GetTableColumns("Records", "Keys").Select(column => column.Name));
    }

    [Fact]
    public void KeyLocks_OfTheSameValue_Exclude_AndOtherValuesAndRowsDoNot()
    {
        var locks = new LockManager(lockAcquireTimeoutMs: 100);
        using var held = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        Task holder = Task.Run(() =>
        {
            Assert.True(locks.AcquireKeyLock("db", "t", KeyLockSet.PrimaryKeyScope, 5));
            held.Set();
            release.Wait();
            locks.ReleaseKeyLock("db", "t", KeyLockSet.PrimaryKeyScope, 5);
        });

        Assert.True(held.Wait(5_000));
        try
        {
            TimeoutException ex = Assert.Throws<TimeoutException>(() => locks.AcquireKeyLock("DB", "T", KeyLockSet.PrimaryKeyScope, 5));
            Assert.Contains("key write lock", ex.Message);

            // Another value, the same value of another constraint, and row 5 itself are all free.
            Assert.True(locks.AcquireKeyLock("db", "t", KeyLockSet.PrimaryKeyScope, 6));
            locks.ReleaseKeyLock("db", "t", KeyLockSet.PrimaryKeyScope, 6);
            Assert.True(locks.AcquireKeyLock("db", "t", "Email", 5));
            locks.ReleaseKeyLock("db", "t", "Email", 5);
            locks.AcquireRowWriteLock("db", "t", 5);
            locks.ReleaseRowWriteLock("db", "t", 5);

            // The holder's intent lock keeps exclusive table writers out.
            Assert.Throws<TimeoutException>(() => locks.AcquireWriteLock("db", "t"));
        }
        finally
        {
            release.Set();
            holder.Wait();
        }

        Assert.True(locks.AcquireKeyLock("db", "t", KeyLockSet.PrimaryKeyScope, 5));
        locks.ReleaseKeyLock("db", "t", KeyLockSet.PrimaryKeyScope, 5);
        Assert.False(LockManager.CurrentThreadHoldsLocks);
        Assert.False(locks.HasActiveRowWriteLocks);
    }

    [Fact]
    public void KeyLock_UnderTheTableWriteLock_TakesNothing()
    {
        var locks = new LockManager();
        locks.AcquireWriteLock("db", "t");
        try
        {
            Assert.False(locks.AcquireKeyLock("db", "t", KeyLockSet.PrimaryKeyScope, 1));
            Assert.False(locks.AcquireKeyLock("db", "t", "Email", "a@x"));
        }
        finally
        {
            locks.ReleaseWriteLock("db", "t");
        }

        Assert.False(LockManager.CurrentThreadHoldsLocks);
    }

    [Fact]
    public async Task KeyLocks_TakenInOppositeOrders_AreReportedAsADeadlock()
    {
        var locks = new LockManager(lockAcquireTimeoutMs: 10_000);
        using var bothHeld = new Barrier(2);
        var deadlocks = new ConcurrentQueue<DeadlockDetectedException>();

        Task Writer(string first, string second) => Task.Run(() =>
        {
            locks.AcquireKeyLock("db", "t", "Email", first);
            try
            {
                bothHeld.SignalAndWait();
                try
                {
                    locks.AcquireKeyLock("db", "t", "Email", second);
                    locks.ReleaseKeyLock("db", "t", "Email", second);
                }
                catch (DeadlockDetectedException ex)
                {
                    deadlocks.Enqueue(ex);
                }
            }
            finally
            {
                locks.ReleaseKeyLock("db", "t", "Email", first);
            }
        });

        await Task.WhenAll(Writer("a@x", "b@x"), Writer("b@x", "a@x")).WaitAsync(TimeSpan.FromSeconds(10));

        DeadlockDetectedException deadlock = Assert.Single(deadlocks);
        Assert.Contains("#key:Email:", deadlock.Message);
    }

    [Theory]
    [InlineData("[42]", true, 42)]
    [InlineData("[-7]", true, -7)]
    [InlineData("[0]", true, 0)]
    [InlineData("[042]", false, 0)]
    [InlineData("[-0]", false, 0)]
    [InlineData("[+4]", false, 0)]
    [InlineData("[4.5]", false, 0)]
    [InlineData("[1,2]", false, 0)]
    [InlineData("[]", false, 0)]
    [InlineData("42", false, 0)]
    [InlineData("[99999999999999999999]", false, 0)]
    public void IntegerKeyStrings_LockByNumber_OnlyInTheirCanonicalForm(string encoded, bool isInteger, long expected)
    {
        Assert.Equal(isInteger, KeyLockSet.TryParseIntegerKey(encoded, out long number));
        if (isInteger)
        {
            Assert.Equal(expected, number);
        }
    }

    private DataVoContext CreateContext(StorageMode mode)
    {
        string? directory = null;
        if (mode == StorageMode.Lsm)
        {
            directory = TestStorageDirectories.Create("insert_key_locks");
            _directories.Add(directory);
        }

        var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory, LsmStrictFsync = false });
        Execute(context, "CREATE DATABASE Keys");
        Execute(context, "USE Keys");
        Execute(context, "CREATE TABLE Records (Id INT PRIMARY KEY, Email VARCHAR(40) UNIQUE, Value INT)");
        return context;
    }

    private static CellValue[] Row(int id, string? email) => [CellValue.From(id), CellValue.From(email), CellValue.From(0)];

    private static IEnumerable<int> Shuffled(int count, int seed)
    {
        int[] values = [.. Enumerable.Range(1, count)];
        new Random(seed).Shuffle(values);
        return values;
    }

    private static Task RunThreads(int count, Action<int> body)
    {
        using var start = new Barrier(count);
        Task[] tasks = [.. Enumerable.Range(0, count).Select(index => Task.Factory.StartNew(() =>
        {
            start.SignalAndWait();
            body(index);
        }, TaskCreationOptions.LongRunning))];
        return Task.WhenAll(tasks).WaitAsync(TimeSpan.FromMinutes(2));
    }

    private static bool IsKeyViolation(Exception ex) =>
        ex.Message.Contains("violation", StringComparison.OrdinalIgnoreCase);

    private static void AssertDistinctIds(DataVoContext context, int expected)
    {
        List<int> ids = [.. context.Execute("SELECT Id FROM Records")[0].Data.Select(row => Convert.ToInt32(row["Id"]))];
        Assert.Equal(expected, ids.Count);
        Assert.Equal(expected, ids.Distinct().Count());
    }

    private readonly record struct QueryResultLike(bool IsError, string Message);

    private static QueryResultLike Run(DataVoContext context, string sql, Guid session)
    {
        var result = context.Execute(sql, session).Last();
        return new QueryResultLike(result.IsError, string.Join(" ", result.Messages));
    }

    private static void Execute(DataVoContext context, string sql, Guid? session = null)
    {
        var results = session is { } id ? context.Execute(sql, id) : context.Execute(sql);
        Assert.False(results.Last().IsError, $"{sql}: {string.Join(" ", results.Last().Messages)}");
    }
}
