using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.MVCC;

/// <summary>
/// Several sessions run interleaved explicit transactions and auto-commit statements against the same rows. Write
/// conflicts must be detected (first committer wins, never a lost update), every snapshot must stay exactly what
/// it was at <c>BEGIN</c> for as long as the transaction lives, and nothing may deadlock or leak versions.
/// </summary>
/// <remarks>Bounded work per test (a few thousand statements); no unbounded loops.</remarks>
[Collection("MvccStress")]
public sealed class MvccConcurrencyStressTests : IDisposable
{
    private const int Accounts = 8;
    private const long Initial = 1_000;
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(90);

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_mvcc_stress_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ConflictingTransfers_NeverLoseAnUpdate(StorageMode mode)
    {
        DataVoContext db = Open(mode);
        Run(db, Session(db), "CREATE TABLE Transfers (Id BIGINT PRIMARY KEY, Amount BIGINT)");
        if (mode == StorageMode.Lsm)
        {
            db.Dispose();
            db = Reopen();
        }

        var committed = new System.Collections.Concurrent.ConcurrentDictionary<long, (int From, int To, long Amount)>();
        long nextTransfer = 0;

        RunWorkers(4, worker =>
        {
            Guid session = Session(db);
            var random = new Random(worker * 7 + 1);
            for (int i = 0; i < 150; i++)
            {
                long id = Interlocked.Increment(ref nextTransfer);
                int from = random.Next(1, Accounts + 1);
                int to = 1 + (from + random.Next(0, Accounts - 1)) % Accounts;
                long amount = random.Next(1, 20);
                if (TryRun(db, session,
                    "BEGIN",
                    $"UPDATE Accounts SET Bal = Bal - {amount} WHERE Id = {from}",
                    "SAVEPOINT s1",
                    $"UPDATE Accounts SET Bal = Bal + 1000 WHERE Id = {to}",
                    "ROLLBACK TO SAVEPOINT s1",
                    $"UPDATE Accounts SET Bal = Bal + {amount} WHERE Id = {to}",
                    $"INSERT INTO Transfers (Id, Amount) VALUES ({id}, {amount})",
                    "COMMIT"))
                {
                    committed[id] = (from, to, amount);
                }
            }
        });

        var expected = Enumerable.Range(1, Accounts).ToDictionary(id => id, _ => Initial);
        foreach ((int from, int to, long amount) in committed.Values)
        {
            expected[from] -= amount;
            expected[to] += amount;
        }

        Assert.NotEmpty(committed);
        AssertNoVersionsLeft(db);
        for (int pass = 0; pass < (mode == StorageMode.Lsm ? 2 : 1); pass++)
        {
            Guid check = Session(db);
            Assert.Equal(expected, Balances(db, check));
            long[] logged = [.. Single(db, check, "SELECT Id FROM Transfers").Data.Select(row => Convert.ToInt64(row["Id"])).Order()];
            Assert.Equal([.. committed.Keys.Order()], logged);

            if (mode == StorageMode.Lsm && pass == 0)
            {
                // Everything acknowledged must survive a clean reopen too.
                db.Dispose();
                db = Reopen();
            }
        }

        db.Dispose();
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void LongLivedReaders_KeepTheirSnapshot_WhileWritersCommit(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        using var stop = new CancellationTokenSource();
        var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();

        Task readers = Task.WhenAll(Enumerable.Range(0, 2).Select(reader => Task.Run(() =>
        {
            Guid session = Session(db);
            while (!stop.IsCancellationRequested)
            {
                Run(db, session, "BEGIN");
                Dictionary<int, long> first = Balances(db, session);
                for (int read = 0; read < 10 && !stop.IsCancellationRequested; read++)
                {
                    Dictionary<int, long> again = Balances(db, session);
                    long viaIndex = Convert.ToInt64(Single(db, session, "SELECT SUM(Bal) AS S FROM Accounts WHERE Id >= 1").Data[0]["S"]);
                    if (!again.SequenceEqual(first) || viaIndex != first.Values.Sum() || first.Values.Sum() != Accounts * Initial)
                    {
                        failures.Enqueue($"reader {reader}: snapshot moved ({string.Join(",", first)} -> {string.Join(",", again)}, sum {viaIndex})");
                    }
                }

                Run(db, session, "COMMIT");
            }
        })));

        RunWorkers(3, worker =>
        {
            Guid session = Session(db);
            var random = new Random(worker + 100);
            for (int i = 0; i < 120; i++)
            {
                int from = random.Next(1, Accounts + 1);
                int to = 1 + (from + random.Next(0, Accounts - 1)) % Accounts;
                long amount = random.Next(1, 20);
                TryRun(db, session,
                    "BEGIN",
                    $"UPDATE Accounts SET Bal = Bal - {amount} WHERE Id = {from}",
                    $"UPDATE Accounts SET Bal = Bal + {amount} WHERE Id = {to}",
                    i % 7 == 0 ? "ROLLBACK" : "COMMIT");
            }
        });

        stop.Cancel();
        Assert.True(readers.Wait(Deadline), "readers did not finish");
        Assert.Empty(failures);
        Assert.Equal(Accounts * Initial, Balances(db, Session(db)).Values.Sum());
        AssertNoVersionsLeft(db);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void InterleavedInsertsDeletesAndAutocommits_KeepEveryCountExact(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Run(db, Session(db), "CREATE TABLE Items (Id INT PRIMARY KEY, Owner INT, Qty INT)");
        Run(db, Session(db), "CREATE INDEX ix_owner ON Items (Owner)");
        long[] expectedPerOwner = new long[5];

        RunWorkers(4, worker =>
        {
            Guid session = Session(db);
            var random = new Random(worker + 500);
            for (int i = 0; i < 100; i++)
            {
                int id = worker * 10_000 + i;
                switch (random.Next(4))
                {
                    case 0:
                        // Auto-commit insert, then an auto-commit delete of half of them.
                        Run(db, session, $"INSERT INTO Items (Id, Owner, Qty) VALUES ({id}, {worker}, 1)");
                        if (i % 2 == 0)
                        {
                            Run(db, session, $"DELETE FROM Items WHERE Id = {id}");
                        }
                        else
                        {
                            Interlocked.Increment(ref expectedPerOwner[worker]);
                        }

                        break;
                    case 1:
                        // A transaction that inserts, re-reads and updates its own rows, then deletes one of them.
                        Run(db, session, "BEGIN");
                        Run(db, session, $"INSERT INTO Items (Id, Owner, Qty) VALUES ({id}, {worker}, 1)");
                        Run(db, session, $"INSERT INTO Items (Id, Owner, Qty) VALUES ({id + 5_000}, {worker}, 1)");
                        Run(db, session, $"UPDATE Items SET Qty = 2 WHERE Owner = {worker} AND Id >= {id}");
                        Run(db, session, $"DELETE FROM Items WHERE Id = {id + 5_000}");
                        if (Convert.ToInt32(Single(db, session, $"SELECT Qty FROM Items WHERE Id = {id}").Data[0]["Qty"]) != 2)
                        {
                            throw new InvalidOperationException("own update not visible");
                        }

                        Run(db, session, "COMMIT");
                        Interlocked.Increment(ref expectedPerOwner[worker]);
                        break;
                    case 2:
                        // A transaction rolled back after a savepoint round trip leaves nothing.
                        Run(db, session, "BEGIN");
                        Run(db, session, $"INSERT INTO Items (Id, Owner, Qty) VALUES ({id}, {worker}, 1)");
                        Run(db, session, "SAVEPOINT s");
                        Run(db, session, $"DELETE FROM Items WHERE Id = {id}");
                        Run(db, session, "ROLLBACK TO SAVEPOINT s");
                        Run(db, session, "ROLLBACK");
                        break;
                    default:
                        // A snapshot that counts this worker's rows twice must read the same number.
                        Run(db, session, "BEGIN");
                        long a = Convert.ToInt64(Single(db, session, $"SELECT COUNT(*) AS C FROM Items WHERE Owner = {worker}").Data[0]["C"]);
                        long b = Single(db, session, $"SELECT Id FROM Items WHERE Owner = {worker}").Data.Count;
                        Run(db, session, "COMMIT");
                        if (a != b || a != Interlocked.Read(ref expectedPerOwner[worker]))
                        {
                            throw new InvalidOperationException($"worker {worker}: count {a}, rows {b}, expected {expectedPerOwner[worker]}");
                        }

                        break;
                }
            }
        });

        Guid check = Session(db);
        for (int owner = 0; owner < 4; owner++)
        {
            Assert.Equal(expectedPerOwner[owner], Convert.ToInt64(Single(db, check, $"SELECT COUNT(*) AS C FROM Items WHERE Owner = {owner}").Data[0]["C"]));
        }

        AssertNoVersionsLeft(db);
    }

    private static void AssertNoVersionsLeft(DataVoContext db)
    {
        Assert.False(db.Engine.TransactionManager.HasAnyActiveTransaction());
        Assert.Equal(0, db.Engine.CommitClock.InFlightCount);
        Assert.Equal(0, db.Engine.VersionStorageManager.VersionCount);
        Assert.False(db.Engine.VersionStorageManager.HasRetainedImages("Stress", "Accounts"));
    }

    private static void RunWorkers(int count, Action<int> work)
    {
        Task all = Task.WhenAll(Enumerable.Range(0, count).Select(worker => Task.Factory.StartNew(
            () => work(worker), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default)));
        Assert.True(all.Wait(Deadline), "workers did not finish (deadlock?)");
    }

    /// <summary>Runs statements in order; on the first error rolls the transaction back and reports false.</summary>
    private static bool TryRun(DataVoContext db, Guid session, params string[] statements)
    {
        foreach (string sql in statements)
        {
            QueryResult result = db.Execute(sql, session)[^1];
            if (result.IsError)
            {
                if (!result.Messages.Any(message => message.Contains("conflict", StringComparison.OrdinalIgnoreCase)))
                {
                    throw new InvalidOperationException($"{sql}: {string.Join(" | ", result.Messages)}");
                }

                if (db.Engine.TransactionManager.HasActiveTransaction(session))
                {
                    Run(db, session, "ROLLBACK");
                }

                return false;
            }
        }

        return true;
    }

    private static Dictionary<int, long> Balances(DataVoContext db, Guid session) =>
        Single(db, session, "SELECT Id, Bal FROM Accounts").Data
            .ToDictionary(row => Convert.ToInt32(row["Id"]), row => Convert.ToInt64(row["Bal"]))
            .OrderBy(pair => pair.Key)
            .ToDictionary(pair => pair.Key, pair => pair.Value);

    private static Guid Session(DataVoContext db)
    {
        Guid session = Guid.NewGuid();
        Run(db, session, "USE Stress");
        return session;
    }

    private static void Run(DataVoContext db, Guid session, string sql) => Single(db, session, sql);

    private static QueryResult Single(DataVoContext db, Guid session, string sql)
    {
        QueryResult result = db.Execute(sql, session)[^1];
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}: {string.Join(" | ", result.Messages)}");
        }

        return result;
    }

    private DataVoContext Reopen() => new(new DataVoConfig
    {
        StorageMode = StorageMode.Lsm,
        DiskStoragePath = _directory,
        LsmStrictFsync = true,
    });

    private DataVoContext Open(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
            LsmStrictFsync = false,
        });
        db.Execute("CREATE DATABASE Stress");
        Guid setup = Session(db);
        Run(db, setup, "CREATE TABLE Accounts (Id INT PRIMARY KEY, Bal BIGINT)");
        for (int id = 1; id <= Accounts; id++)
        {
            Run(db, setup, $"INSERT INTO Accounts (Id, Bal) VALUES ({id}, {Initial})");
        }

        return db;
    }
}
