using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Transactions;

/// <summary>
/// The in-transaction fast path for prepared single-row primary-key updates
/// (<c>UPDATE t SET col = col + @p WHERE Id = @id</c>). A differential oracle runs the same random transactional
/// workload through the fast path and through the general path (forced by enabling diagnostics) and requires
/// identical results after every statement and commit. The remaining cases cover MVCC conflicts, concurrent
/// sessions, durability across an LSM reopen, NULL handling, change capture and plan invalidation.
/// </summary>
[Collection("MvccStress")]
public sealed class PreparedArithmeticUpdateTests : IDisposable
{
    private const string AddScore = "UPDATE Board SET Score = Score + @points WHERE Id = @id";
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_pk_arith_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode, int> OracleCases => new()
    {
        { StorageMode.InMemory, 1 },
        { StorageMode.InMemory, 2 },
        { StorageMode.Lsm, 3 },
        { StorageMode.Lsm, 4 },
    };

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(OracleCases))]
    public void RandomTransactionalWorkload_MatchesTheGeneralPath(StorageMode mode, int seed)
    {
        using DataVoContext fast = Open(mode, "fast");
        using DataVoContext general = Open(mode, "general");
        general.Engine.Diagnostics.Enabled = true; // diagnostics route every statement through the general path
        var fastStatements = new Workload(fast);
        var generalStatements = new Workload(general);

        var random = new Random(seed);
        for (int transaction = 0; transaction < 60; transaction++)
        {
            Both(fast, general, "BEGIN");
            int statements = random.Next(1, 25);
            for (int i = 0; i < statements; i++)
            {
                (string Sql, DataVoQueryParameter[] Args) step = NextStep(random);
                QueryResult expected = generalStatements.Run(step.Sql, step.Args);
                QueryResult actual = fastStatements.Run(step.Sql, step.Args);
                Assert.Equal(expected.IsError, actual.IsError);
                Assert.Equal(expected.Messages, actual.Messages);
                Assert.Equal(Snapshot(general), Snapshot(fast)); // reads inside the transaction see the same rows
            }

            Both(fast, general, random.Next(5) == 0 ? "ROLLBACK" : "COMMIT");
            Assert.Equal(Snapshot(general), Snapshot(fast));
        }

        Assert.True(fastStatements.FastPathExecutions > 100, $"fast path ran {fastStatements.FastPathExecutions} times");
        Assert.Equal(0, generalStatements.FastPathExecutions);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ConcurrentUpdateOfTheSameRow_FirstCommitterWins(StorageMode mode)
    {
        using DataVoContext db = Open(mode, "conflict");
        Guid first = Session(db);
        Guid second = Session(db);
        DataVoPreparedStatement add = db.Prepare(AddScore);

        Ok(db, "BEGIN", first);
        Ok(db, "BEGIN", second);
        Assert.False(add.Execute([("points", 5), ("id", 1)], first).IsError);
        Assert.False(add.Execute([("points", 7), ("id", 1)], second).IsError);
        Ok(db, "COMMIT", first);

        QueryResult commit = db.Execute("COMMIT", second)[^1];
        Assert.True(commit.IsError);
        Assert.Contains(commit.Messages, message => message.Contains("conflict", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(15, Score(db, 1)); // 10 + 5: the losing increment is not applied
        Assert.Equal(2, add.TransactionalUpdateExecutions);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UpdateAfterAnotherSessionCommitted_SeesItsSnapshotAndConflictsAtCommit(StorageMode mode)
    {
        using DataVoContext db = Open(mode, "snapshot");
        Guid tx = Session(db);
        Guid other = Session(db);
        DataVoPreparedStatement add = db.Prepare(AddScore);

        Ok(db, "BEGIN", tx);
        Ok(db, "UPDATE Board SET Score = 100 WHERE Id = 2", other);

        // The row changed after BEGIN: the update is refused (first committer wins), like the general path.
        QueryResult update = add.Execute([("points", 1), ("id", 2)], tx);
        Assert.True(update.IsError);
        Assert.Contains(update.Messages, message => message.Contains("conflict", StringComparison.OrdinalIgnoreCase));
        Ok(db, "ROLLBACK", tx);
        Assert.Equal(100, Score(db, 2));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ParallelSessions_IncrementingSharedRows_LoseNoCommittedIncrement(StorageMode mode)
    {
        using DataVoContext db = Open(mode, "parallel");
        DataVoPreparedStatement add = db.Prepare(AddScore);
        var committedPoints = new long[Rows + 1];

        Task workers = Task.WhenAll(Enumerable.Range(0, 4).Select(worker => Task.Factory.StartNew(() =>
        {
            Guid session = Session(db);
            var random = new Random(worker + 100);
            for (int transaction = 0; transaction < 60; transaction++)
            {
                var pending = new long[Rows + 1];
                bool ok = !db.Execute("BEGIN", session)[^1].IsError;
                for (int i = 0; ok && i < 5; i++)
                {
                    int id = random.Next(1, Rows + 1);
                    int points = random.Next(1, 10);
                    ok = !add.Execute([("points", points), ("id", id)], session).IsError;
                    pending[id] += points;
                }

                if (ok && !db.Execute("COMMIT", session)[^1].IsError)
                {
                    lock (committedPoints)
                    {
                        for (int id = 1; id <= Rows; id++)
                        {
                            committedPoints[id] += pending[id];
                        }
                    }
                }
                else if (db.Engine.TransactionManager.HasActiveTransaction(session))
                {
                    db.Execute("ROLLBACK", session);
                }
            }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default)));

        Assert.True(workers.Wait(TimeSpan.FromSeconds(90)), "workers did not finish (deadlock?)");
        Assert.True(committedPoints.Sum() > 0);
        for (int id = 1; id <= Rows; id++)
        {
            Assert.Equal(id * 10 + committedPoints[id], Score(db, id));
        }

        Assert.Equal(0, db.Engine.VersionStorageManager.VersionCount);
    }

    [Fact]
    public void CommittedIncrementsSurviveAnLsmReopen_AndRolledBackOnesDoNot()
    {
        string path = Path.Combine(_directory, "durable");
        using (DataVoContext db = Open(StorageMode.Lsm, "durable", path))
        {
            DataVoPreparedStatement add = db.Prepare(AddScore);
            Ok(db, "BEGIN");
            Assert.False(add.Execute(("points", 5), ("id", 1)).IsError);
            Assert.False(add.Execute(("points", 5), ("id", 1)).IsError);
            Ok(db, "COMMIT");

            Ok(db, "BEGIN");
            Assert.False(add.Execute(("points", 1000), ("id", 2)).IsError);
            Ok(db, "ROLLBACK");
            Assert.Equal(3, add.TransactionalUpdateExecutions);
        }

        using DataVoContext reopened = new(new DataVoConfig { StorageMode = StorageMode.Lsm, DataDirectory = path });
        Ok(reopened, "USE Arith");
        Assert.Equal(20, Score(reopened, 1));
        Assert.Equal(20, Score(reopened, 2));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void NullOperands_FollowSqlNullRules_AndNotNullViolationsReportTheGeneralError(StorageMode mode)
    {
        using DataVoContext db = Open(mode, "nulls");
        DataVoPreparedStatement addBonus = db.Prepare("UPDATE Board SET Bonus = Bonus + @points WHERE Id = @id");
        DataVoPreparedStatement addScore = db.Prepare(AddScore);

        Ok(db, "BEGIN");
        Assert.False(addBonus.Execute(("points", 3), ("id", 1)).IsError); // NULL + 3 stays NULL
        QueryResult violation = addScore.Execute(DataVoQueryParameter.Null("points"), ("id", 1));
        Ok(db, "COMMIT");

        Assert.True(violation.IsError);
        Assert.Contains(violation.Messages, message => message.Contains("NOT NULL", StringComparison.OrdinalIgnoreCase));
        Assert.Null(Single(db, "SELECT Bonus FROM Board WHERE Id = 1")["Bonus"]);
        Assert.Equal(10, Score(db, 1));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void FastPathUpdates_ReachChangeCaptureAndReactiveViews(StorageMode mode)
    {
        using DataVoContext db = Open(mode, "capture");
        var top = new List<(long Id, long Score)>();
        using IDisposable view = db.SubscribeZeroAlloc(
            "SELECT Id, Score FROM Board ORDER BY Score DESC LIMIT 1",
            (in QueryChangeRef change) =>
            {
                for (int i = 0; i < change.Added.Count; i++)
                {
                    top.Add((change.Added[i][0].AsInt32(), change.Added[i][1].AsInt32()));
                }
            });

        db.Changes.Enabled = true;
        var captured = new List<RowChange>();
        db.Changes.Captured += set => captured.AddRange(set.Changes);
        DataVoPreparedStatement add = db.Prepare(AddScore);

        Ok(db, "BEGIN");
        Assert.False(add.Execute(("points", 500), ("id", 3)).IsError);
        Assert.False(add.Execute(("points", 1), ("id", 3)).IsError);
        Ok(db, "COMMIT");
        db.DispatchPendingNotifications();

        RowChange update = Assert.Single(captured, change => change.Kind == ChangeKind.Update);
        Assert.Equal(30, Convert.ToInt32(update.Before!["Score"]));
        Assert.Equal(531, Convert.ToInt32(update.After!["Score"]));
        Assert.Equal((3, 531), top[^1]);
        Assert.Equal(2, add.TransactionalUpdateExecutions);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void TypePromotion_AndConstantsAndSubtraction_MatchTheGeneralPath(StorageMode mode)
    {
        using DataVoContext fast = Open(mode, "types-fast");
        using DataVoContext general = Open(mode, "types-general");
        general.Engine.Diagnostics.Enabled = true;
        string[] statements =
        [
            "UPDATE Board SET Total = Total + @points WHERE Id = @id",
            "UPDATE Board SET Ratio = Ratio - @points WHERE Id = @id",
            "UPDATE Board SET Score = @points + Score, Total = 7 WHERE Id = @id",
            "UPDATE Board SET Score = Score - 2 WHERE Id = @id",
        ];

        Both(fast, general, "BEGIN");
        foreach (string sql in statements)
        {
            foreach (object points in new object[] { 3, 4_000_000_000L, 1.5 })
            {
                DataVoQueryParameter[] args = [Parameter("points", points), ("id", 2)];
                QueryResult expected = general.Prepare(sql).Execute(args);
                QueryResult actual = fast.Prepare(sql).Execute(args);
                Assert.Equal(expected.IsError, actual.IsError);
                Assert.Equal(expected.Messages, actual.Messages);
            }
        }

        Both(fast, general, "COMMIT");
        Assert.Equal(Snapshot(general), Snapshot(fast));
    }

    [Fact]
    public void SchemaChange_InvalidatesThePlan()
    {
        using DataVoContext db = Open(StorageMode.InMemory, "schema");
        DataVoPreparedStatement add = db.Prepare(AddScore);
        Ok(db, "BEGIN");
        Assert.False(add.Execute(("points", 1), ("id", 1)).IsError);
        Ok(db, "COMMIT");
        Assert.Equal(1, add.TransactionalUpdateExecutions);

        // Score becomes text: the plan is rebuilt and no longer applies, so the general path answers.
        Ok(db, "DROP TABLE Board");
        Ok(db, "CREATE TABLE Board (Id INT PRIMARY KEY, Score VARCHAR(10), Total BIGINT, Ratio DOUBLE, Bonus INT)");
        Ok(db, "INSERT INTO Board VALUES (1, 'a', 0, 0, NULL)");
        Ok(db, "BEGIN");
        QueryResult result = add.Execute(("points", 1), ("id", 1));
        Ok(db, "ROLLBACK");

        Assert.Equal(1, add.TransactionalUpdateExecutions);
        Assert.NotNull(result);
    }

    private const int Rows = 6;

    private sealed class Workload(DataVoContext db)
    {
        private readonly Dictionary<string, DataVoPreparedStatement> _prepared = [];

        public long FastPathExecutions => _prepared.Values.Sum(statement => statement.TransactionalUpdateExecutions);

        public QueryResult Run(string sql, DataVoQueryParameter[] args)
        {
            if (!_prepared.TryGetValue(sql, out DataVoPreparedStatement? statement))
            {
                statement = db.Prepare(sql);
                _prepared[sql] = statement;
            }

            return statement.Execute(args);
        }
    }

    /// <summary>
    /// One random statement: mostly arithmetic updates (some on missing keys), plus inserts, deletes and key
    /// changes that put rows in the transaction's insert buffer, delete set or update buffer under a new key.
    /// </summary>
    private static (string Sql, DataVoQueryParameter[] Args) NextStep(Random random)
    {
        int id = random.Next(1, Rows + 4); // ids above Rows are often missing
        int points = random.Next(-20, 50);
        return random.Next(10) switch
        {
            < 5 => (AddScore, [("points", points), ("id", id)]),
            5 => ("UPDATE Board SET Total = Total - @points, Ratio = Ratio + @points WHERE Id = @id", [("points", points), ("id", id)]),
            6 => ("INSERT INTO Board (Id, Score, Total, Ratio, Bonus) VALUES (@id, @points, 0, 0.5, NULL)", [("id", id), ("points", points)]),
            7 => ("DELETE FROM Board WHERE Id = @id", [("id", id)]),
            8 => ("UPDATE Board SET Id = @newId WHERE Id = @id", [("newId", random.Next(1, Rows + 4)), ("id", id)]),
            _ => ("UPDATE Board SET Bonus = Bonus + @points WHERE Id = @id", [("points", points), ("id", id)]),
        };
    }

    private static string Snapshot(DataVoContext db) =>
        string.Join(
            "\n",
            db.Execute("SELECT Id, Score, Total, Ratio, Bonus FROM Board")[^1].Data
                .Select(row => string.Join(",", new[] { "Id", "Score", "Total", "Ratio", "Bonus" }
                    .Select(column => $"{row[column]}:{row[column]?.GetType().Name}")))
                .Order());

    private DataVoContext Open(StorageMode mode, string name, string? path = null)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = mode == StorageMode.InMemory ? null! : path ?? Path.Combine(_directory, name),
        });
        Ok(db, "CREATE DATABASE Arith");
        Ok(db, "USE Arith");
        Ok(db, "CREATE TABLE Board (Id INT PRIMARY KEY, Score INT NOT NULL, Total BIGINT, Ratio DOUBLE, Bonus INT)");
        for (int id = 1; id <= Rows; id++)
        {
            Ok(db, $"INSERT INTO Board VALUES ({id}, {id * 10}, {id * 100}, {id}.25, NULL)");
        }

        return db;
    }

    private static DataVoQueryParameter Parameter(string name, object value) => value switch
    {
        int i => (name, i),
        long l => (name, l),
        double d => (name, d),
        _ => throw new ArgumentOutOfRangeException(nameof(value)),
    };

    private static Guid Session(DataVoContext db)
    {
        Guid session = Guid.NewGuid();
        Ok(db, "USE Arith", session);
        return session;
    }

    private static void Both(DataVoContext first, DataVoContext second, string sql)
    {
        QueryResult a = first.Execute(sql)[^1];
        QueryResult b = second.Execute(sql)[^1];
        Assert.Equal(b.IsError, a.IsError);
        Assert.Equal(b.Messages, a.Messages);
    }

    private static int Score(DataVoContext db, int id) => Convert.ToInt32(Single(db, $"SELECT Score FROM Board WHERE Id = {id}")["Score"]);

    private static IReadOnlyDictionary<string, object?> Single(DataVoContext db, string sql) =>
        Assert.Single(Ok(db, sql).Data);

    private static QueryResult Ok(DataVoContext db, string sql, Guid? session = null)
    {
        QueryResult result = session is { } id ? db.Execute(sql, id)[^1] : db.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
