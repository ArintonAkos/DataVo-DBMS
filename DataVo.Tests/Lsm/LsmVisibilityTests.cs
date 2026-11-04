using System.Reflection;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

/// <summary>
/// In Lsm mode a statement's changes (rows and index entries) become visible to other sessions only once
/// its commit record is durable; concurrent writers cannot slip past each other's pending changes; and a
/// statement that fails leaves nothing behind.
/// </summary>
public sealed class LsmVisibilityTests
{
    [Fact]
    public async Task PendingInsert_IsInvisibleToOtherSessions_UntilPublished()
    {
        using DataVoContext db = Open();
        LsmStorageEngine engine = EngineOf(db);
        using var reachedPublish = new ManualResetEventSlim();
        using var allowPublish = new ManualResetEventSlim();
        engine.BeforePublishForTesting = _ =>
        {
            reachedPublish.Set();
            allowPublish.Wait(TimeSpan.FromSeconds(10));
        };

        Guid writer = Guid.NewGuid();
        Use(db, writer);
        Task<List<QueryResult>> insert = Task.Run(() => db.Execute("INSERT INTO T (Id, Name) VALUES (42, 'pending')", writer));
        Assert.True(reachedPublish.Wait(TimeSpan.FromSeconds(10)), "the insert never reached publication");

        // Durable but not yet published: invisible to every other session, in storage and in the index.
        Guid reader = Guid.NewGuid();
        Use(db, reader);
        Assert.Empty(db.Execute("SELECT * FROM T WHERE Id = 42", reader).Single().Data);
        Assert.Empty(db.Execute("SELECT * FROM T", reader).Single().Data);
        Assert.False(db.Engine.IndexManager.IndexContainsKey("[42]", "_PK_T", "T", "V"));

        engine.BeforePublishForTesting = null;
        allowPublish.Set();
        Assert.False((await insert.WaitAsync(TimeSpan.FromSeconds(10))).Single().IsError);

        Assert.Single(db.Execute("SELECT * FROM T WHERE Id = 42", reader).Single().Data);
        Assert.True(db.Engine.IndexManager.IndexContainsKey("[42]", "_PK_T", "T", "V"));
    }

    [Fact]
    public void ConcurrentInsertsOfTheSameKey_LeaveExactlyOneRow()
    {
        using DataVoContext db = Open();
        const int writers = 8;
        var outcomes = new QueryResult[writers];

        Parallel.For(0, writers, new ParallelOptions { MaxDegreeOfParallelism = writers }, i =>
        {
            Guid session = Guid.NewGuid();
            Use(db, session);
            outcomes[i] = db.Execute($"INSERT INTO T (Id, Name) VALUES (7, 'w{i}')", session).Single();
        });

        Assert.Single(outcomes, r => !r.IsError);
        Assert.All(outcomes.Where(r => r.IsError), r => Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, r.ErrorCode));
        Assert.Single(db.Execute("SELECT * FROM T WHERE Id = 7").Single().Data);
    }

    [Fact]
    public void FailedCommit_LeavesNoPartOfTheTransaction()
    {
        using DataVoContext db = Open();
        Guid first = Guid.NewGuid();
        Guid second = Guid.NewGuid();
        Use(db, first);
        Use(db, second);

        Assert.False(db.Execute("BEGIN", first).Single().IsError);
        Assert.False(db.Execute("INSERT INTO T (Id, Name) VALUES (50, 'a')", first).Single().IsError);
        Assert.False(db.Execute("INSERT INTO T (Id, Name) VALUES (51, 'b')", first).Single().IsError);
        Assert.False(db.Execute("INSERT INTO T (Id, Name) VALUES (51, 'other')", second).Single().IsError);

        QueryResult commit = db.Execute("COMMIT", first).Single();

        Assert.True(commit.IsError, string.Join(" / ", commit.Messages));
        List<Dictionary<string, object?>> rows = db.Execute("SELECT * FROM T").Single().Data;
        Assert.Single(rows);
        Assert.Equal("other", rows[0]["Name"]);
    }

    private static DataVoContext Open()
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = StorageMode.Lsm,
            DiskStoragePath = TestStorageDirectories.Create("lsm_visibility"),
            LsmStrictFsync = false,
        });
        db.Execute("CREATE DATABASE V");
        db.Execute("USE V");
        db.Execute("CREATE TABLE T (Id INT PRIMARY KEY, Name VARCHAR(20))");
        return db;
    }

    private static void Use(DataVoContext db, Guid session) => Assert.False(db.Execute("USE V", session).Single().IsError);

    private static LsmStorageEngine EngineOf(DataVoContext db)
    {
        object backend = typeof(StorageContext)
            .GetField("_storageEngine", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(db.Engine.StorageContext)!;
        return (LsmStorageEngine)backend.GetType()
            .GetField("_inner", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(backend)!;
    }
}
