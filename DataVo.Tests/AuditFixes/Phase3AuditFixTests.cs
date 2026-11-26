using DataVo.Core;
using DataVo.Core.BTree;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
using DataVo.Core.Execution.Volcano;
using DataVo.Core.Indexing.BTree;
using DataVo.Core.Indexing.HNSW;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Runtime.Security;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.ValueConversion;
using Xunit;

namespace DataVo.Tests.AuditFixes;

/// <summary>
/// Regression tests for the Phase 3 audit fixes (LE-02, LE-04 … LE-11, LE-16) in <c>DataVo.Core</c>.
/// </summary>
public class Phase3AuditFixTests
{
    // ── LE-02: B-Tree default degree ────────────────────────────────────

    [Fact]
    public void BTreeIndexFactory_DefaultsToTheProductionMinimumDegree()
    {
        var index = (InMemoryBTreeIndex)new BTreeIndexFactory().CreateIndex("ix", "Id", []);

        Assert.Equal(InMemoryBTreeIndex.DefaultMinDegree, index.MinDegree);
        Assert.Equal(50, index.MinDegree);
    }

    [Fact]
    public void BTreeIndexFactory_HonorsAnExplicitMinimumDegree()
    {
        var index = (InMemoryBTreeIndex)new BTreeIndexFactory().CreateIndex("ix", "Id", new Dictionary<string, object> { ["minDegree"] = 4 });

        Assert.Equal(4, index.MinDegree);
    }

    // ── LE-04: security store & direct-API authorization ─────────────────

    [Fact]
    public void Security_SchemaOrSecurityRightsAlone_DoNotGrantDataAccess()
    {
        using DataVoContext db = SecuredContext();
        Ok(db, "CREATE TABLE Docs (Id INT PRIMARY KEY, Name VARCHAR)");
        Ok(db, "INSERT INTO Docs VALUES (1, 'Seed')");
        db.Engine.CreateUser("architect", "pw", roleName: null);
        db.Engine.GrantPermissionsToUser("architect", [DatabasePermission.ManageSchema, DatabasePermission.ManageSecurity]);

        var architect = Guid.NewGuid();
        Assert.True(db.Login("architect", "pw", architect));
        SessionPrincipal principal = db.Engine.GetSessionPrincipal(architect)!;

        // Before the fix these rights inferred DatabaseRole.Admin, which granted every permission.
        Assert.NotEqual(DatabaseRole.Admin, principal.Role);
        Assert.Throws<AuthorizationException>(() => db.Engine.AuthorizeDirectAccess(architect, DatabasePermission.ReadData));
        Assert.Throws<AuthorizationException>(() => db.Engine.AuthorizeDirectAccess(architect, DatabasePermission.WriteData));
        Assert.Throws<AuthorizationException>(() => db.Engine.AuthorizeDirectAccess(architect, DatabasePermission.Admin));
        db.Engine.AuthorizeDirectAccess(architect, DatabasePermission.ManageSchema);
        db.Engine.AuthorizeDirectAccess(architect, DatabasePermission.ManageSecurity);

        using (Switch(db, architect))
        {
            Assert.Contains("not authorized", Error(db, "SELECT * FROM Docs"), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("not authorized", Error(db, "INSERT INTO Docs VALUES (2, 'x')"), StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void Security_RoleMutatorsSeedBuiltInRolesWhenCalledFirst()
    {
        using var context = new DataVoContext(SecuredConfig());

        // Before the fix the built-in roles did not exist yet, so this threw "Role 'ReadOnly' does not exist".
        context.Engine.GrantPermissionsToRole(nameof(DatabaseRole.ReadOnly), [DatabasePermission.ManageTransactions]);
        context.Engine.CreateRole("analyst");

        Assert.True(context.Login("admin", "adminpw"));
        QueryResult roles = context.Execute("SHOW ROLES").Single();
        Assert.Contains(roles.Data, row => string.Equals(row["Role"]?.ToString(), "analyst", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(roles.Data, row =>
            string.Equals(row["Role"]?.ToString(), nameof(DatabaseRole.ReadOnly), StringComparison.OrdinalIgnoreCase)
            && (row["Permissions"]?.ToString() ?? string.Empty).Contains(nameof(DatabasePermission.ManageTransactions), StringComparison.Ordinal));
    }

    [Fact]
    public void Security_CreateAndDropUser_LeaveTheCallersConfigUntouched()
    {
        DataVoConfig config = SecuredConfig();
        using var context = new DataVoContext(config);

        context.Engine.CreateUser("bob", "pw", nameof(DatabaseRole.ReadOnly));
        Assert.Single(config.AuthorizationUsers);
        Assert.DoesNotContain(config.AuthorizationUsers, user => user.Username == "bob");

        context.Engine.DropUser("bob");
        context.Engine.DropUser("admin");
        Assert.Single(config.AuthorizationUsers);
        Assert.False(context.Login("admin", "adminpw"));
    }

    [Fact]
    public void Security_DropUser_LogsOutTheUsersActiveSessions()
    {
        using DataVoContext db = SecuredContext();
        Ok(db, "CREATE TABLE Docs (Id INT PRIMARY KEY)");
        db.Engine.CreateUser("alice", "pw", nameof(DatabaseRole.ReadWrite));
        Guid admin = db.SessionId;
        Guid alice;
        using (SessionScope scope = LogIn(db, "alice", "pw"))
        {
            alice = scope.Session;
            Ok(db, "SELECT * FROM Docs");
        }

        db.Engine.DropUser("alice");

        Assert.Null(db.Engine.GetSessionPrincipal(alice));
        Assert.NotNull(db.Engine.GetSessionPrincipal(admin));
        using (Switch(db, alice))
        {
            Assert.Contains("authentication is required", Error(db, "SELECT * FROM Docs"), StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void Security_GrantsAndRevokes_ApplyToSessionsAlreadyLoggedIn()
    {
        using DataVoContext db = SecuredContext();
        Ok(db, "CREATE TABLE Docs (Id INT PRIMARY KEY)");
        db.Engine.CreateRole("writers");
        db.Engine.CreateUser("alice", "pw", nameof(DatabaseRole.ReadOnly));
        Guid alice;
        using (SessionScope scope = LogIn(db, "alice", "pw"))
        {
            alice = scope.Session;
        }

        void AsAlice(Action action)
        {
            using (Switch(db, alice))
            {
                action();
            }
        }

        AsAlice(() => Assert.Contains("not authorized", Error(db, "INSERT INTO Docs VALUES (1)"), StringComparison.OrdinalIgnoreCase));

        db.Engine.GrantPermissionsToUser("alice", [DatabasePermission.WriteData]);
        AsAlice(() => Ok(db, "INSERT INTO Docs VALUES (1)"));

        db.Engine.RevokePermissionsFromUser("alice", [DatabasePermission.WriteData]);
        AsAlice(() => Assert.Contains("not authorized", Error(db, "INSERT INTO Docs VALUES (2)"), StringComparison.OrdinalIgnoreCase));

        db.Engine.GrantRoleToUser("writers", "alice");
        db.Engine.GrantPermissionsToRole("writers", [DatabasePermission.WriteData]);
        AsAlice(() => Ok(db, "INSERT INTO Docs VALUES (3)"));

        db.Engine.DropRole("writers");
        AsAlice(() => Assert.Contains("not authorized", Error(db, "INSERT INTO Docs VALUES (4)"), StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Security_DirectApis_EnforceTheSessionsPermissions()
    {
        using DataVoContext db = SecuredContext();
        Ok(db, "CREATE TABLE Docs (Id INT PRIMARY KEY, Name VARCHAR, Emb VECTOR(2))");
        Ok(db, "CREATE INDEX ix_emb ON Docs (Emb) USING HNSW");
        Ok(db, "INSERT INTO Docs VALUES (1, 'Seed', '[1,0]')");
        DataVoSnapshot adminSnapshot = db.CreateSnapshot();
        db.Engine.CreateUser("reader", "pw", nameof(DatabaseRole.ReadOnly));

        var schema = new ReactiveRowSchema("Id", "Name", "Emb");
        CellValue[] row = [CellValue.From(2), CellValue.From("x"), CellValue.FromVector([0f, 1f])];
        using (SessionScope reader = LogIn(db, "reader", "pw"))
        {
            Assert.Throws<AuthorizationException>(() => db.InsertTyped("Docs", schema, row));
            Assert.Throws<AuthorizationException>(() => db.InsertTypedBatch("Docs", schema, [row]));
            Assert.Throws<AuthorizationException>(() => db.BulkInsert("Docs", [new Dictionary<string, object?> { ["Id"] = 3, ["Name"] = "y" }]));
            Assert.Throws<AuthorizationException>(() => db.Insert("Docs", new DocRow { Id = 4, Name = "z" }));
            Assert.Throws<AuthorizationException>(() => db.ApplyBatch(new DataVoWriteBatch()));
            Assert.Throws<AuthorizationException>(() => db.RestoreSnapshot(adminSnapshot));

            // Reads stay open to the reader.
            Assert.Single(db.SearchNearest("Docs", "ix_emb", [1f, 0f], 1));
            Assert.Equal(1, CountRows(db.StreamRef("Docs")));
            db.Subscribe("SELECT Id FROM Docs", _ => { }).Dispose();
            Assert.NotNull(db.CreateSnapshot());

            // A session with no identity is refused every direct read as well.
            db.Logout();
            Assert.Throws<AuthorizationException>(() => db.SearchNearest("Docs", "ix_emb", [1f, 0f], 1));
            Assert.Throws<AuthorizationException>(() => db.StreamRef("Docs"));
            Assert.Throws<AuthorizationException>(() => db.Subscribe("SELECT Id FROM Docs", _ => { }));
            Assert.Throws<AuthorizationException>(() => db.SubscribeZeroAlloc("SELECT Id FROM Docs ORDER BY Id LIMIT 1", (in QueryChangeRef _) => { }));
            Assert.Throws<AuthorizationException>(() => db.CreateSnapshot());
        }

        Assert.Equal(1, CountRows(db.StreamRef("Docs"))); // back as admin
    }

    [Fact]
    public void Security_CompiledQueries_EnforceTheSessionsPermissions()
    {
        using DataVoContext db = SecuredContext();
        Ok(db, "CREATE TABLE Accounts (Id INT PRIMARY KEY, Balance INT)");
        Ok(db, "INSERT INTO Accounts VALUES (1, 10)");
        db.Engine.CreateUser("reader", "pw", nameof(DatabaseRole.ReadOnly));

        DataVoCompiledQueryPlan select = DataVoCompiledQueryPlan.SelectSingle("Accounts", ["Id", "Balance"], "Id", "id");
        DataVoCompiledQueryPlan update = DataVoCompiledQueryPlan.Update("Accounts", new Dictionary<string, string> { ["Balance"] = "b" }, "Id", "id");
        DataVoCompiledQueryParameter[] args = [new("id", 1), new("b", 20)];

        using (LogIn(db, "reader", "pw"))
        {
            Assert.NotNull(DataVoCompiledQuery.SelectSingle(db, select, args, row => row["Balance"]));
            Assert.Throws<AuthorizationException>(() => DataVoCompiledQuery.Update(db, update, args));
            Assert.Throws<AuthorizationException>(() => DataVoCompiledQuery.UpdateFixedWidthByPrimaryKey(
                db, update, DataVoFixedWidthValue.From(1), [DataVoFixedWidthValue.From(20)]));

            db.Logout();
            Assert.Throws<AuthorizationException>(() => DataVoCompiledQuery.SelectSingle(db, select, args, row => row["Balance"]));
        }

        Assert.Equal(1, DataVoCompiledQuery.Update(db, update, args)); // the admin may write
    }

    [Fact]
    public void Security_DirectApisWithoutAuthorization_StayOpen()
    {
        using DataVoContext context = Context(StorageMode.InMemory);
        Ok(context, "CREATE TABLE Docs (Id INT PRIMARY KEY, Name VARCHAR)");

        context.InsertTyped("Docs", new ReactiveRowSchema("Id", "Name"), [CellValue.From(1), CellValue.From("a")]);

        Assert.Equal(1, CountRows(context.StreamRef("Docs")));
        Assert.NotNull(context.CreateSnapshot());
    }

    // ── LE-05: ALTER TABLE / VACUUM rebuild every index kind ─────────────

    [Theory]
    [InlineData(StorageMode.InMemory, "HNSW")]
    [InlineData(StorageMode.InMemory, "FLAT")]
    [InlineData(StorageMode.Lsm, "HNSW")]
    public void AlterAndVacuum_KeepVectorIndexesSearchable(StorageMode mode, string kind)
    {
        using DataVoContext db = Context(mode);
        Ok(db, "CREATE TABLE Docs (Id INT PRIMARY KEY, Emb VECTOR(2))");
        Ok(db, $"CREATE INDEX ix_emb ON Docs (Emb) USING {kind}");
        Ok(db, "INSERT INTO Docs VALUES (1, '[1,0]'), (2, '[0,1]'), (3, '[0.7,0.7]')");

        Ok(db, "ALTER TABLE Docs ADD COLUMN Note VARCHAR");
        Assert.Equal(2, NearestId(db, [0f, 1f]));

        Ok(db, "DELETE FROM Docs WHERE Id = 1");
        Ok(db, "VACUUM Docs");
        Assert.Equal(3, NearestId(db, [1f, 0f]));
        Assert.Equal(2, NearestId(db, [0f, 1f]));

        Ok(db, "ALTER TABLE Docs DROP COLUMN Note");
        Assert.Equal(3, NearestId(db, [0.6f, 0.8f]));
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Vacuum_RebuildsScalarIndexesAgainstTheNewRowIds(StorageMode mode)
    {
        using DataVoContext db = Context(mode);
        Ok(db, "CREATE TABLE Items (Id INT PRIMARY KEY, Code VARCHAR UNIQUE, Qty INT)");
        Ok(db, "CREATE INDEX ix_qty ON Items (Qty)");
        Ok(db, "INSERT INTO Items VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
        Ok(db, "DELETE FROM Items WHERE Id = 1");

        Ok(db, "VACUUM Items");

        Assert.Equal("c", Assert.Single(Rows(db, "SELECT Code FROM Items WHERE Qty = 30"))["Code"]);
        Assert.Equal(20, Convert.ToInt32(Assert.Single(Rows(db, "SELECT Qty FROM Items WHERE Code = 'b'"))["Qty"]));
        Assert.Equal(3, Convert.ToInt32(Assert.Single(Rows(db, "SELECT Id FROM Items WHERE Id = 3"))["Id"]));
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void AlterTable_RejectsDroppingOrModifyingAColumnarIndexedColumn_AnyCase(StorageMode mode)
    {
        using DataVoContext db = Context(mode);
        Ok(db, "CREATE TABLE Ticks (Id INT PRIMARY KEY, Price INT, Note VARCHAR)");
        Ok(db, "CREATE INDEX cx ON Ticks (Price) USING COLUMNAR");

        Assert.Contains("indexed column", Error(db, "ALTER TABLE Ticks DROP COLUMN price"), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("indexed column", Error(db, "ALTER TABLE Ticks MODIFY COLUMN PRICE BIGINT"), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("PRIMARY KEY", Error(db, "ALTER TABLE Ticks DROP COLUMN id"), StringComparison.OrdinalIgnoreCase);

        // A differently-cased name still resolves to the stored column.
        Ok(db, "ALTER TABLE Ticks DROP COLUMN NOTE");
        Assert.DoesNotContain(db.Engine.Catalog.GetTableColumns("Ticks", Database(db)), column => column.Name == "Note");
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Vacuum_KeepsColumnarAggregatesCorrect(StorageMode mode)
    {
        using DataVoContext db = Context(mode);
        Ok(db, "CREATE TABLE Ticks (Id INT PRIMARY KEY, Price INT)");
        Ok(db, "CREATE INDEX cx ON Ticks (Price) USING COLUMNAR");
        Ok(db, "INSERT INTO Ticks VALUES " + string.Join(", ", Enumerable.Range(1, 100).Select(i => $"({i}, {i})")));
        Assert.Equal(5050L, Convert.ToInt64(Assert.Single(Rows(db, "SELECT SUM(Price) AS S FROM Ticks"))["S"]));

        Ok(db, "DELETE FROM Ticks WHERE Id <= 50");
        Ok(db, "VACUUM Ticks");

        Assert.Equal(3775L, Convert.ToInt64(Assert.Single(Rows(db, "SELECT SUM(Price) AS S FROM Ticks"))["S"]));
        Assert.Equal(50L, Convert.ToInt64(Assert.Single(Rows(db, "SELECT COUNT(*) AS N FROM Ticks WHERE Price > 50"))["N"]));
    }

    [Fact]
    public void Vacuum_DiscardsMvccEntriesNamingTheOldRowIds()
    {
        using DataVoContext db = Context(StorageMode.InMemory);
        Ok(db, "CREATE TABLE Items (Id INT PRIMARY KEY, Qty INT)");
        Ok(db, "INSERT INTO Items VALUES (1, 10), (2, 20)");
        string database = Database(db);
        db.Engine.VersionStorageManager.RetainImage(database, "Items", RowKey.FromInt64(1), new Dictionary<string, object?> { ["Id"] = 1, ["Qty"] = 10 }, xmin: 1, xmax: 2);
        db.Engine.VersionStorageManager.AllocateVersion(database, "Items", RowKey.FromInt64(2), xmin: 1);
        Assert.True(db.Engine.VersionStorageManager.HasRetainedImages(database, "Items"));

        Ok(db, "VACUUM Items");

        Assert.False(db.Engine.VersionStorageManager.HasRetainedImages(database, "Items"));
        Assert.Null(db.Engine.VersionStorageManager.GetVersion(database, "Items", RowKey.FromInt64(2)));
    }

    // ── LE-06: Volcano joins, cardinality feedback, EXPLAIN estimates ─────

    [Fact]
    public void VolcanoJoins_NeverMatchNullKeys()
    {
        static List<ExecutionRow> Left() =>
        [
            new(1, new Dictionary<string, object?> { ["Id"] = 1, ["Ref"] = null }),
            new(2, new Dictionary<string, object?> { ["Id"] = 2, ["Ref"] = 7 }),
        ];

        static List<ExecutionRow> Right() =>
        [
            new(10, new Dictionary<string, object?> { ["Key"] = null, ["Name"] = "null-key" }),
            new(11, new Dictionary<string, object?> { ["Key"] = 7L, ["Name"] = "seven" }),
        ];

        foreach (IQueryOperator join in new IQueryOperator[]
        {
            new InnerJoinOperator(new TableScanOperator(Left()), new TableScanOperator(Right()), "Ref", "Key", "L", "R"),
            new NestedLoopJoinOperator(new TableScanOperator(Left()), new TableScanOperator(Right()), "Ref", "Key", "L", "R"),
        })
        {
            ExecutionRow match = Assert.Single(OperatorPipelineRunner.ExecuteToList(join));
            Assert.Equal("seven", match["R.Name"]);
        }
    }

    [Fact]
    public void JoinCardinalityFeedback_IsScopedToItsEngine()
    {
        using DataVoContext first = Context(StorageMode.InMemory);
        using DataVoContext second = Context(StorageMode.InMemory);

        first.Engine.JoinCardinalityFeedback.Record([("A.Id=B.AId", 500)]);

        Assert.True(first.Engine.JoinCardinalityFeedback.TryGet("A.Id=B.AId", out long learned));
        Assert.Equal(500, learned);
        Assert.False(second.Engine.JoinCardinalityFeedback.TryGet("A.Id=B.AId", out _));
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Explain_CostReflectsTheTablesStoredRowCount(StorageMode mode)
    {
        using DataVoContext db = Context(mode);
        Ok(db, "CREATE TABLE Small (Id INT, Name VARCHAR)");
        Ok(db, "CREATE TABLE Large (Id INT, Name VARCHAR)");
        Ok(db, "INSERT INTO Large VALUES " + string.Join(", ", Enumerable.Range(1, 5_000).Select(i => $"({i}, 'n{i}')")));

        int small = Convert.ToInt32(Assert.Single(Rows(db, "EXPLAIN SELECT * FROM Small WHERE Name = 'x'"))["EstimatedCost"]);
        int large = Convert.ToInt32(Assert.Single(Rows(db, "EXPLAIN SELECT * FROM Large WHERE Name = 'x'"))["EstimatedCost"]);

        Assert.True(large > small, $"EXPLAIN cost of 5,000 rows ({large}) should exceed that of an empty table ({small}).");
    }

    // ── LE-07: typed-row compaction & WASM fallback capabilities ─────────

    [Fact]
    public void Vacuum_CompactsInMemoryTablesHoldingTypedRows()
    {
        using DataVoContext db = Context(StorageMode.InMemory);
        Ok(db, "CREATE TABLE Players (Id INT PRIMARY KEY, Score INT)");
        var schema = new ReactiveRowSchema("Id", "Score");
        db.InsertTypedBatch("Players", schema, [.. Enumerable.Range(1, 10).Select(i => new[] { CellValue.From(i), CellValue.From(i * 10) })]);
        Ok(db, "DELETE FROM Players WHERE Id <= 5");

        Ok(db, "VACUUM Players");

        Assert.Equal(5, Rows(db, "SELECT Id FROM Players").Count);
        Assert.Equal(90, Convert.ToInt32(Assert.Single(Rows(db, "SELECT Score FROM Players WHERE Id = 9"))["Score"]));
        db.InsertTyped("Players", schema, [CellValue.From(11), CellValue.From(110)]);
        Assert.Equal(6, Rows(db, "SELECT Id FROM Players").Count);
    }

    [Fact]
    public void WasmMode_WithoutAnOpfsEngine_KeepsEveryInMemoryCapability()
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Wasm });
        StorageContext storage = db.Engine.StorageContext;

        Assert.Equal("Wasm", storage.Backend!.BackendKind);
        Assert.NotNull(storage.ColumnarStore);
        Assert.True(storage.TryEstimateRowCount("Any", "Db", out _));
        Assert.NotNull(storage.CreateInMemorySnapshot());
    }

    // ── LE-08: batched fixed-width updates ───────────────────────────────

    [Fact]
    public void UpdateFixedWidthByPrimaryKeyBatch_RejectsAnActiveTransaction()
    {
        using DataVoContext db = Context(StorageMode.Lsm);
        Ok(db, "CREATE TABLE Records (Id INT PRIMARY KEY, Value INT, Score INT)");
        Ok(db, "INSERT INTO Records VALUES (1, 1, 1)");
        Ok(db, "BEGIN TRANSACTION");

        Assert.Throws<NotSupportedException>(() => DataVoCompiledQuery.UpdateFixedWidthByPrimaryKeyBatch(db, RecordsUpdate, [Entry(1, 5, 5)]));

        Ok(db, "ROLLBACK");
    }

    [Fact]
    public void UpdateFixedWidthByPrimaryKeyBatch_WaitsForTheTableWriteLock()
    {
        string root = TestStorageDirectories.Create("phase3-batch-lock");
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = root, LockAcquireTimeoutMs = 250 });
        Ok(db, "CREATE DATABASE Batches");
        Ok(db, "USE Batches");
        Ok(db, "CREATE TABLE Records (Id INT PRIMARY KEY, Value INT, Score INT)");
        Ok(db, "INSERT INTO Records VALUES (1, 1, 1), (2, 2, 2)");

        using var held = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var holder = Task.Run(() =>
        {
            db.Engine.LockManager.AcquireWriteLock("Batches", "Records");
            held.Set();
            release.Wait();
            db.Engine.LockManager.ReleaseWriteLock("Batches", "Records");
        });

        held.Wait();
        try
        {
            Assert.Throws<TimeoutException>(() => DataVoCompiledQuery.UpdateFixedWidthByPrimaryKeyBatch(db, RecordsUpdate, [Entry(1, 9, 9)]));
        }
        finally
        {
            release.Set();
            holder.Wait();
        }

        Assert.Equal(2, DataVoCompiledQuery.UpdateFixedWidthByPrimaryKeyBatch(db, RecordsUpdate, [Entry(1, 9, 9), Entry(2, 8, 8)]));
        Assert.Equal(9, Convert.ToInt32(Assert.Single(Rows(db, "SELECT Value FROM Records WHERE Id = 1"))["Value"]));
    }

    // ── LE-09: reactive buffer after the last subscription ───────────────

    [Fact]
    public void Reactive_StopsBufferingChangesOnceEverySubscriptionIsDisposed()
    {
        using DataVoContext db = Context(StorageMode.InMemory);
        Ok(db, "CREATE TABLE Players (Id INT PRIMARY KEY, Score INT)");
        Assert.False(db.Changes.Enabled);

        IDisposable subscription = db.Subscribe("SELECT Id FROM Players WHERE Score > 0", _ => { });
        Assert.True(db.Changes.Enabled);
        Ok(db, "INSERT INTO Players VALUES (1, 5)");
        Assert.Equal(1, db.Engine.Reactive.PendingChangeSetCount);

        subscription.Dispose();
        Assert.Equal(0, db.Engine.Reactive.PendingChangeSetCount);
        Assert.False(db.Changes.Enabled);

        for (int i = 2; i <= 50; i++)
        {
            Ok(db, $"INSERT INTO Players VALUES ({i}, 5)");
        }

        Assert.Equal(0, db.Engine.Reactive.PendingChangeSetCount);

        // Subscribing again re-attaches and delivers new changes.
        var seen = new List<QueryChange>();
        using IDisposable again = db.Subscribe("SELECT Id FROM Players WHERE Score > 0", seen.Add);
        Ok(db, "INSERT INTO Players VALUES (100, 5)");
        db.DispatchPendingNotifications();
        Assert.Single(seen);
    }

    [Fact]
    public void Reactive_KeepsCaptureEnabledWhenTheUserEnabledIt()
    {
        using DataVoContext db = Context(StorageMode.InMemory);
        Ok(db, "CREATE TABLE Players (Id INT PRIMARY KEY, Score INT)");
        db.Changes.Enabled = true;

        db.Subscribe("SELECT Id FROM Players", _ => { }).Dispose();

        Assert.True(db.Changes.Enabled);
    }

    // ── LE-10: DATETIME aggregates, GROUP BY keys, stray '(' ──────────────

    [Fact]
    public void MinMax_AcceptDateTimeColumns()
    {
        using DataVoContext db = Context(StorageMode.InMemory);
        Ok(db, "CREATE TABLE Events (Id INT PRIMARY KEY, Kind VARCHAR, At DATETIME)");
        Ok(db, "INSERT INTO Events VALUES (1, 'a', '2026-01-02T03:04:05Z'), (2, 'a', '2026-05-06T07:08:09Z'), (3, 'b', '2025-12-31T00:00:00Z')");

        Dictionary<string, object?> a = Assert.Single(Rows(db, "SELECT Kind, MIN(At) AS First, MAX(At) AS Last FROM Events WHERE Kind = 'a' GROUP BY Kind"));

        Assert.Equal(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc), a["First"]);
        Assert.Equal(new DateTime(2026, 5, 6, 7, 8, 9, DateTimeKind.Utc), a["Last"]);
    }

    [Fact]
    public void GroupByKeys_AreBuiltFromValuesAndNeverCollide()
    {
        Assert.NotEqual(GroupBy.BuildGroupKey(["a", "bc"]), GroupBy.BuildGroupKey(["ab", "c"]));
        Assert.NotEqual(GroupBy.BuildGroupKey([null]), GroupBy.BuildGroupKey([string.Empty]));
        Assert.NotEqual(GroupBy.BuildGroupKey(["V1:x"]), GroupBy.BuildGroupKey(["x"]));
        Assert.Equal(GroupBy.BuildGroupKey([1, "x"]), GroupBy.BuildGroupKey([1, "x"]));

        // 50,000 distinct values: 32-bit hash keys merge a pair here with probability ~25%; value keys never do.
        HashSet<string> keys = [.. Enumerable.Range(0, 50_000).Select(i => GroupBy.BuildGroupKey([$"value-{i}"]))];
        Assert.Equal(50_000, keys.Count);
    }

    [Fact]
    public void GroupBy_KeepsNullAndEmptyStringGroupsApart()
    {
        using DataVoContext db = Context(StorageMode.InMemory);
        Ok(db, "CREATE TABLE Tags (Id INT PRIMARY KEY, Label VARCHAR)");
        Ok(db, "INSERT INTO Tags VALUES (1, NULL), (2, ''), (3, ''), (4, 'x')");

        List<Dictionary<string, object?>> groups = Rows(db, "SELECT Label, COUNT(*) AS N FROM Tags GROUP BY Label");

        Assert.Equal(3, groups.Count);
    }

    [Theory]
    [InlineData("( CREATE TABLE T (Id INT)")]
    [InlineData("(USE Anything")]
    public void Parser_RejectsAStrayOpeningParenthesis(string sql)
    {
        using DataVoContext db = Context(StorageMode.InMemory);

        Assert.Contains("Unexpected token", Error(db, sql), StringComparison.OrdinalIgnoreCase);
    }

    // ── LE-11: HNSW factory dimension parameter ──────────────────────────

    [Fact]
    public void HnswFactory_WiresTheDimensionParameter()
    {
        var index = (HNSWIndex)new HNSWIndexFactory().CreateIndex("ix", "Emb", new Dictionary<string, object>
        {
            ["expectedCount"] = 16,
            ["dimension"] = 3,
        });

        index.Insert(RowKey.FromInt64(1), [1f, 0f, 0f]);
        Assert.Throws<ArgumentException>(() => index.Insert(RowKey.FromInt64(2), [1f, 0f]));
    }

    // ── LE-16: nullable converted types & DateOnly ───────────────────────

    [Fact]
    public void ValueConverters_MaterializeIntoNullableTargets()
    {
        var registry = new DataVoValueConverterRegistry();
        registry.RegisterRange([new DataVoValueConverter<Money>(
            "DataVo.Tests.Phase3.Money.v1",
            money => CellValue.From(money.Cents),
            value => new Money(Convert.ToInt64(value)))]);

        Assert.Equal(new Money(250), registry.ConvertMaterialized<Money?>(250L));
        Assert.Null(registry.ConvertMaterialized<Money?>(null));
        Assert.Equal(new Money(7), registry.ConvertMaterialized(7L, typeof(Money?)));
        Assert.Null(registry.ConvertMaterialized(null, typeof(Money?)));
    }

#if NET8_0_OR_GREATER
    [Fact]
    public void DataVoDateTime_AcceptsDateOnlyAsMidnightUtc()
    {
        Assert.True(DataVoDateTime.TryConvert(new DateOnly(2026, 11, 26), out long ticks));

        Assert.Equal(new DateTime(2026, 11, 26, 0, 0, 0, DateTimeKind.Utc).Ticks, ticks);
    }
#endif

    // ── helpers ──────────────────────────────────────────────────────────

    private static readonly DataVoCompiledQueryPlan RecordsUpdate = DataVoCompiledQueryPlan.Update(
        "Records",
        new Dictionary<string, string> { ["Value"] = "value", ["Score"] = "score" },
        whereColumn: "Id",
        whereParameterName: "id");

    private static DataVoFixedWidthUpdateBatchEntry Entry(int id, int value, int score) =>
        new(DataVoFixedWidthValue.From(id), DataVoFixedWidthValue.From(value), DataVoFixedWidthValue.From(score));

    private static DataVoConfig SecuredConfig() => new()
    {
        StorageMode = StorageMode.InMemory,
        EnableAuthorization = true,
        AllowAnonymousSession = false,
        AuthorizationUsers = [new DataVoAuthUser { Username = "admin", Password = "adminpw", Role = DatabaseRole.Admin }],
    };

    private static DataVoContext SecuredContext()
    {
        var context = new DataVoContext(SecuredConfig());
        Assert.True(context.Login("admin", "adminpw"));
        Ok(context, "CREATE DATABASE Secured");
        Ok(context, "USE Secured");
        return context;
    }

    /// <summary>
    /// Logs a user into a fresh session of the same engine, selects the admin session's database and makes the
    /// new session current; disposing switches back to the previous session.
    /// </summary>
    private static SessionScope LogIn(DataVoContext db, string user, string password)
    {
        string database = Database(db);
        Guid session = Guid.NewGuid();
        Assert.True(db.Login(user, password, session));
        SessionScope scope = Switch(db, session);
        Ok(db, $"USE {database}");
        return scope;
    }

    private static SessionScope Switch(DataVoContext db, Guid session)
    {
        var scope = new SessionScope(db, db.SessionId, session);
        db.SessionId = session;
        return scope;
    }

    private sealed class SessionScope(DataVoContext db, Guid previous, Guid session) : IDisposable
    {
        public Guid Session { get; } = session;

        public void Dispose() => db.SessionId = previous;
    }

    private static DataVoContext Context(StorageMode mode)
    {
        DataVoConfig config = mode == StorageMode.Lsm
            ? new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = TestStorageDirectories.Create("phase3") }
            : new DataVoConfig { StorageMode = mode };
        var context = new DataVoContext(config);
        string database = $"Phase3_{Guid.NewGuid():N}";
        Ok(context, $"CREATE DATABASE {database}");
        Ok(context, $"USE {database}");
        return context;
    }

    private static string Database(DataVoContext context) => context.Engine.Sessions.Get(context.SessionId)!;

    private static int NearestId(DataVoContext db, float[] query) =>
        Convert.ToInt32(Assert.Single(db.SearchNearest("Docs", "ix_emb", query, 1))["Id"]);

    private static int CountRows(RowRefStream stream)
    {
        int count = 0;
        foreach (DataVo.Core.RowRef _ in stream)
        {
            count++;
        }

        return count;
    }

    private static void Ok(DataVoContext context, string sql)
    {
        foreach (QueryResult result in context.Execute(sql))
        {
            Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        }
    }

    private static string Error(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Last();
        Assert.True(result.IsError, $"Expected '{sql}' to fail.");
        return string.Join(" | ", result.Messages);
    }

    private static List<Dictionary<string, object?>> Rows(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Last();
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        return result.Data;
    }

    private readonly record struct Money(long Cents);

    private sealed class DocRow
    {
        public int Id { get; set; }

        public string? Name { get; set; }
    }
}
