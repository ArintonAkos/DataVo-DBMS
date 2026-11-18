using DataVo.Core;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Batches;

/// <summary>
/// <see cref="DataVoContext.ApplyBatch"/> writes primary-key upserts and deletes as one atomic unit on the
/// in-memory and LSM engines, with the same constraints, indexes and change capture as SQL.
/// </summary>
public sealed class ApplyBatchTests : IDisposable
{
    private static readonly DataVoBatchLayout Players = new("Players", ["Id", "Name", "Hp"]);
    private static readonly DataVoBatchLayout PlayerHp = new("Players", ["Id", "Hp"]);
    private static readonly DataVoBatchLayout Position = new("ResumePosition", ["Source", "Lsn"]);

    private readonly List<string> _roots = [];
    private readonly List<DataVoContext> _contexts = [];

    public static TheoryData<StorageMode> Modes => [StorageMode.InMemory, StorageMode.Lsm];

    public void Dispose()
    {
        foreach (DataVoContext context in _contexts)
        {
            context.Dispose();
        }

        foreach (string root in _roots)
        {
            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void InsertsUpdatesAndDeletes_ByPrimaryKey(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "INSERT INTO Players VALUES (1, 'Ann', 100), (2, 'Bob', 80), (3, 'Cid', 60)");

        var batch = new DataVoWriteBatch();
        batch.Upsert(Players, 1, "Ann", 90);    // update
        batch.Upsert(Players, 4, "Dee", 50);    // insert
        batch.Delete(Players, 2, null, null);   // delete
        batch.Delete(Players, 9, null, null);   // missing: unchanged
        batch.Upsert(Players, 3, "Cid", 60);    // same values: unchanged

        Assert.Equal(new DataVoBatchResult(Inserted: 1, Updated: 1, Deleted: 1, Unchanged: 2), context.ApplyBatch(batch));
        Assert.Equal(["1:Ann:90", "3:Cid:60", "4:Dee:50"], Rows(context));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void PartialLayout_KeepsTheOtherColumns(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "INSERT INTO Players VALUES (1, 'Ann', 100)");

        var batch = new DataVoWriteBatch();
        batch.Upsert(PlayerHp, 1, 70);
        batch.Upsert(PlayerHp, 2, 10); // new row: Name takes its default (NULL)
        context.ApplyBatch(batch);

        Assert.Equal(["1:Ann:70", "2::10"], Rows(context));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void OperationsOnOneRow_FoldBeforeWriting(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "INSERT INTO Players VALUES (1, 'Ann', 100)");

        var batch = new DataVoWriteBatch();
        batch.Upsert(Players, 5, "Eve", 1);
        batch.Delete(Players, 5, null, null);   // insert + delete: nothing
        batch.Upsert(PlayerHp, 6, 1);
        batch.Upsert(Players, 6, "Fay", 2);     // upserts merge, later wins
        batch.Delete(Players, 1, null, null);
        batch.Upsert(PlayerHp, 1, 5);           // delete + upsert: restarts from defaults

        DataVoBatchResult result = context.ApplyBatch(batch);

        Assert.Equal(new DataVoBatchResult(Inserted: 1, Updated: 1, Deleted: 0, Unchanged: 1), result);
        Assert.Equal(["1::5", "6:Fay:2"], Rows(context));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ReapplyingABatch_ChangesNothing_AndPublishesNothing(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        var batch = new DataVoWriteBatch();
        batch.Upsert(Players, 1, "Ann", 100);
        batch.Upsert(Position, "feed", 42L);
        context.ApplyBatch(batch);

        var captured = new List<ChangeSet>();
        context.Engine.Changes.Enabled = true;
        context.Engine.Changes.Captured += captured.Add;

        Assert.Equal(new DataVoBatchResult(0, 0, 0, 2), context.ApplyBatch(batch));
        Assert.Empty(captured);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BookkeepingRow_CommitsWithTheData_InOneChangeSet(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        var captured = new List<ChangeSet>();
        context.Engine.Changes.Enabled = true;
        context.Engine.Changes.Captured += captured.Add;

        var batch = new DataVoWriteBatch();
        batch.Upsert(Players, 1, "Ann", 100);
        batch.Upsert(Position, "feed", 42L);
        using (ChangeCapture.BeginOrigin("replica"))
        {
            context.ApplyBatch(batch);
        }

        ChangeSet set = Assert.Single(captured);
        Assert.Equal("replica", set.Origin);
        Assert.Equal(["Players", "ResumePosition"], set.Tables);
        Assert.Equal("42", Scalar(context, "SELECT Lsn FROM ResumePosition WHERE Source = 'feed'"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void AnInvalidRow_RejectsTheWholeBatch_BeforeWriting(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "INSERT INTO Players VALUES (1, 'Ann', 100)");

        var batch = new DataVoWriteBatch();
        batch.Upsert(Players, 1, "Ann", 1);
        batch.Upsert(Players, 2, "Bob", 2);
        batch.Upsert(Position, "feed", "not a number");

        Assert.ThrowsAny<Exception>(() => context.ApplyBatch(batch));
        Assert.Equal(["1:Ann:100"], Rows(context));
        Assert.Equal("0", Scalar(context, "SELECT COUNT(*) FROM ResumePosition"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UniqueViolation_RejectsTheBatch(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "CREATE TABLE Users (Id INT PRIMARY KEY, Email VARCHAR(40) UNIQUE)");
        Ok(context, "INSERT INTO Users VALUES (1, 'a@x'), (2, 'b@x')");
        var users = new DataVoBatchLayout("Users", ["Id", "Email"]);

        var batch = new DataVoWriteBatch();
        batch.Upsert(users, 3, "c@x");
        batch.Upsert(users, 2, "a@x");

        Assert.ThrowsAny<Exception>(() => context.ApplyBatch(batch));
        Assert.Equal("2", Scalar(context, "SELECT COUNT(*) FROM Users"));
        Assert.Equal("b@x", Scalar(context, "SELECT Email FROM Users WHERE Id = 2"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void NotNull_IsEnforced(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "CREATE TABLE Strict (Id INT PRIMARY KEY, Name VARCHAR(10) NOT NULL)");
        var strict = new DataVoBatchLayout("Strict", ["Id", "Name"]);

        var batch = new DataVoWriteBatch();
        batch.Upsert(strict, 1, null);

        Assert.ThrowsAny<Exception>(() => context.ApplyBatch(batch));
        Assert.Equal("0", Scalar(context, "SELECT COUNT(*) FROM Strict"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void KeyTypes_GuidBigintVarcharAndComposite(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "CREATE TABLE G (Id GUID PRIMARY KEY, V INT)");
        Ok(context, "CREATE TABLE B (Id BIGINT PRIMARY KEY, V INT)");
        Ok(context, "CREATE TABLE S (Code VARCHAR(20) PRIMARY KEY, V INT)");
        Ok(context, "CREATE TABLE C (A INT, B VARCHAR(10), V INT, PRIMARY KEY (A, B))");
        Guid id = Guid.NewGuid();
        var g = new DataVoBatchLayout("G", ["Id", "V"]);
        var b = new DataVoBatchLayout("B", ["Id", "V"]);
        var s = new DataVoBatchLayout("S", ["Code", "V"]);
        var c = new DataVoBatchLayout("C", ["A", "B", "V"]);

        for (int round = 1; round <= 2; round++)
        {
            var batch = new DataVoWriteBatch();
            batch.Upsert(g, id, round);
            batch.Upsert(b, 5_000_000_000L, round);
            batch.Upsert(s, "alpha", round);
            batch.Upsert(c, 1, "x", round);
            batch.Upsert(c, 1, "y", round * 10);
            context.ApplyBatch(batch);
        }

        Assert.Equal("2", Scalar(context, $"SELECT V FROM G WHERE Id = '{id}'"));
        Assert.Equal("2", Scalar(context, "SELECT V FROM B WHERE Id = 5000000000"));
        Assert.Equal("2", Scalar(context, "SELECT V FROM S WHERE Code = 'alpha'"));
        Assert.Equal("20", Scalar(context, "SELECT V FROM C WHERE A = 1 AND B = 'y'"));
        Assert.Equal("1", Scalar(context, "SELECT COUNT(*) FROM G"));
        Assert.Equal("2", Scalar(context, "SELECT COUNT(*) FROM C"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ColumnTypes_FloatBitDateAndVector(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "CREATE TABLE Typed (Id INT PRIMARY KEY, F FLOAT, Flag BIT, Day DATE, Emb VECTOR(3))");
        var typed = new DataVoBatchLayout("Typed", ["Id", "F", "Flag", "Day", "Emb"]);

        var batch = new DataVoWriteBatch();
        batch.Upsert(typed, 1, 1.5, true, new DateTime(2026, 11, 18), new float[] { 1, 2, 3 });
        context.ApplyBatch(batch);
        DataVoBatchResult again = context.ApplyBatch(batch);

        Assert.Equal(1, again.Unchanged);
        Assert.Equal("1.5", Scalar(context, "SELECT F FROM Typed WHERE Id = 1"));
        Assert.Equal("True", Scalar(context, "SELECT Flag FROM Typed WHERE Id = 1"));
        Assert.Equal("2026-11-18", Scalar(context, "SELECT Day FROM Typed WHERE Id = 1"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SecondaryIndexes_FollowTheBatch(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "CREATE INDEX IX_Name ON Players (Name)");
        var batch = new DataVoWriteBatch();
        batch.Upsert(Players, 1, "Ann", 1);
        batch.Upsert(Players, 2, "Bob", 2);
        context.ApplyBatch(batch);

        batch.Clear();
        batch.Upsert(Players, 1, "Zed", 1);
        batch.Delete(Players, 2, null, null);
        context.ApplyBatch(batch);

        Assert.Equal("0", Scalar(context, "SELECT COUNT(*) FROM Players WHERE Name = 'Ann'"));
        Assert.Equal("0", Scalar(context, "SELECT COUNT(*) FROM Players WHERE Name = 'Bob'"));
        Assert.Equal("1", Scalar(context, "SELECT Id FROM Players WHERE Name = 'Zed'"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ForeignKeys_ParentInsertedEarlierInTheBatchCounts(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "CREATE TABLE Teams (Id INT PRIMARY KEY, Name VARCHAR(20))");
        Ok(context, "CREATE TABLE Members (Id INT PRIMARY KEY, TeamId INT REFERENCES Teams(Id))");
        var teams = new DataVoBatchLayout("Teams", ["Id", "Name"]);
        var members = new DataVoBatchLayout("Members", ["Id", "TeamId"]);

        var ok = new DataVoWriteBatch();
        ok.Upsert(teams, 1, "Red");
        ok.Upsert(members, 10, 1);
        context.ApplyBatch(ok);

        var orphan = new DataVoWriteBatch();
        orphan.Upsert(members, 11, 99);
        Assert.ThrowsAny<Exception>(() => context.ApplyBatch(orphan));
        Assert.Equal("1", Scalar(context, "SELECT COUNT(*) FROM Members"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ReactiveQueries_SeeTheBatch(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "INSERT INTO Players VALUES (1, 'Ann', 100), (2, 'Bob', 80)");
        var live = new Dictionary<int, int>();
        using IDisposable subscription = context.Subscribe("SELECT Id, Hp FROM Players WHERE Hp > 50", change =>
        {
            foreach (var row in change.Added.Concat(change.Updated))
            {
                live[(int)row["Id"]!] = (int)row["Hp"]!;
            }

            foreach (var row in change.Removed)
            {
                live.Remove((int)row["Id"]!);
            }
        });

        var batch = new DataVoWriteBatch();
        batch.Upsert(PlayerHp, 1, 70);
        batch.Upsert(PlayerHp, 2, 10);
        batch.Upsert(Players, 3, "Cid", 99);
        context.ApplyBatch(batch);
        context.DispatchPendingNotifications();

        Assert.Equal(new Dictionary<int, int> { [1] = 70, [3] = 99 }, live);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ForeignKeyFailure_LateInTheBatch_LeavesNothingWritten(StorageMode mode)
    {
        DataVoContext context = OpenWithTeams(mode);
        Ok(context, "INSERT INTO Players VALUES (1, 'Ann', 100), (2, 'Bob', 80)");

        var batch = new DataVoWriteBatch();
        batch.Delete(Players, 2, null, null);          // a delete,
        batch.Upsert(PlayerHp, 1, 5);                  // an update,
        batch.Upsert(TeamLayout, 9, "New");            // an insert,
        batch.Upsert(MemberLayout, 30, 99);            // then a reference to a team nobody has

        Assert.ThrowsAny<Exception>(() => context.ApplyBatch(batch));
        Assert.Equal(["1:Ann:100", "2:Bob:80"], Rows(context));
        Assert.Equal("0", Scalar(context, "SELECT COUNT(*) FROM Teams WHERE Id = 9"));
        Assert.Equal("2", Scalar(context, "SELECT COUNT(*) FROM Members"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void OnDeleteRestrict_RejectsTheWholeBatch(StorageMode mode)
    {
        DataVoContext context = OpenWithTeams(mode);
        Ok(context, "INSERT INTO Players VALUES (1, 'Ann', 100)");

        var batch = new DataVoWriteBatch();
        batch.Upsert(PlayerHp, 1, 5);
        batch.Delete(TeamLayout, 1, null); // member 10 still points at it

        Assert.ThrowsAny<Exception>(() => context.ApplyBatch(batch));
        Assert.Equal(["1:Ann:100"], Rows(context));
        Assert.Equal("1", Scalar(context, "SELECT COUNT(*) FROM Teams WHERE Id = 1"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void OnDeleteRestrict_PassesWhenTheBatchDeletesOrMovesTheChildren(StorageMode mode)
    {
        DataVoContext context = OpenWithTeams(mode);

        var batch = new DataVoWriteBatch();
        batch.Delete(TeamLayout, 1, null);            // parent first in the batch ...
        batch.Delete(MemberLayout, 10, null);         // ... its child later: still fine
        batch.Upsert(MemberLayout, 20, 1);            // member 20 moves from team 2 to team 1? no: team 1 is gone
        Assert.ThrowsAny<Exception>(() => context.ApplyBatch(batch));
        Assert.Equal("2", Scalar(context, "SELECT COUNT(*) FROM Members"));

        batch.Clear();
        batch.Delete(TeamLayout, 1, null);
        batch.Upsert(MemberLayout, 10, 2);            // the child moves to another team
        context.ApplyBatch(batch);
        Assert.Equal("0", Scalar(context, "SELECT COUNT(*) FROM Teams WHERE Id = 1"));
        Assert.Equal("2", Scalar(context, "SELECT TeamId FROM Members WHERE Id = 10"));

        batch.Clear();
        batch.Delete(TeamLayout, 2, null);
        batch.Delete(MemberLayout, 10, null);
        batch.Delete(MemberLayout, 20, null);
        Assert.Equal(3, context.ApplyBatch(batch).Deleted);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void OnDeleteCascade_RemovesChildren_AndARestrictGrandchildStopsEverything(StorageMode mode)
    {
        DataVoContext context = OpenWithOrders(mode);
        var orders = new DataVoBatchLayout("Orders", ["Id", "Note"]);

        var batch = new DataVoWriteBatch();
        batch.Delete(orders, 1, null);
        DataVoBatchResult result = context.ApplyBatch(batch);
        Assert.Equal(3, result.Deleted); // the order and its two lines
        Assert.Equal("0", Scalar(context, "SELECT COUNT(*) FROM OrderLines WHERE OrderId = 1"));

        batch.Clear();
        batch.Upsert(orders, 3, "kept?");
        batch.Delete(orders, 2, null);   // line 21 has a RESTRICT refund
        Assert.ThrowsAny<Exception>(() => context.ApplyBatch(batch));
        Assert.Equal("1", Scalar(context, "SELECT COUNT(*) FROM Orders WHERE Id = 2"));
        Assert.Equal("1", Scalar(context, "SELECT COUNT(*) FROM OrderLines WHERE OrderId = 2"));
        Assert.Equal("0", Scalar(context, "SELECT COUNT(*) FROM Orders WHERE Id = 3"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UpdateOfARowACascadeRemoves_EndsDeleted(StorageMode mode)
    {
        DataVoContext context = OpenWithOrders(mode);
        var lines = new DataVoBatchLayout("OrderLines", ["Id", "Qty"]);
        var orders = new DataVoBatchLayout("Orders", ["Id", "Note"]);

        var batch = new DataVoWriteBatch();
        batch.Upsert(lines, 10, 99);
        batch.Delete(orders, 1, null);
        context.ApplyBatch(batch);

        Assert.Equal("0", Scalar(context, "SELECT COUNT(*) FROM OrderLines WHERE Id = 10"));
    }

    [Fact]
    public void CascadeChildTables_AreLockedWithTheBatch()
    {
        DataVoContext context = OpenWithOrders(StorageMode.InMemory);

        IReadOnlyCollection<string> locked = DataVo.Core.Batches.BatchIntegrity.TablesToLock(context.Engine, "B", ["Orders"]);

        Assert.Equal(["OrderLines", "Orders"], locked.Order(StringComparer.OrdinalIgnoreCase));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UniqueValues_CanBeSwappedFreedAndReused_InOneBatch(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "CREATE TABLE Users (Id INT PRIMARY KEY, Email VARCHAR(40) UNIQUE)");
        Ok(context, "INSERT INTO Users VALUES (1, 'a@x'), (2, 'b@x'), (3, 'c@x')");
        var users = new DataVoBatchLayout("Users", ["Id", "Email"]);

        var swap = new DataVoWriteBatch();
        swap.Upsert(users, 1, "b@x");
        swap.Upsert(users, 2, "a@x");
        context.ApplyBatch(swap);
        Assert.Equal("b@x", Scalar(context, "SELECT Email FROM Users WHERE Id = 1"));
        Assert.Equal("a@x", Scalar(context, "SELECT Email FROM Users WHERE Id = 2"));

        var reuse = new DataVoWriteBatch();
        reuse.Delete(users, 3, null);
        reuse.Upsert(users, 4, "c@x");      // freed by the delete
        reuse.Upsert(users, 1, "z@x");
        reuse.Upsert(users, 5, "b@x");      // freed by the update of row 1
        context.ApplyBatch(reuse);
        Assert.Equal("c@x", Scalar(context, "SELECT Email FROM Users WHERE Id = 4"));
        Assert.Equal("b@x", Scalar(context, "SELECT Email FROM Users WHERE Id = 5"));

        var clash = new DataVoWriteBatch();
        clash.Upsert(users, 6, "a@x");      // row 2 still holds it
        Assert.ThrowsAny<Exception>(() => context.ApplyBatch(clash));
        clash.Clear();
        clash.Upsert(users, 2, "q@x");
        clash.Upsert(users, 6, "q@x");      // taken by the update in the same batch
        Assert.ThrowsAny<Exception>(() => context.ApplyBatch(clash));
        Assert.Equal("a@x", Scalar(context, "SELECT Email FROM Users WHERE Id = 2"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void UpdatedReferences_SeeParentsTheBatchAddsOrRemoves(StorageMode mode)
    {
        DataVoContext context = OpenWithTeams(mode);

        var batch = new DataVoWriteBatch();
        batch.Upsert(MemberLayout, 10, 3);  // the parent comes later in the batch
        batch.Upsert(TeamLayout, 3, "Blue");
        context.ApplyBatch(batch);
        Assert.Equal("3", Scalar(context, "SELECT TeamId FROM Members WHERE Id = 10"));

        batch.Clear();
        batch.Upsert(MemberLayout, 20, 3);
        batch.Delete(MemberLayout, 10, null);
        batch.Delete(TeamLayout, 3, null);  // the new parent of member 20 goes away in the same batch
        Assert.ThrowsAny<Exception>(() => context.ApplyBatch(batch));
        Assert.Equal("2", Scalar(context, "SELECT TeamId FROM Members WHERE Id = 20"));
        Assert.Equal("1", Scalar(context, "SELECT COUNT(*) FROM Teams WHERE Id = 3"));
    }

    [Fact]
    public void Lsm_BatchIsDurableAcrossReopen()
    {
        string root = NewRoot();
        using (DataVoContext first = Create(StorageMode.Lsm, root))
        {
            Setup(first);
            var batch = new DataVoWriteBatch();
            batch.Upsert(Players, 1, "Ann", 100);
            batch.Upsert(Position, "feed", 7L);
            first.ApplyBatch(batch);
        }

        using DataVoContext reopened = Create(StorageMode.Lsm, root);
        Ok(reopened, "USE B");
        Assert.Equal(["1:Ann:100"], Rows(reopened));
        Assert.Equal("7", Scalar(reopened, "SELECT Lsn FROM ResumePosition WHERE Source = 'feed'"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void LargeBatch_ThenUpdates_StayIndexed(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        var batch = new DataVoWriteBatch();
        for (int i = 0; i < 20_000; i++)
        {
            batch.Upsert(Players, i, $"p{i}", i);
        }

        Assert.Equal(20_000, context.ApplyBatch(batch).Inserted);

        batch.Clear();
        for (int i = 0; i < 20_000; i += 2)
        {
            batch.Upsert(PlayerHp, i, -i);
        }

        // Id 0 keeps Hp 0 (-0 == 0), so it is reported unchanged.
        Assert.Equal(new DataVoBatchResult(0, 9_999, 0, 1), context.ApplyBatch(batch));
        Assert.Equal("-4", Scalar(context, "SELECT Hp FROM Players WHERE Id = 4"));
        Assert.Equal("5", Scalar(context, "SELECT Hp FROM Players WHERE Id = 5"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Update_ChangesExistingRows_AndReportsMissingOnes(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "INSERT INTO Players VALUES (1, 'Ann', 100)");

        var batch = new DataVoWriteBatch();
        batch.Update(PlayerHp, 1, 50);          // exists: updated
        batch.Update(PlayerHp, 2, 10);          // missing: reported, not created
        batch.Update(PlayerHp, 3, 1);
        batch.Upsert(Players, 3, "Cid", 2);     // an upsert in the fold may create the row
        batch.Delete(Players, 1, null, null);
        batch.Update(PlayerHp, 1, 99);          // after a delete: the row stays deleted

        Assert.Equal(new DataVoBatchResult(Inserted: 1, Updated: 0, Deleted: 1, Unchanged: 0, Missing: 1), context.ApplyBatch(batch));
        Assert.Equal(["3:Cid:2"], Rows(context));
    }

    [Fact]
    public void SessionOverload_WritesToThatSessionsDatabase()
    {
        DataVoContext context = Open(StorageMode.InMemory);
        Ok(context, "CREATE DATABASE Other");
        Ok(context, "USE Other");
        Ok(context, "CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Hp INT)");
        Guid importer = Guid.NewGuid();
        context.Engine.Sessions.Set(importer, "B");
        var batch = new DataVoWriteBatch();
        batch.Upsert(Players, 1, "Ann", 1);

        context.ApplyBatch(batch, importer);

        Assert.Equal("0", Scalar(context, "SELECT COUNT(*) FROM Players"));
        Ok(context, "USE B");
        Assert.Equal(["1:Ann:1"], Rows(context));
        Assert.Throws<InvalidOperationException>(() => context.ApplyBatch(batch, Guid.NewGuid()));
    }

    [Fact]
    public void DiskMode_IsNotSupported()
    {
        using DataVoContext context = Create(StorageMode.Disk, NewRoot());
        var batch = new DataVoWriteBatch();
        batch.Upsert(Players, 1, "Ann", 1);

        Assert.Throws<NotSupportedException>(() => context.ApplyBatch(batch));
    }

    [Fact]
    public void OpenTransaction_IsRejected()
    {
        DataVoContext context = Open(StorageMode.InMemory);
        Ok(context, "BEGIN TRANSACTION");
        var batch = new DataVoWriteBatch();
        batch.Upsert(Players, 1, "Ann", 1);

        Assert.Throws<InvalidOperationException>(() => context.ApplyBatch(batch));
        Ok(context, "ROLLBACK");
    }

    [Fact]
    public void BadLayouts_AreRejected()
    {
        DataVoContext context = Open(StorageMode.InMemory);
        Ok(context, "CREATE TABLE NoKey (A INT, B INT)");

        Assert.Throws<ArgumentException>(() => Apply(context, new DataVoBatchLayout("Missing", ["Id"]), 1));
        Assert.Throws<ArgumentException>(() => Apply(context, new DataVoBatchLayout("Players", ["Id", "Nope"]), 1, 2));
        Assert.Throws<ArgumentException>(() => Apply(context, new DataVoBatchLayout("Players", ["Hp"]), 1));
        Assert.Throws<ArgumentException>(() => Apply(context, Players, null, "x", 1));
        Assert.Throws<InvalidOperationException>(() => Apply(context, new DataVoBatchLayout("NoKey", ["A", "B"]), 1, 2));
        Assert.Throws<ArgumentException>(() => new DataVoWriteBatch().Upsert(Players, 1, "too few"));
    }

    private static void Apply(DataVoContext context, DataVoBatchLayout layout, params object?[] values)
    {
        var batch = new DataVoWriteBatch();
        batch.Upsert(layout, values);
        context.ApplyBatch(batch);
    }

    private DataVoContext Open(StorageMode mode)
    {
        DataVoContext context = Create(mode, mode == StorageMode.InMemory ? null : NewRoot());
        _contexts.Add(context);
        Setup(context);
        return context;
    }

    private static readonly DataVoBatchLayout TeamLayout = new("Teams", ["Id", "Name"]);
    private static readonly DataVoBatchLayout MemberLayout = new("Members", ["Id", "TeamId"]);

    /// <summary>Teams 1 and 2, with members 10 (team 1) and 20 (team 2); members restrict team deletes.</summary>
    private DataVoContext OpenWithTeams(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "CREATE TABLE Teams (Id INT PRIMARY KEY, Name VARCHAR(20))");
        Ok(context, "CREATE TABLE Members (Id INT PRIMARY KEY, TeamId INT REFERENCES Teams(Id))");
        Ok(context, "INSERT INTO Teams VALUES (1, 'Red'), (2, 'Green')");
        Ok(context, "INSERT INTO Members VALUES (10, 1), (20, 2)");
        return context;
    }

    /// <summary>Orders 1 and 2; lines cascade with their order; a refund restricts deleting line 21.</summary>
    private DataVoContext OpenWithOrders(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        Ok(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, Note VARCHAR(20))");
        Ok(context, "CREATE TABLE OrderLines (Id INT PRIMARY KEY, OrderId INT REFERENCES Orders(Id) ON DELETE CASCADE, Qty INT)");
        Ok(context, "CREATE TABLE Refunds (Id INT PRIMARY KEY, LineId INT REFERENCES OrderLines(Id))");
        Ok(context, "INSERT INTO Orders VALUES (1, 'a'), (2, 'b')");
        Ok(context, "INSERT INTO OrderLines VALUES (10, 1, 1), (11, 1, 2), (21, 2, 3)");
        Ok(context, "INSERT INTO Refunds VALUES (100, 21)");
        return context;
    }

    private static void Setup(DataVoContext context)
    {
        Ok(context, "CREATE DATABASE B");
        Ok(context, "USE B");
        Ok(context, "CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Hp INT)");
        Ok(context, "CREATE TABLE ResumePosition (Source VARCHAR(40) PRIMARY KEY, Lsn BIGINT NOT NULL)");
    }

    private static DataVoContext Create(StorageMode mode, string? root) =>
        new(new DataVoConfig { StorageMode = mode, DiskStoragePath = root ?? string.Empty, LsmStrictFsync = false });

    private string NewRoot()
    {
        string root = TestStorageDirectories.Create("apply_batch");
        _roots.Add(root);
        return root;
    }

    private static List<string> Rows(DataVoContext context) =>
        [.. context.Execute("SELECT Id, Name, Hp FROM Players ORDER BY Id")[^1].Data
            .Select(row => $"{row["Id"]}:{row["Name"]}:{row["Hp"]}")];

    private static string Scalar(DataVoContext context, string sql)
    {
        var result = context.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        return Convert.ToString(result.Data[0].Values.First(), System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static void Ok(DataVoContext context, string sql)
    {
        foreach (var result in context.Execute(sql))
        {
            Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        }
    }
}
