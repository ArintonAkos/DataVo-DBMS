using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Tests.BrowserParity;

namespace DataVo.Tests.E2E;

/// <summary>
/// Regression guard: keys written through any insert path must be visible to a fresh engine opened
/// on the same directory. The in-memory key fast lanes are caches; the persisted scalar index must
/// still receive every key in persistent storage modes.
/// </summary>
[BrowserTranslateIgnore("Disk persistence across engine restart is outside browser SQL parity")]
public sealed class DiskIntegerPrimaryKeyPersistenceTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"datavo_pk_persist_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> PersistentModes => new() { StorageMode.Disk, StorageMode.Lsm };

    [Theory]
    [MemberData(nameof(PersistentModes))]
    public void SqlInsert_DuplicateIntegerPrimaryKey_IsRejected_AfterRestart(StorageMode mode)
    {
        using (var first = Open(mode))
        {
            CreateBoards(first);
            AssertOk(first.Execute("INSERT INTO Boards (Id, Name) VALUES (1, 'A')"));
        }

        using var second = Reopen(mode);
        AssertPrimaryKeyViolation(second.Execute("INSERT INTO Boards (Id, Name) VALUES (1, 'B')"));
        Assert.Single(second.Execute("SELECT * FROM Boards")[0].Data);
    }

    [Theory]
    [MemberData(nameof(PersistentModes))]
    public void SqlInsert_DuplicateOfPersistedKey_IsRejected_AfterNewKeyCreatesLane(StorageMode mode)
    {
        using (var first = Open(mode))
        {
            CreateBoards(first);
            AssertOk(first.Execute("INSERT INTO Boards (Id, Name) VALUES (1, 'A')"));
        }

        using var second = Reopen(mode);
        AssertOk(second.Execute("INSERT INTO Boards (Id, Name) VALUES (2, 'B')"));
        AssertPrimaryKeyViolation(second.Execute("INSERT INTO Boards (Id, Name) VALUES (1, 'Dup')"));
        Assert.Single(second.Execute("SELECT * FROM Boards WHERE Id = 1")[0].Data);
        Assert.Equal(2, second.Execute("SELECT * FROM Boards")[0].Data.Count);
    }

    [Theory]
    [MemberData(nameof(PersistentModes))]
    public void SqlInsert_KeyedReadAndForeignKey_Resolve_AfterRestart(StorageMode mode)
    {
        using (var first = Open(mode))
        {
            CreateBoards(first);
            AssertOk(first.Execute("CREATE TABLE Cards (Id INT PRIMARY KEY, BoardId INT REFERENCES Boards(Id), Title VARCHAR(40))"));
            AssertOk(first.Execute("INSERT INTO Boards (Id, Name) VALUES (1, 'Roadmap')"));
        }

        using var second = Reopen(mode);
        Assert.Single(second.Execute("SELECT * FROM Boards WHERE Id = 1")[0].Data);
        AssertOk(second.Execute("INSERT INTO Cards (Id, BoardId, Title) VALUES (10, 1, 'Phase A')"));
        Assert.Single(second.Execute("SELECT * FROM Cards")[0].Data);
    }

    [Theory]
    [MemberData(nameof(PersistentModes))]
    public void TypedInsert_IntegerPrimaryKey_IsVisible_AfterRestart(StorageMode mode)
    {
        var schema = new ReactiveRowSchema("Id", "Name");
        using (var first = Open(mode))
        {
            CreateBoards(first);
            first.InsertTyped("Boards", schema, [CellValue.From(7), CellValue.From("Typed")]);
        }

        using var second = Reopen(mode);
        Assert.Single(second.Execute("SELECT * FROM Boards WHERE Id = 7")[0].Data);
        AssertPrimaryKeyViolation(second.Execute("INSERT INTO Boards (Id, Name) VALUES (7, 'Dup')"));
    }

    [Theory]
    [MemberData(nameof(PersistentModes))]
    public void TypedInsert_DuplicateOfPersistedKey_IsRejected_AfterNewKeyCreatesLane(StorageMode mode)
    {
        var schema = new ReactiveRowSchema("Id", "Name");
        using (var first = Open(mode))
        {
            CreateBoards(first);
            first.InsertTyped("Boards", schema, [CellValue.From(1), CellValue.From("A")]);
        }

        using var second = Reopen(mode);
        second.InsertTyped("Boards", schema, [CellValue.From(2), CellValue.From("B")]);

        Assert.Throws<DataVo.Core.Exceptions.ConstraintViolationException>(() => second.InsertTyped("Boards", schema, [CellValue.From(1), CellValue.From("Dup")]));
        Assert.Throws<DataVo.Core.Exceptions.ConstraintViolationException>(() => second.InsertTypedBatch("Boards", schema, [[CellValue.From(1), CellValue.From("DupBatch")]]));
        Assert.Equal(2, second.Execute("SELECT * FROM Boards")[0].Data.Count);
    }

    [Theory]
    [MemberData(nameof(PersistentModes))]
    public void TypedInsert_GuidPrimaryKey_IsVisible_AfterRestart(StorageMode mode)
    {
        Guid id = Guid.NewGuid();
        var schema = new ReactiveRowSchema("Id", "Name");
        using (var first = Open(mode))
        {
            AssertOk(first.Execute("CREATE DATABASE PkDb"));
            AssertOk(first.Execute("USE PkDb"));
            AssertOk(first.Execute("CREATE TABLE Items (Id GUID PRIMARY KEY, Name VARCHAR(40))"));
            first.InsertTyped("Items", schema, [CellValue.From(id), CellValue.From("Typed")]);
        }

        using var second = Reopen(mode);
        Assert.Single(second.Execute($"SELECT * FROM Items WHERE Id = '{id:D}'")[0].Data);
    }

    [Theory]
    [MemberData(nameof(PersistentModes))]
    public void CompiledUpdate_RepointedPrimaryKey_IsVisible_AfterRestart(StorageMode mode)
    {
        using (var first = Open(mode))
        {
            AssertOk(first.Execute("CREATE DATABASE PkDb"));
            AssertOk(first.Execute("USE PkDb"));
            AssertOk(first.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)"));
            AssertOk(first.Execute("INSERT INTO Players VALUES (1, 'Ada', 5)"));

            // The zero-alloc compiled UPDATE moves the row out of place and re-points the key lane.
            Assert.Equal(1, GeneratedGameQueries.SetPlayerLevel(first, 1, 9));
        }

        using var second = Reopen(mode);
        QueryResult keyed = second.Execute("SELECT * FROM Players WHERE Id = 1")[0];
        Assert.Single(keyed.Data);
        Assert.Equal(9, Convert.ToInt32(keyed.Data[0]["Level"]));
        AssertPrimaryKeyViolation(second.Execute("INSERT INTO Players VALUES (1, 'Dup', 1)"));
    }

    [Theory]
    [MemberData(nameof(PersistentModes))]
    public void CompiledUpdate_OfPersistedKey_Applies_AfterNewKeyCreatesLane(StorageMode mode)
    {
        using (var first = Open(mode))
        {
            AssertOk(first.Execute("CREATE DATABASE PkDb"));
            AssertOk(first.Execute("USE PkDb"));
            AssertOk(first.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)"));
            AssertOk(first.Execute("INSERT INTO Players VALUES (1, 'Ada', 5)"));
        }

        using var second = Reopen(mode);
        AssertOk(second.Execute("INSERT INTO Players VALUES (2, 'Grace', 8)"));

        // A lane holding only key 2 must not make the update of key 1 a silent zero-row no-op.
        Assert.Equal(1, GeneratedGameQueries.SetPlayerLevel(second, 1, 9));
        Assert.Equal(9, Convert.ToInt32(second.Execute("SELECT * FROM Players WHERE Id = 1")[0].Data.Single()["Level"]));
    }

    [Theory]
    [MemberData(nameof(PersistentModes))]
    public void SqlUpdate_ChangedPrimaryKey_IsConsistent_AfterRestart(StorageMode mode)
    {
        using (var first = Open(mode))
        {
            CreateBoards(first);
            AssertOk(first.Execute("INSERT INTO Boards (Id, Name) VALUES (1, 'A')"));
            AssertOk(first.Execute("UPDATE Boards SET Id = 5 WHERE Id = 1"));
        }

        using var second = Reopen(mode);
        Assert.Empty(second.Execute("SELECT * FROM Boards WHERE Id = 1")[0].Data);
        Assert.Single(second.Execute("SELECT * FROM Boards WHERE Id = 5")[0].Data);
        AssertOk(second.Execute("INSERT INTO Boards (Id, Name) VALUES (1, 'Reused')"));
        AssertPrimaryKeyViolation(second.Execute("INSERT INTO Boards (Id, Name) VALUES (5, 'Dup')"));
    }

    [Fact]
    public void InMemorySnapshotRestore_RejectsDuplicateTypedKey()
    {
        var schema = new ReactiveRowSchema("Id", "Name");
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        CreateBoards(context);
        context.InsertTyped("Boards", schema, [CellValue.From(1), CellValue.From("A")]);

        var snapshot = context.CreateSnapshot();
        context.RestoreSnapshot(snapshot);

        context.InsertTyped("Boards", schema, [CellValue.From(2), CellValue.From("B")]);
        Assert.Throws<DataVo.Core.Exceptions.ConstraintViolationException>(() => context.InsertTyped("Boards", schema, [CellValue.From(1), CellValue.From("Dup")]));
        Assert.Equal(2, context.Execute("SELECT * FROM Boards")[0].Data.Count);
    }

    public void Dispose()
    {
        if (Directory.Exists(_path))
        {
            Directory.Delete(_path, recursive: true);
        }
    }

    private DataVoContext Open(StorageMode mode) =>
        new(new DataVoConfig { StorageMode = mode, DiskStoragePath = _path, LsmStrictFsync = false });

    private DataVoContext Reopen(StorageMode mode)
    {
        DataVoContext context = Open(mode);
        AssertOk(context.Execute("USE PkDb"));
        return context;
    }

    private static void CreateBoards(DataVoContext context)
    {
        AssertOk(context.Execute("CREATE DATABASE PkDb"));
        AssertOk(context.Execute("USE PkDb"));
        AssertOk(context.Execute("CREATE TABLE Boards (Id INT PRIMARY KEY, Name VARCHAR(40))"));
    }

    private static void AssertPrimaryKeyViolation(List<QueryResult> results)
    {
        QueryResult result = Assert.Single(results);
        Assert.True(result.IsError);
        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, result.ErrorCode);
    }

    private static void AssertOk(List<QueryResult> results)
    {
        QueryResult? error = results.FirstOrDefault(r => r.IsError);
        Assert.True(error is null, error is null ? null : string.Join("; ", error.Messages));
    }
}

