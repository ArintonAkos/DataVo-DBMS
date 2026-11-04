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

    // Lsm is intentionally absent: an Lsm context does not yet persist its catalog or scalar indexes
    // across restarts, so it cannot reopen a database at all. Add it here once that is supported.
    public static TheoryData<StorageMode> PersistentModes => new() { StorageMode.Disk };

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

    // A rejected duplicate is reported as a violation message with zero affected rows, not as IsError.
    private static void AssertPrimaryKeyViolation(List<QueryResult> results)
    {
        List<string> messages = results.SelectMany(r => r.Messages).ToList();
        Assert.Contains(messages, m => m.Contains("Primary key violation", StringComparison.Ordinal));
        Assert.Contains("Rows affected: 0", messages);
    }

    private static void AssertOk(List<QueryResult> results)
    {
        QueryResult? error = results.FirstOrDefault(r => r.IsError);
        Assert.True(error is null, error is null ? null : string.Join("; ", error.Messages));
    }
}

