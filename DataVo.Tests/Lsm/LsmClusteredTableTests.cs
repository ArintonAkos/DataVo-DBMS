using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Clustered primary keys: an LSM table whose primary key is one INT or GUID column stores each row under
/// its key. A primary-key read is one lookup, no primary-key index is written, uniqueness is "a live row
/// has this key", and an UPDATE that changes the key moves the row.
/// </summary>
public sealed class LsmClusteredTableTests : IDisposable
{
    private readonly string _root = TestStorageDirectories.Create("lsm_clustered");

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Fact]
    public void IntegerKey_IsTheRowKey_AndNoPrimaryKeyIndexIsWritten()
    {
        using (DataVoContext db = Open())
        {
            CreateItems(db);
            Execute(db, "INSERT INTO Items (Id, Name, Code) VALUES (42, 'a', 1), (-7, 'b', 2), (0, 'c', 1)");

            LsmStorageEngine lsm = db.Engine.StorageContext.LsmEngine!;
            Assert.Equal((RowKey)(-7), Assert.Single(lsm.ReadAllRows("Shop", "Items"), row => row.RowId == (RowKey)(-7)).RowId);
            Assert.NotEmpty(lsm.ReadRow("Shop", "Items", 42)); // stored under its key: no lookup needed

            Assert.Equal([42], Ids(db, "SELECT Id FROM Items WHERE Id = 42"));
            Assert.Equal([-7, 0, 42], Ids(db, "SELECT Id FROM Items ORDER BY Id"));
            Assert.Equal([0, 42], Ids(db, "SELECT Id FROM Items WHERE Code = 1"));
        }

        Assert.False(Directory.Exists(Path.Combine(_root, "Shop", "Items", ".idx", "_PK_Items")));
        Assert.True(Directory.Exists(Path.Combine(_root, "Shop", "Items", ".idx", "ix_code")));

        using DataVoContext reopened = Open();
        Execute(reopened, "USE Shop");
        Assert.Equal([-7], Ids(reopened, "SELECT Id FROM Items WHERE Id = -7"));
        Assert.Equal([0, 42], Ids(reopened, "SELECT Id FROM Items WHERE Code = 1"));
    }

    [Fact]
    public void KeyIsStoredOnce_AndRowsThatStillHoldItReadTheSame()
    {
        using DataVoContext db = Open();
        CreateItems(db);
        Execute(db, "INSERT INTO Items (Id, Name, Code) VALUES (42, 'a', 1)");

        // As stored, the row holds its key cell as NULL: the row key already is the key.
        LsmStorageEngine lsm = db.Engine.StorageContext.LsmEngine!;
        var columns = db.Engine.Catalog.GetTableColumns("Items", "Shop");
        CellValue[] stored = DataVo.Core.StorageEngine.Serialization.RowSerializer.DeserializeCells(lsm.ReadRow("Shop", "Items", 42), columns);
        Assert.True(stored[0].IsNull);
        Assert.Equal("a", stored[1].AsString());

        // A row written with its key cell (before keys were stored once) reads unchanged.
        byte[] full = DataVo.Core.StorageEngine.Serialization.RowSerializer.SerializeCells(columns, [CellValue.From(43), CellValue.From("old"), CellValue.From(1)]);
        Assert.True(lsm.TryInsertRowWithKey("Shop", "Items", 43, full));

        Assert.Equal([42, 43], Ids(db, "SELECT Id FROM Items WHERE Code = 1 OR Id = 43"));
        Assert.Equal("old", db.Execute("SELECT Name FROM Items WHERE Id = 43")[0].Data.Single()["Name"]);
        Execute(db, "UPDATE Items SET Name = 'new' WHERE Id = 43");
        Assert.Equal("new", db.Execute("SELECT Name FROM Items WHERE Id = 43")[0].Data.Single()["Name"]);
    }

    [Fact]
    public void DuplicateKey_IsRejected_AndARejectedMultiRowInsertWritesNothing()
    {
        using DataVoContext db = Open();
        CreateItems(db);
        Execute(db, "INSERT INTO Items (Id, Name, Code) VALUES (1, 'a', 1)");

        QueryResult single = db.Execute("INSERT INTO Items (Id, Name, Code) VALUES (1, 'dup', 9)").Single();
        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, single.ErrorCode);

        QueryResult batch = db.Execute("INSERT INTO Items (Id, Name, Code) VALUES (2, 'b', 2), (1, 'dup', 9)").Single();
        Assert.True(batch.IsError);
        Assert.Equal([1], Ids(db, "SELECT Id FROM Items"));
        Assert.Empty(Ids(db, "SELECT Id FROM Items WHERE Code = 9"));
    }

    [Fact]
    public void Update_KeepsOrMovesTheRow_AndKeepsSecondaryIndexesPointingAtIt()
    {
        using (DataVoContext db = Open())
        {
            CreateItems(db);
            Execute(db, "INSERT INTO Items (Id, Name, Code) VALUES (1, 'a', 5), (2, 'b', 5), (3, 'c', 6)");

            Execute(db, "UPDATE Items SET Name = 'renamed' WHERE Id = 1");
            Execute(db, "UPDATE Items SET Id = 10 WHERE Id = 2"); // moves the row to key 10
            Execute(db, "UPDATE Items SET Code = Code + 1 WHERE Id = 3");

            Assert.Empty(Ids(db, "SELECT Id FROM Items WHERE Id = 2"));
            Assert.Equal([10], Ids(db, "SELECT Id FROM Items WHERE Id = 10"));
            Assert.Equal([1, 10], Ids(db, "SELECT Id FROM Items WHERE Code = 5"));
            Assert.Equal([3], Ids(db, "SELECT Id FROM Items WHERE Code = 7"));
            Assert.Equal("renamed", db.Execute("SELECT Name FROM Items WHERE Id = 1")[0].Data.Single()["Name"]);

            Assert.True(db.Execute("UPDATE Items SET Id = 1 WHERE Id = 10").Single().IsError); // key taken
            Assert.Equal([1, 3, 10], Ids(db, "SELECT Id FROM Items"));
        }

        using DataVoContext reopened = Open();
        Execute(reopened, "USE Shop");
        Assert.Equal([1, 3, 10], Ids(reopened, "SELECT Id FROM Items"));
        Assert.Equal([1, 10], Ids(reopened, "SELECT Id FROM Items WHERE Code = 5"));
    }

    [Fact]
    public void DeletedKey_CanBeInsertedAgain_AlsoInsideATransaction()
    {
        using DataVoContext db = Open();
        CreateItems(db);
        Execute(db, "INSERT INTO Items (Id, Name, Code) VALUES (5, 'a', 1)");
        Execute(db, "DELETE FROM Items WHERE Id = 5");
        Execute(db, "INSERT INTO Items (Id, Name, Code) VALUES (5, 'again', 2)");
        Execute(db, "UPDATE Items SET Code = 3 WHERE Id = 5");
        Assert.Equal([5], Ids(db, "SELECT Id FROM Items WHERE Code = 3"));

        Guid session = Guid.NewGuid();
        Execute(db, "USE Shop", session);
        Execute(db, "BEGIN", session);
        Execute(db, "DELETE FROM Items WHERE Id = 5", session);
        Execute(db, "COMMIT", session);

        // Inside a transaction, as committed by the one before it.
        Execute(db, "BEGIN", session);
        Execute(db, "INSERT INTO Items (Id, Name, Code) VALUES (5, 'third', 4)", session);
        Execute(db, "INSERT INTO Items (Id, Name, Code) VALUES (6, 'new', 4)", session);
        Execute(db, "COMMIT", session);
        Assert.Equal([5, 6], Ids(db, "SELECT Id FROM Items WHERE Code = 4"));

        Execute(db, "BEGIN", session);
        Execute(db, "INSERT INTO Items (Id, Name, Code) VALUES (7, 'rolled back', 4)", session);
        Execute(db, "ROLLBACK", session);
        Assert.Equal([5, 6], Ids(db, "SELECT Id FROM Items"));
    }

    [Fact]
    public void GuidKey_IsTheRowKey_InRfcOrder()
    {
        Guid early = Guid.Parse("018f2c1e-0000-7000-8000-000000000001");
        Guid late = Guid.Parse("018f2c1e-0001-7000-8000-000000000001");
        using (DataVoContext db = Open())
        {
            Execute(db, "CREATE DATABASE Shop");
            Execute(db, "USE Shop");
            Execute(db, "CREATE TABLE Orders (Id GUID PRIMARY KEY, Total INT)");
            Execute(db, $"INSERT INTO Orders (Id, Total) VALUES ('{late}', 2), ('{early}', 1)");

            LsmStorageEngine lsm = db.Engine.StorageContext.LsmEngine!;
            Assert.Equal([RowKey.FromGuid(early), RowKey.FromGuid(late)], lsm.ReadAllRows("Shop", "Orders").Select(row => row.RowId));

            QueryResult duplicate = db.Execute($"INSERT INTO Orders (Id, Total) VALUES ('{early}', 9)").Single();
            Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, duplicate.ErrorCode);
        }

        using DataVoContext reopened = Open();
        Execute(reopened, "USE Shop");
        Assert.Equal(2, Convert.ToInt32(reopened.Execute($"SELECT Total FROM Orders WHERE Id = '{late}'")[0].Data.Single()["Total"]));
    }

    [Fact]
    public void VectorIndex_OnAGuidKeyedTable_SearchesReopensAndForgetsDeletedRows()
    {
        // The vector index keys its nodes by row key: GUID keys work, and its snapshot survives a reopen.
        Guid[] ids = [.. Enumerable.Range(0, 40).Select(i => new Guid(i + 1, 0, 0, new byte[8]))];
        using (DataVoContext db = Open())
        {
            Execute(db, "CREATE DATABASE Shop");
            Execute(db, "USE Shop");
            Execute(db, "CREATE TABLE Docs (Id GUID PRIMARY KEY, Emb VECTOR(8))");
            Execute(db, "CREATE INDEX idx_emb ON Docs (Emb) USING HNSW");
            for (int i = 0; i < ids.Length; i++)
            {
                Execute(db, $"INSERT INTO Docs (Id, Emb) VALUES ('{ids[i]}', '{Literal(Vector(i))}')");
            }

            Assert.Equal(ids[7], NearestGuid(db, Vector(7)));
        }

        Assert.True(File.Exists(Path.Combine(_root, "Shop", "Docs", ".vec", "idx_emb.hnsw")));

        using DataVoContext reopened = Open();
        Execute(reopened, "USE Shop");
        Assert.Equal(ids[7], NearestGuid(reopened, Vector(7)));
        Execute(reopened, $"DELETE FROM Docs WHERE Id = '{ids[7]}'");
        Assert.NotEqual(ids[7], NearestGuid(reopened, Vector(7)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void DeletedRows_LeaveTheVectorIndex(bool clustered)
    {
        // Found while testing clustered vector tables: in LSM mode a DELETE never reached the vector graph,
        // so a top-1 search for a deleted row returned nothing instead of the next nearest row.
        using DataVoContext db = Open(clustered);
        Execute(db, "CREATE DATABASE Shop");
        Execute(db, "USE Shop");
        Execute(db, "CREATE TABLE Docs (Id INT PRIMARY KEY, Emb VECTOR(8))");
        Execute(db, "CREATE INDEX idx_emb ON Docs (Emb) USING HNSW");
        for (int i = 0; i < 40; i++)
        {
            Execute(db, $"INSERT INTO Docs (Id, Emb) VALUES ({i}, '{Literal(Vector(i))}')");
        }

        Execute(db, "DELETE FROM Docs WHERE Id = 7");
        Assert.Single(db.SearchNearest("Docs", "idx_emb", Vector(7), 1));
    }

    [Fact]
    public void TypedInserts_UseTheKeyToo()
    {
        using DataVoContext db = Open();
        CreateItems(db);
        var schema = new ReactiveRowSchema("Id", "Name", "Code");
        RowKey key = db.InsertTyped("Items", schema, [CellValue.From(77), CellValue.From("typed"), CellValue.From(3)]);

        Assert.Equal((RowKey)77, key);
        Assert.Equal([77], Ids(db, "SELECT Id FROM Items WHERE Code = 3"));
        Assert.ThrowsAny<Exception>(() => db.InsertTyped("Items", schema, [CellValue.From(77), CellValue.From("dup"), CellValue.From(3)]));
        Assert.Equal([77], Ids(db, "SELECT Id FROM Items"));
    }

    [Fact]
    public void TypedBatch_StoresRowsUnderTheirKey_AndADuplicateWritesNothing()
    {
        using DataVoContext db = Open();
        Execute(db, "CREATE DATABASE Shop");
        Execute(db, "USE Shop");
        Execute(db, "CREATE TABLE Orders (Id GUID PRIMARY KEY, Total INT)");
        var schema = new ReactiveRowSchema("Id", "Total");
        Guid[] ids = [.. Enumerable.Range(0, 5).Select(_ => Guid.NewGuid())];

        IReadOnlyList<RowKey> keys = db.InsertTypedBatch("Orders", schema, [.. ids.Select((id, i) => new[] { CellValue.From(id), CellValue.From(i) })]);
        Assert.Equal(ids.Select(RowKey.FromGuid), keys);

        // Stored without the key cell, read back with it.
        LsmStorageEngine lsm = db.Engine.StorageContext.LsmEngine!;
        var columns = db.Engine.Catalog.GetTableColumns("Orders", "Shop");
        Assert.True(DataVo.Core.StorageEngine.Serialization.RowSerializer.DeserializeCells(lsm.ReadRow("Shop", "Orders", keys[2]), columns)[0].IsNull);
        Assert.Equal(2, Convert.ToInt32(db.Execute($"SELECT Total FROM Orders WHERE Id = '{ids[2]}'")[0].Data.Single()["Total"]));

        Guid fresh = Guid.NewGuid();
        Assert.ThrowsAny<Exception>(() => db.InsertTypedBatch("Orders", schema, [[CellValue.From(fresh), CellValue.From(9)], [CellValue.From(fresh), CellValue.From(9)]]));
        Assert.ThrowsAny<Exception>(() => db.InsertTypedBatch("Orders", schema, [[CellValue.From(Guid.NewGuid()), CellValue.From(9)], [CellValue.From(ids[0]), CellValue.From(9)]]));
        Assert.Empty(db.Execute("SELECT Id FROM Orders WHERE Total = 9")[0].Data);
        Assert.Equal(5, db.Execute("SELECT Id FROM Orders")[0].Data.Count);
    }

    [Theory]
    [InlineData("CREATE TABLE T (Id VARCHAR(10) PRIMARY KEY, V INT)", "'a'")]
    [InlineData("CREATE TABLE T (A INT PRIMARY KEY, B INT PRIMARY KEY, V INT)", null)]
    [InlineData("CREATE TABLE T (Id INT, V INT)", null)]
    public void OtherKeys_KeepEngineAssignedRowIds(string createTable, string? key)
    {
        using DataVoContext db = Open();
        Execute(db, "CREATE DATABASE Shop");
        Execute(db, "USE Shop");
        Execute(db, createTable);
        string insert = createTable.Contains("(A INT", StringComparison.Ordinal)
            ? "INSERT INTO T (A, B, V) VALUES (1, 2, 3)"
            : key is null ? "INSERT INTO T (Id, V) VALUES (100, 3)" : $"INSERT INTO T (Id, V) VALUES ({key}, 3)";
        Execute(db, insert);

        Assert.False(db.Engine.Catalog.IsTableClustered("T", "Shop"));
        Assert.Equal((RowKey)1, db.Engine.StorageContext.LsmEngine!.ReadAllRows("Shop", "T").Single().RowId);
    }

    [Fact]
    public void TablesCreatedWithClusteringOff_StayUnclustered_AfterReopen()
    {
        using (DataVoContext db = Open(clustered: false))
        {
            CreateItems(db);
            Execute(db, "INSERT INTO Items (Id, Name, Code) VALUES (42, 'a', 1)");
        }

        using DataVoContext reopened = Open();
        Execute(reopened, "USE Shop");
        Assert.False(reopened.Engine.Catalog.IsTableClustered("Items", "Shop"));
        Execute(reopened, "INSERT INTO Items (Id, Name, Code) VALUES (43, 'b', 1)");
        Assert.Equal([42, 43], Ids(reopened, "SELECT Id FROM Items WHERE Code = 1"));
        Assert.Equal([(RowKey)1, (RowKey)2], reopened.Engine.StorageContext.LsmEngine!.ReadAllRows("Shop", "Items").Select(row => row.RowId));
        Assert.Equal(DataVoErrorCodes.PrimaryKeyViolation, reopened.Execute("INSERT INTO Items (Id, Name, Code) VALUES (42, 'dup', 1)").Single().ErrorCode);
    }

    private static Guid NearestGuid(DataVoContext db, float[] query) =>
        Guid.Parse(Convert.ToString(db.SearchNearest("Docs", "idx_emb", query, 1).Single()["Id"])!);

    // Distinct directions per seed, so the nearest neighbour of Vector(i) is row i.
    private static float[] Vector(int seed)
    {
        var random = new Random(seed);
        return [.. Enumerable.Range(0, 8).Select(_ => (float)((random.NextDouble() * 2) - 1))];
    }

    private static string Literal(float[] vector) =>
        "[" + string.Join(",", vector.Select(v => v.ToString("R", System.Globalization.CultureInfo.InvariantCulture))) + "]";

    private static void CreateItems(DataVoContext db)
    {
        Execute(db, "CREATE DATABASE Shop");
        Execute(db, "USE Shop");
        Execute(db, "CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(20), Code INT)");
        Execute(db, "CREATE INDEX ix_code ON Items (Code)");
        Assert.True(db.Engine.Catalog.IsTableClustered("Items", "Shop") == db.Engine.Config.ClusteredPrimaryKeys);
    }

    private DataVoContext Open(bool clustered = true) => new(new DataVoConfig
    {
        StorageMode = StorageMode.Lsm,
        DiskStoragePath = _root,
        LsmStrictFsync = false,
        ClusteredPrimaryKeys = clustered,
    });

    private static void Execute(DataVoContext db, string sql, Guid? session = null)
    {
        QueryResult result = (session is { } id ? db.Execute(sql, id) : db.Execute(sql)).Single();
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }

    private static List<int> Ids(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql).Single();
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        return [.. result.Data.Select(row => Convert.ToInt32(row["Id"])).Order()];
    }
}
