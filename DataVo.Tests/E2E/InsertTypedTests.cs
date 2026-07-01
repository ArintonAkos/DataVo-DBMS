using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class InsertTypedTests
{
    private static readonly ReactiveRowSchema PlayerSchema = new("Id", "Name", "Level");

    [Fact]
    public void InsertTyped_InsertsStoredValuesAndReturnsRowIdLikeBulkInsert()
    {
        using DataVoContext typed = CreateContext();
        typed.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");

        long typedRowId = typed.InsertTyped("Players", PlayerSchema,
            [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)]);

        using DataVoContext dict = CreateContext();
        dict.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        IReadOnlyList<long> dictRowIds = dict.BulkInsert("Players",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 7 }
        ]);

        Assert.Equal(dictRowIds.Single(), typedRowId);
        Dictionary<string, object?> typedRow = Select(typed, "SELECT Id, Name, Level FROM Players").Single();
        Dictionary<string, object?> dictRow = Select(dict, "SELECT Id, Name, Level FROM Players").Single();
        Assert.Equal(dictRow["Id"], typedRow["Id"]);
        Assert.Equal(dictRow["Name"], typedRow["Name"]);
        Assert.Equal(dictRow["Level"], typedRow["Level"]);
    }

    [Fact]
    public void InsertTyped_WhenColumnCountDoesNotMatchRowLength_Throws()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");

        ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            ctx.InsertTyped("Players", PlayerSchema, [CellValue.From(1), CellValue.From("Ada")]));

        Assert.Contains("schema", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InsertTyped_WhenCellTypeDoesNotExactlyMatchColumn_Throws()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() =>
            ctx.InsertTyped("Players", PlayerSchema,
                [CellValue.From(1L), CellValue.From("Ada"), CellValue.From(7)]));

        Assert.Contains("Id", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("dictionary", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InsertTyped_WhenSchemaIsNotFullCatalogOrder_Throws()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        var wrongOrder = new ReactiveRowSchema("Name", "Id", "Level");

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() =>
            ctx.InsertTyped("Players", wrongOrder,
                [CellValue.From("Ada"), CellValue.From(1), CellValue.From(7)]));

        Assert.Contains("catalog order", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InsertTyped_WhenVarcharWouldBeTruncated_Throws()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(3), Level INT)");

        NotSupportedException ex = Assert.Throws<NotSupportedException>(() =>
            ctx.InsertTyped("Players", PlayerSchema,
                [CellValue.From(1), CellValue.From("Ada Lovelace"), CellValue.From(7)]));

        Assert.Contains("Name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void InsertTyped_FloatInsert_MatchesBulkInsertReadBack()
    {
        var schema = new ReactiveRowSchema("Id", "Reading");

        using DataVoContext typed = CreateContext();
        typed.Execute("CREATE TABLE Metrics (Id INT PRIMARY KEY, Reading FLOAT)");
        typed.InsertTyped("Metrics", schema, [CellValue.From(1), CellValue.From(12.5d)]);

        using DataVoContext dict = CreateContext();
        dict.Execute("CREATE TABLE Metrics (Id INT PRIMARY KEY, Reading FLOAT)");
        dict.BulkInsert("Metrics",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Reading"] = 12.5d }
        ]);

        Dictionary<string, object?> typedRow = Select(typed, "SELECT Id, Reading FROM Metrics").Single();
        Dictionary<string, object?> dictRow = Select(dict, "SELECT Id, Reading FROM Metrics").Single();
        Assert.Equal(dictRow["Id"], typedRow["Id"]);
        Assert.Equal(dictRow["Reading"], typedRow["Reading"]);
    }

    [Fact]
    public void InsertTyped_DateInsert_MatchesBulkInsertReadBack()
    {
        var schema = new ReactiveRowSchema("Id", "ObservedOn");
        var observedOn = new DateOnly(2026, 6, 22);

        using DataVoContext typed = CreateContext();
        typed.Execute("CREATE TABLE Metrics (Id INT PRIMARY KEY, ObservedOn DATE)");
        typed.InsertTyped("Metrics", schema, [CellValue.From(1), CellValue.From(observedOn)]);

        using DataVoContext dict = CreateContext();
        dict.Execute("CREATE TABLE Metrics (Id INT PRIMARY KEY, ObservedOn DATE)");
        dict.BulkInsert("Metrics",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["ObservedOn"] = observedOn }
        ]);

        Dictionary<string, object?> typedRow = Select(typed, "SELECT Id, ObservedOn FROM Metrics").Single();
        Dictionary<string, object?> dictRow = Select(dict, "SELECT Id, ObservedOn FROM Metrics").Single();
        Assert.Equal(dictRow["Id"], typedRow["Id"]);
        Assert.Equal(dictRow["ObservedOn"], typedRow["ObservedOn"]);
    }

    [Fact]
    public void InsertTyped_VectorInsert_MatchesBulkInsertReadBack()
    {
        var schema = new ReactiveRowSchema("Id", "Emb", "Label");
        float[] vector = [1f, 0.25f, 0f];

        using DataVoContext typed = CreateContext();
        typed.Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(20))");
        typed.InsertTyped("Embeddings", schema, [CellValue.From(1), CellValue.From(vector), CellValue.From("typed")]);

        using DataVoContext dict = CreateContext();
        dict.Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(20))");
        dict.BulkInsert("Embeddings",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Emb"] = vector, ["Label"] = "typed" }
        ]);

        Dictionary<string, object?> typedRow = Select(typed, "SELECT Id, Emb, Label FROM Embeddings").Single();
        Dictionary<string, object?> dictRow = Select(dict, "SELECT Id, Emb, Label FROM Embeddings").Single();
        Assert.Equal(dictRow["Id"], typedRow["Id"]);
        Assert.Equal(dictRow["Label"], typedRow["Label"]);
        AssertVectorsEqual((float[])dictRow["Emb"]!, (float[])typedRow["Emb"]!);
    }

    [Fact]
    public void InsertTyped_VectorIndexExistingBeforeInsert_SearchesTypedRows()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(20))");
        ctx.Execute("CREATE INDEX idx_emb ON Embeddings (Emb) USING HNSW");

        ctx.InsertTyped("Embeddings", new ReactiveRowSchema("Id", "Emb", "Label"),
            [CellValue.From(1), CellValue.From(new[] { 1f, 0f, 0f }), CellValue.From("A")]);
        ctx.InsertTyped("Embeddings", new ReactiveRowSchema("Id", "Emb", "Label"),
            [CellValue.From(2), CellValue.From(new[] { 0f, 1f, 0f }), CellValue.From("B")]);

        List<Dictionary<string, object?>> nearest = ctx.SearchNearest("Embeddings", "idx_emb", [0.95f, 0.05f, 0f], topK: 1);

        Dictionary<string, object?> row = Assert.Single(nearest);
        Assert.Equal(1, row["Id"]);
        Assert.Equal("A", row["Label"]);
    }

    [Fact]
    public void InsertTyped_ConstraintMessagesMatchSqlInsert()
    {
        const string createParent = "CREATE TABLE Parents (Id INT PRIMARY KEY)";
        const string createChild = "CREATE TABLE Children (Id INT PRIMARY KEY, ParentId INT REFERENCES Parents(Id), Code VARCHAR(20) UNIQUE)";
        var childSchema = new ReactiveRowSchema("Id", "ParentId", "Code");

        using DataVoContext typed = CreateContext();
        typed.Execute(createParent);
        typed.Execute(createChild);
        typed.Execute("INSERT INTO Parents VALUES (1)");
        typed.InsertTyped("Children", childSchema, [CellValue.From(1), CellValue.From(1), CellValue.From("A")]);

        AssertTypedConstraintMatchesSql(
            typed,
            () => typed.InsertTyped("Children", childSchema, [CellValue.From(1), CellValue.From(1), CellValue.From("B")]),
            "Primary key violation in row 1!",
            createParent,
            createChild,
            "INSERT INTO Parents VALUES (1)",
            "INSERT INTO Children VALUES (1, 1, 'A')",
            "INSERT INTO Children VALUES (1, 1, 'B')");

        AssertTypedConstraintMatchesSql(
            typed,
            () => typed.InsertTyped("Children", childSchema, [CellValue.From(2), CellValue.From(1), CellValue.From("A")]),
            "Unique key violation in row 1!",
            createParent,
            createChild,
            "INSERT INTO Parents VALUES (1)",
            "INSERT INTO Children VALUES (1, 1, 'A')",
            "INSERT INTO Children VALUES (2, 1, 'A')");

        AssertTypedConstraintMatchesSql(
            typed,
            () => typed.InsertTyped("Children", childSchema, [CellValue.From(3), CellValue.From(99), CellValue.From("C")]),
            "Foreign key violation in row 1!",
            createParent,
            createChild,
            "INSERT INTO Parents VALUES (1)",
            "INSERT INTO Children VALUES (1, 1, 'A')",
            "INSERT INTO Children VALUES (3, 99, 'C')");
    }

    [Fact]
    public void InsertTyped_RecorderKeepsPublicDictionaryPayloadForTypedCells()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Metrics (Id INT PRIMARY KEY, Reading FLOAT, ObservedOn DATE, Emb VECTOR(3))");
        ctx.Changes.Enabled = true;
        var captured = new List<RowChange>();
        ctx.Changes.Captured += set => captured.AddRange(set.Changes);

        var schema = new ReactiveRowSchema("Id", "Reading", "ObservedOn", "Emb");
        var observedOn = new DateOnly(2026, 6, 22);
        ctx.InsertTyped("Metrics", schema,
            [CellValue.From(1), CellValue.From(12.5d), CellValue.From(observedOn), CellValue.From(new[] { 1f, 0f, 0f })]);

        RowChange change = Assert.Single(captured);
        Assert.NotNull(change.After);
        Assert.Equal(1, change.After!["Id"]);
        Assert.Equal(12.5d, change.After["Reading"]);
        Assert.Equal(observedOn, change.After["ObservedOn"]);
        AssertVectorsEqual([1f, 0f, 0f], (float[])change.After["Emb"]!);
        Assert.NotNull(change.TypedAfter);
    }

    [Fact]
    public void InsertTyped_PopulatesTypedAfter_AndBulkInsertLeavesTypedAfterNull()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        ctx.Changes.Enabled = true;
        var captured = new List<RowChange>();
        ctx.Changes.Captured += set => captured.AddRange(set.Changes);

        long rowId = ctx.InsertTyped("Players", PlayerSchema,
            [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)]);
        ctx.BulkInsert("Players",
        [
            new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Bob", ["Level"] = 3 }
        ]);

        RowChange typedChange = captured.Single(c => c.RowId == rowId);
        Assert.NotNull(typedChange.TypedAfter);
        Assert.Equal(1, typedChange.TypedAfter.Value.AsRowRef()[0].AsInt32());
        Assert.Equal("Ada", typedChange.TypedAfter.Value.AsRowRef()[1].AsString());
        Assert.Equal(7, typedChange.After!["Level"]);

        RowChange dictChange = captured.Single(c => Equals(c.After!["Id"], 2));
        Assert.Null(dictChange.TypedAfter);
        Assert.Equal("Bob", dictChange.After!["Name"]);
    }

    [Fact]
    public void InsertTypedDispatchLoop_AllocatesMateriallyLessThanBulkInsertDispatchLoop()
    {
        long typed = MeasureOrderInsertLoop(useTypedInsert: true, startId: 1);
        long bulk = MeasureOrderInsertLoop(useTypedInsert: false, startId: 1);

        Assert.True(typed < bulk, $"typed={typed} bulk={bulk}");
        Assert.True(typed <= bulk * 90 / 100, $"typed={typed} bulk={bulk}");
    }

    [Fact]
    public void InsertTyped_WhenExistingValidationRejectsSingleRow_Throws()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        ctx.InsertTyped("Players", PlayerSchema,
            [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)]);

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            ctx.InsertTyped("Players", PlayerSchema,
                [CellValue.From(1), CellValue.From("Duplicate"), CellValue.From(8)]));

        Assert.Equal("Primary key violation in row 1!", ex.Message);
    }

    [Fact]
    public void InsertTypedBatch_InsertsRowsAndRejectsDuplicatePrimaryKeyWithinBatch()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");

        IReadOnlyList<long> rowIds = ctx.InsertTypedBatch("Players", PlayerSchema,
        [
            [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)],
            [CellValue.From(2), CellValue.From("Bob"), CellValue.From(3)]
        ]);

        Assert.Equal([1L, 2L], rowIds);
        List<Dictionary<string, object?>> rows = Select(ctx, "SELECT Id, Name, Level FROM Players");
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, row => Equals(row["Id"], 1) && Equals(row["Name"], "Ada"));
        Assert.Contains(rows, row => Equals(row["Id"], 2) && Equals(row["Name"], "Bob"));

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            ctx.InsertTypedBatch("Players", PlayerSchema,
            [
                [CellValue.From(3), CellValue.From("Cara"), CellValue.From(4)],
                [CellValue.From(3), CellValue.From("Duplicate"), CellValue.From(5)]
            ]));

        Assert.Equal("Primary key violation in row 2!", ex.Message);
    }

    [Fact]
    public void InsertTypedBatch_RejectsDuplicatePrimaryKeyFromEarlierBatch()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        ctx.InsertTypedBatch("Players", PlayerSchema,
        [
            [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)],
            [CellValue.From(2), CellValue.From("Bob"), CellValue.From(3)]
        ]);

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            ctx.InsertTypedBatch("Players", PlayerSchema,
            [
                [CellValue.From(3), CellValue.From("Cara"), CellValue.From(4)],
                [CellValue.From(2), CellValue.From("Duplicate"), CellValue.From(5)]
            ]));

        Assert.Equal("Primary key violation in row 2!", ex.Message);
    }

    [Fact]
    public void InsertTypedBatch_CallerOwnedBuffers_InMemoryStorageIsIsolatedFromLaterMutation()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        CellValue[] buffer = [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)];

        ctx.InsertTypedBatch("Players", PlayerSchema, [buffer], callerOwnsRowBuffers: true);

        // Caller-owned mode lets bulk loaders reuse row buffers: mutating the buffer afterwards must
        // not reach into retained storage.
        buffer[1] = CellValue.From("Mutated");
        buffer[2] = CellValue.From(999);

        Dictionary<string, object?> row = Select(ctx, "SELECT Id, Name, Level FROM Players").Single();
        Assert.Equal("Ada", row["Name"]);
        Assert.Equal(7, row["Level"]);
    }

    [Fact]
    public void InsertTypedBatch_CallerOwnedBuffers_LsmStorageIsIsolatedFromLaterMutation()
    {
        string root = Path.Combine(Path.GetTempPath(), "datavo-typed-lsm-tests", Guid.NewGuid().ToString("N"));
        try
        {
            using DataVoContext ctx = new(new DataVoConfig
            {
                StorageMode = StorageMode.Lsm,
                DiskStoragePath = root,
                LsmStrictFsync = false,
            });
            string dbName = $"TypedInsertLsm_{Guid.NewGuid():N}";
            ctx.Execute($"CREATE DATABASE {dbName}");
            ctx.Execute($"USE {dbName}");
            ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
            CellValue[] buffer = [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)];

            ctx.InsertTypedBatch("Players", PlayerSchema, [buffer], callerOwnsRowBuffers: true);

            buffer[1] = CellValue.From("Mutated");
            buffer[2] = CellValue.From(999);

            Dictionary<string, object?> row = Select(ctx, "SELECT Id, Name, Level FROM Players").Single();
            Assert.Equal("Ada", row["Name"]);
            Assert.Equal(7, row["Level"]);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public void InsertTypedBatch_AutoCommitWithNoOpenTransactions_SkipsPerRowVersionMetadata()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");

        IReadOnlyList<long> rowIds = ctx.InsertTypedBatch("Players", PlayerSchema,
        [
            [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)],
            [CellValue.From(2), CellValue.From("Bob"), CellValue.From(3)]
        ]);

        // No snapshot can exist without an open transaction, and absent metadata already means
        // "always-visible base version" — bulk auto-commit ingest must not pay a per-row entry.
        string databaseName = ctx.Engine.Sessions.Get(ctx.SessionId)
            ?? throw new InvalidOperationException("Expected selected database.");
        foreach (long rowId in rowIds)
        {
            Assert.Null(ctx.Engine.VersionStorageManager.GetVersion(databaseName, "Players", rowId));
        }
    }

    [Fact]
    public void InsertTypedBatch_WhileAnotherSessionHoldsTransaction_RegistersPerRowVersionMetadata()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        Guid otherSession = Guid.NewGuid();
        ctx.Engine.TransactionManager.Begin(otherSession, ctx.Engine.TransactionIdAllocator);

        try
        {
            IReadOnlyList<long> rowIds = ctx.InsertTypedBatch("Players", PlayerSchema,
            [
                [CellValue.From(1), CellValue.From("Ada"), CellValue.From(7)]
            ]);

            string databaseName = ctx.Engine.Sessions.Get(ctx.SessionId)
                ?? throw new InvalidOperationException("Expected selected database.");
            Assert.NotNull(ctx.Engine.VersionStorageManager.GetVersion(databaseName, "Players", rowIds[0]));
        }
        finally
        {
            ctx.Engine.TransactionManager.Rollback(otherSession);
        }
    }

    [Fact]
    public void InsertTypedBatch_IntPrimaryKey_PopulatesIntegerFastLane()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");

        ctx.InsertTypedBatch("Players", PlayerSchema,
        [
            [CellValue.From(10), CellValue.From("Ada"), CellValue.From(7)],
            [CellValue.From(20), CellValue.From("Bob"), CellValue.From(3)]
        ]);

        string databaseName = ctx.Engine.Sessions.Get(ctx.SessionId)
            ?? throw new InvalidOperationException("Expected selected database.");
        Assert.True(ctx.Engine.IndexManager.TryLookupIntegerPrimaryKey(
            20,
            "_PK_Players",
            "Players",
            databaseName,
            out long rowId));
        Assert.Equal(2L, rowId);
    }

    [Fact]
    public void InsertTypedBatch_IntSecondaryIndex_PopulatesIntegerFastLane()
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, OrderId INT, Sku INT)");
        ctx.Execute("CREATE INDEX ix_Items_OrderId ON Items (OrderId)");

        ctx.InsertTypedBatch("Items", new ReactiveRowSchema("Id", "OrderId", "Sku"),
        [
            [CellValue.From(1), CellValue.From(7), CellValue.From(100)],
            [CellValue.From(2), CellValue.From(7), CellValue.From(101)],
            [CellValue.From(3), CellValue.From(8), CellValue.From(102)]
        ]);

        string databaseName = ctx.Engine.Sessions.Get(ctx.SessionId)
            ?? throw new InvalidOperationException("Expected selected database.");
        IReadOnlyList<long> rowIds = ctx.Engine.IndexManager.LookupIntegerIndex(
            7,
            "ix_Items_OrderId",
            "Items",
            databaseName);

        Assert.Equal([1L, 2L], rowIds);
    }

    private static long MeasureOrderInsertLoop(bool useTypedInsert, int startId)
    {
        using DataVoContext ctx = CreateContext();
        ctx.Execute("CREATE TABLE Accounts (Id INT PRIMARY KEY, IsVip BIT)");
        ctx.Execute("CREATE TABLE Markets (Id INT PRIMARY KEY, Category VARCHAR(20))");
        ctx.Execute("CREATE TABLE Orders (Id INT, AccountId INT, MarketId INT, Stake INT)");
        ctx.Execute("INSERT INTO Accounts VALUES (1, true)");
        ctx.Execute("INSERT INTO Markets VALUES (1, 'sports')");

        // Allocation-light sinks that prove the reactive callback actually reads delivered
        // rows (Category + TotalExposure) rather than being a no-op. No LINQ, no boxing, and
        // no per-dispatch allocation, so the GC measurement below stays undistorted.
        long categorySink = 0;
        decimal exposureSink = 0m;
        using IDisposable sub = ctx.SubscribeZeroAlloc("""
            SELECT m.Category, SUM(o.Stake) AS TotalExposure
            FROM Orders o
            JOIN Accounts a ON o.AccountId = a.Id
            JOIN Markets m ON o.MarketId = m.Id
            WHERE a.IsVip = true
            GROUP BY m.Category
            """, (in QueryChangeRef change) =>
        {
            // Read both added and updated rows: a category enters as Added the first time it
            // appears, then every later insert into that category arrives as Updated. The
            // measured loop below re-inserts into the existing "sports" category, so its deltas
            // are Updated — accumulating both kinds is what lets the assertion prove delivery
            // during the measured region, not just during warmup.
            RowSet added = change.Added;
            for (int i = 0; i < added.Count; i++)
            {
                RowRef row = added[i];
                string? category = row["Category"].AsString();
                categorySink += category?.Length ?? 0;
                exposureSink += row["TotalExposure"].AsDecimal();
            }

            RowSet updated = change.Updated;
            for (int i = 0; i < updated.Count; i++)
            {
                RowRef row = updated[i];
                string? category = row["Category"].AsString();
                categorySink += category?.Length ?? 0;
                exposureSink += row["TotalExposure"].AsDecimal();
            }
        });

        var schema = new ReactiveRowSchema("Id", "AccountId", "MarketId", "Stake");
        CellValue[] cells = new CellValue[4];

        for (int i = 0; i < 200; i++)
        {
            InsertOne(ctx, useTypedInsert, schema, cells, startId + i);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Discard warmup deliveries so the post-loop assertion strictly reflects rows the
        // reactive pipeline delivered during the measured loop (all of which arrive as Updated).
        categorySink = 0;
        exposureSink = 0m;

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 2_000; i++)
        {
            InsertOne(ctx, useTypedInsert, schema, cells, startId + 200 + i);
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // The reactive pipeline must have delivered real rows into the sinks; otherwise the
        // callback could silently be a no-op and the benchmark would measure nothing useful.
        // Captured after `allocated` so the assertion strings don't skew the measurement.
        Assert.True(exposureSink > 0m, $"exposureSink={exposureSink}");
        Assert.True(categorySink > 0, $"categorySink={categorySink}");

        return allocated;
    }

    private static void InsertOne(
        DataVoContext ctx,
        bool useTypedInsert,
        ReactiveRowSchema schema,
        CellValue[] cells,
        int id)
    {
        if (useTypedInsert)
        {
            cells[0] = CellValue.From(id);
            cells[1] = CellValue.From(1);
            cells[2] = CellValue.From(1);
            cells[3] = CellValue.From(25);
            ctx.InsertTyped("Orders", schema, cells);
        }
        else
        {
            ctx.BulkInsert("Orders",
            [
                new Dictionary<string, object?>
                {
                    ["Id"] = id,
                    ["AccountId"] = 1,
                    ["MarketId"] = 1,
                    ["Stake"] = 25
                }
            ]);
        }

        ctx.DispatchPendingNotifications();
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string dbName = $"TypedInsert_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {dbName}");
        context.Execute($"USE {dbName}");
        return context;
    }

    private static List<Dictionary<string, object?>> Select(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Single();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return result.Data;
    }

    private static void AssertTypedConstraintMatchesSql(
        DataVoContext typed,
        Action typedInsert,
        string expectedMessage,
        params string[] sqlStatements)
    {
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(typedInsert);
        Assert.Equal(expectedMessage, ex.Message);

        using DataVoContext sql = CreateContext();
        for (int i = 0; i < sqlStatements.Length - 1; i++)
        {
            sql.Execute(sqlStatements[i]);
        }

        QueryResult result = sql.Execute(sqlStatements[^1]).Single();
        Assert.Contains(expectedMessage, result.Messages);
    }

    private static void AssertVectorsEqual(float[] expected, float[] actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i], actual[i], precision: 6);
        }
    }
}
