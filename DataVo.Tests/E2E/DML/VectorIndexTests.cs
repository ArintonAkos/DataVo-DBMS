using DataVo.Core.StorageEngine.Config;
using System.Diagnostics;
using System.Globalization;
using DataVo.Tests.BrowserParity;

namespace DataVo.Tests.E2E.DML;

public abstract class VectorIndexTestsBase(DataVoConfig config, string testDbName) : SqlExecutionTestsBase(config, testDbName)
{
    [Fact]
    public void Insert_VectorColumn_RoundTripsAsFloatArray()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR)");
        Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (1, '[1,0,0]', 'A')");

        var result = ExecuteAndReturn("SELECT * FROM Embeddings");

        Assert.Single(result.Data);
        Assert.IsType<float[]>(result.Data[0]["Emb"]!);

        var vector = (float[])result.Data[0]["Emb"]!;
        Assert.Equal(3, vector.Length);
        Assert.Equal(1f, vector[0]);
        Assert.Equal(0f, vector[1]);
        Assert.Equal(0f, vector[2]);
    }

    [Fact]
    public void Select_VectorColumn_PublicResultArrayIsIndependentOfStoredState()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3))");
        Execute("INSERT INTO Embeddings (Id, Emb) VALUES (1, '[1,2,3]')");

        var first = ExecuteAndReturn("SELECT Emb FROM Embeddings WHERE Id = 1");
        Assert.False(first.IsError);
        float[] handedOut = Assert.IsType<float[]>(first.Data[0]["Emb"]!);

        // Mutating the array handed to the caller must never corrupt stored state at the public boundary.
        handedOut[0] = 999f;

        var second = ExecuteAndReturn("SELECT Emb FROM Embeddings WHERE Id = 1");
        Assert.False(second.IsError);
        Assert.Equal([1f, 2f, 3f], Assert.IsType<float[]>(second.Data[0]["Emb"]!));
    }

    [Fact]
    public void CreateIndex_UsingHnsw_SearchNearestReturnsClosestRow()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR)");
        Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (1, '[1,0,0]', 'A')");
        Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (2, '[0,1,0]', 'B')");
        var createIndexResult = ExecuteAndReturn("CREATE INDEX idx_emb ON Embeddings (Emb) USING HNSW");
        Assert.DoesNotContain(createIndexResult.Messages, message => message.Contains("Error", StringComparison.OrdinalIgnoreCase));

        List<long> rowIds = Engine.IndexManager.SearchVector([0.95f, 0.05f, 0f], 1, "idx_emb", "Embeddings", TestDb);

        Assert.Single(rowIds);
        Assert.True(rowIds[0] >= 0);
    }

    [Fact]
    public void CreateIndex_UsingFlat_SearchNearestReturnsClosestRow()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR)");
        Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (1, '[1,0,0]', 'A')");
        Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (2, '[0,1,0]', 'B')");
        var createIndexResult = ExecuteAndReturn("CREATE INDEX idx_flat_emb ON Embeddings (Emb) USING FLAT");
        Assert.DoesNotContain(createIndexResult.Messages, message => message.Contains("Error", StringComparison.OrdinalIgnoreCase));

        List<long> rowIds = Engine.IndexManager.SearchVector([0.95f, 0.05f, 0f], 1, "idx_flat_emb", "Embeddings", TestDb, indexType: "FLAT");

        Assert.Single(rowIds);
        Assert.True(rowIds[0] >= 0);
    }

    [Fact]
    public void HnswIndex_NewInsert_IsSearchable()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR)");
        Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (1, '[1,0,0]', 'A')");
        var createIndexResult = ExecuteAndReturn("CREATE INDEX idx_emb ON Embeddings (Emb) USING HNSW");
        Assert.DoesNotContain(createIndexResult.Messages, message => message.Contains("Error", StringComparison.OrdinalIgnoreCase));

        List<long> beforeInsert = Engine.IndexManager.SearchVector([0.1f, 0.9f, 0f], 1, "idx_emb", "Embeddings", TestDb);
        Assert.Single(beforeInsert);

        Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (2, '[0,1,0]', 'B')");

        List<long> rowIds = Engine.IndexManager.SearchVector([0.1f, 0.9f, 0f], 1, "idx_emb", "Embeddings", TestDb);

        Assert.Single(rowIds);
        Assert.NotEqual(beforeInsert[0], rowIds[0]);
    }

    [Fact]
    public void Select_UsingCosineOperator_OrderByLimit_ReturnsNearestFirst()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR)");
        Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (1, '[1,0,0]', 'A')");
        Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (2, '[0,1,0]', 'B')");
        ExecuteAndReturn("CREATE INDEX idx_emb ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT Id, Label, Emb <=> '[0.9,0.1,0]' AS rank
            FROM Embeddings
            ORDER BY rank ASC
            LIMIT 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(1, result.Data[0]["Id"]);
    }

    [Fact]
    public void Select_NearestThenJoinPattern_WorksWithCte()
    {
        Execute("CREATE TABLE p_embeddings (product_id INT PRIMARY KEY, Emb VECTOR(3))");
        Execute("CREATE TABLE products (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO p_embeddings (product_id, Emb) VALUES (1, '[1,0,0]')");
        Execute("INSERT INTO p_embeddings (product_id, Emb) VALUES (2, '[0,1,0]')");
        Execute("INSERT INTO products (Id, Name) VALUES (1, 'Chair')");
        Execute("INSERT INTO products (Id, Name) VALUES (2, 'Table')");
        ExecuteAndReturn("CREATE INDEX idx_p_emb ON p_embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            WITH nn AS (
                SELECT product_id, Emb <=> '[0.95,0.05,0]' AS rank
                FROM p_embeddings
                ORDER BY rank ASC
                LIMIT 1
            )
            SELECT p.Id, p.Name
            FROM nn
            JOIN products p ON p.Id = nn.product_id");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("Chair", result.Data[0]["p.Name"]);
    }

    [Fact]
    public void Select_NearestJoin_AutomaticPlannerPath_WorksWithoutCte()
    {
        Execute("CREATE TABLE p_embeddings (product_id INT PRIMARY KEY, Emb VECTOR(3))");
        Execute("CREATE TABLE products (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO p_embeddings (product_id, Emb) VALUES (1, '[1,0,0]')");
        Execute("INSERT INTO p_embeddings (product_id, Emb) VALUES (2, '[0,1,0]')");
        Execute("INSERT INTO products (Id, Name) VALUES (1, 'Chair')");
        Execute("INSERT INTO products (Id, Name) VALUES (2, 'Table')");
        ExecuteAndReturn("CREATE INDEX idx_p_emb ON p_embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT p.Name, p.Id, a.Emb <=> '[0.95,0.05,0]' AS rank
            FROM p_embeddings a
            JOIN products p ON a.product_id = p.Id
            ORDER BY rank ASC
            LIMIT 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("Chair", result.Data[0]["p.Name"]);
    }

    [Fact]
    public void Select_VectorOperator_OnNonVectorColumn_ReturnsError()
    {
        Execute("CREATE TABLE Products (Id INT PRIMARY KEY, Name VARCHAR)");
        Execute("INSERT INTO Products (Id, Name) VALUES (1, 'A')");

        var result = ExecuteAndReturn(@"
            SELECT Id, Name <=> '[1,0,0]' AS rank
            FROM Products
            ORDER BY rank ASC
            LIMIT 1");

        Assert.True(result.IsError);
        Assert.Contains(result.Messages, message => message.Contains("requires left VECTOR column", StringComparison.OrdinalIgnoreCase)
                                                || message.Contains("can only be used with VECTOR columns", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Select_NearestWithSimpleWhere_FilteredBeforeJoin()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
        Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (1, '[1,0,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (2, '[0,1,0]', 'inactive')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (3, '[0.9,0.1,0]', 'active')");
        Execute("INSERT INTO Items (Id, Name) VALUES (1, 'Item1')");
        Execute("INSERT INTO Items (Id, Name) VALUES (3, 'Item3')");

        ExecuteAndReturn("CREATE INDEX idx_vec ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT i.Name, e.Id, e.Emb <=> '[0.95,0.05,0]' AS distance
            FROM Embeddings e
            WHERE e.Status = 'active'
            JOIN Items i ON e.Id = i.Id
            ORDER BY distance ASC
            LIMIT 2");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        // Should only return 'active' embeddings
        Assert.NotEmpty(result.Data);
        Assert.All(result.Data, row =>
            Assert.Contains("Item", (string)row["i.Name"]));
    }

    [Fact]
    public void Select_NearestWithMultipleAndPredicates_AllApplied()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Category VARCHAR, Score INT)");
        Execute("CREATE TABLE Metadata (Id INT PRIMARY KEY, Title VARCHAR)");

        Execute("INSERT INTO Embeddings (Id, Emb, Category, Score) VALUES (1, '[1,0,0]', 'tech', 95)");
        Execute("INSERT INTO Embeddings (Id, Emb, Category, Score) VALUES (2, '[0,1,0]', 'tech', 50)");
        Execute("INSERT INTO Embeddings (Id, Emb, Category, Score) VALUES (3, '[0.92,0.08,0]', 'news', 95)");
        Execute("INSERT INTO Embeddings (Id, Emb, Category, Score) VALUES (4, '[0.91,0.09,0]', 'tech', 95)");
        Execute("INSERT INTO Metadata (Id, Title) VALUES (1, 'First')");
        Execute("INSERT INTO Metadata (Id, Title) VALUES (4, 'Fourth')");

        ExecuteAndReturn("CREATE INDEX idx_vec ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT m.Title, e.Id, e.Emb <=> '[0.95,0.05,0]' AS distance
            FROM Embeddings e
            WHERE e.Category = 'tech' AND e.Score >= 95
            JOIN Metadata m ON e.Id = m.Id
            ORDER BY distance ASC
            LIMIT 5");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        // Should only return 'tech' embeddings with score >= 95
        Assert.NotEmpty(result.Data);
        Assert.All(result.Data, row =>
        {
            Assert.True((int)row["e.Id"] == 1 || (int)row["e.Id"] == 4);
        });
    }

    [Fact]
    public void Select_NearestWithIsNullPredicate_FiltersNullRows()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), DeletedAt VARCHAR)");
        Execute("CREATE TABLE Refs (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Embeddings (Id, Emb, DeletedAt) VALUES (1, '[1,0,0]', NULL)");
        Execute("INSERT INTO Embeddings (Id, Emb, DeletedAt) VALUES (2, '[0,1,0]', '2026-01-01')");
        Execute("INSERT INTO Embeddings (Id, Emb, DeletedAt) VALUES (3, '[0.9,0.1,0]', NULL)");
        Execute("INSERT INTO Refs (Id, Name) VALUES (1, 'Ref1')");
        Execute("INSERT INTO Refs (Id, Name) VALUES (3, 'Ref3')");

        ExecuteAndReturn("CREATE INDEX idx_vec ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT r.Name, e.Id, e.Emb <=> '[0.95,0.05,0]' AS distance
            FROM Embeddings e
            WHERE e.DeletedAt IS NULL
            JOIN Refs r ON e.Id = r.Id
            ORDER BY distance ASC
            LIMIT 5");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        // Should only return embeddings where deleted_at IS NULL
        Assert.NotEmpty(result.Data);
        Assert.All(result.Data, row =>
            Assert.NotNull(row["e.Id"]));
    }

    [Fact]
    public void Select_NearestWithUnsupportedOrPredicate_FallsBackToNormalExecution()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
        Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (1, '[1,0,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (2, '[0,1,0]', 'pending')");
        Execute("INSERT INTO Items (Id, Name) VALUES (1, 'Item1')");
        Execute("INSERT INTO Items (Id, Name) VALUES (2, 'Item2')");

        ExecuteAndReturn("CREATE INDEX idx_vec ON Embeddings (Emb) USING HNSW");

        // OR predicates should fall back to normal execution (not use HNSW fast path)
        var result = ExecuteAndReturn(@"
            SELECT i.Name, e.Id
            FROM Embeddings e
            WHERE e.Status = 'active' OR e.Status = 'pending'
            JOIN Items i ON e.Id = i.Id
            ORDER BY e.Id ASC
            LIMIT 5");

        // Should complete without error (fallback to normal execution)
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.NotEmpty(result.Data);
    }

    [Fact]
    public void Select_VectorDistanceWherePredicate_UsesPrefilterAndReturnsMatchingRows()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (1, '[1,0,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (2, '[0,1,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (3, '[0.9,0.1,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (4, '[0.8,0.2,0]', 'inactive')");
        ExecuteAndReturn("CREATE INDEX idx_vec_where ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT Id
            FROM Embeddings
            WHERE Emb <=> '[0.95,0.05,0]' < 0.2 AND Status = 'active'
            ORDER BY Id ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal(1, result.Data[0]["Id"]);
        Assert.Equal(3, result.Data[1]["Id"]);
    }

    [Fact]
    public void Select_VectorDistanceWherePredicate_WithJoin_DoesNotFailAndReturnsMatches()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
        Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (1, '[1,0,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (2, '[0,1,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (3, '[0.9,0.1,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (4, '[0.8,0.2,0]', 'inactive')");

        Execute("INSERT INTO Items (Id, Name) VALUES (1, 'Item1')");
        Execute("INSERT INTO Items (Id, Name) VALUES (3, 'Item3')");
        Execute("INSERT INTO Items (Id, Name) VALUES (4, 'Item4')");

        ExecuteAndReturn("CREATE INDEX idx_vec_where_join ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT e.Id, i.Name
            FROM Embeddings e
            WHERE e.Emb <=> '[0.95,0.05,0]' < 0.2 AND e.Status = 'active'
            JOIN Items i ON e.Id = i.Id
            ORDER BY e.Id ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal(1, result.Data[0]["e.Id"]);
        Assert.Equal("Item1", result.Data[0]["i.Name"]);
        Assert.Equal(3, result.Data[1]["e.Id"]);
        Assert.Equal("Item3", result.Data[1]["i.Name"]);
    }

    [Fact]
    public void Select_VectorDistanceWhereThreshold_StringLiteral_CurrentCultureFallback_NoJoin()
    {
        var originalCulture = CultureInfo.CurrentCulture;
        var originalUiCulture = CultureInfo.CurrentUICulture;

        try
        {
            var de = CultureInfo.GetCultureInfo("de-DE");
            CultureInfo.CurrentCulture = de;
            CultureInfo.CurrentUICulture = de;

            Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
            Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (1, '[1,0,0]', 'active')");
            Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (2, '[0,1,0]', 'active')");
            Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (3, '[0.9,0.1,0]', 'active')");
            ExecuteAndReturn("CREATE INDEX idx_vec_locale_nojoin ON Embeddings (Emb) USING HNSW");

            // String threshold with comma decimal separator should parse under CurrentCulture fallback.
            var result = ExecuteAndReturn(@"
                SELECT Id
                FROM Embeddings
                WHERE Emb <=> '[0.95,0.05,0]' < '0,2'
                ORDER BY Id ASC");

            Assert.False(result.IsError, string.Join(" | ", result.Messages));
            Assert.Equal(2, result.Data.Count);
            Assert.Equal(1, result.Data[0]["Id"]);
            Assert.Equal(3, result.Data[1]["Id"]);
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
            CultureInfo.CurrentUICulture = originalUiCulture;
        }
    }

    [Fact]
    public void Select_VectorDistanceWhereThreshold_StringLiteral_CurrentCultureFallback_WithJoin()
    {
        var originalCulture = CultureInfo.CurrentCulture;
        var originalUiCulture = CultureInfo.CurrentUICulture;

        try
        {
            var de = CultureInfo.GetCultureInfo("de-DE");
            CultureInfo.CurrentCulture = de;
            CultureInfo.CurrentUICulture = de;

            Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
            Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR)");
            Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (1, '[1,0,0]', 'active')");
            Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (2, '[0,1,0]', 'active')");
            Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (3, '[0.9,0.1,0]', 'active')");
            Execute("INSERT INTO Items (Id, Name) VALUES (1, 'Item1')");
            Execute("INSERT INTO Items (Id, Name) VALUES (2, 'Item2')");
            Execute("INSERT INTO Items (Id, Name) VALUES (3, 'Item3')");
            ExecuteAndReturn("CREATE INDEX idx_vec_locale_join ON Embeddings (Emb) USING HNSW");

            var result = ExecuteAndReturn(@"
                SELECT i.Name, e.Id
                FROM Embeddings e
                WHERE e.Emb <=> '[0.95,0.05,0]' < '0,2'
                JOIN Items i ON e.Id = i.Id
                ORDER BY e.Id ASC");

            Assert.False(result.IsError, string.Join(" | ", result.Messages));
            Assert.Equal(2, result.Data.Count);
            Assert.Equal(1, result.Data[0]["e.Id"]);
            Assert.Equal(3, result.Data[1]["e.Id"]);
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
            CultureInfo.CurrentUICulture = originalUiCulture;
        }
    }

    [Fact]
    public void Select_VectorDistanceWherePredicate_ReversedComparison_Works()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (1, '[1,0,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (2, '[0,1,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (3, '[0.9,0.1,0]', 'active')");
        ExecuteAndReturn("CREATE INDEX idx_vec_reversed ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT Id
            FROM Embeddings
            WHERE 0.2 > Emb <=> '[0.95,0.05,0]'
            ORDER BY Id ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal(1, result.Data[0]["Id"]);
        Assert.Equal(3, result.Data[1]["Id"]);
    }

    [Fact]
    public void Select_VectorDistanceWherePredicate_GreaterThan_Works()
    {
        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (1, '[1,0,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (2, '[0,1,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (3, '[0.9,0.1,0]', 'active')");
        ExecuteAndReturn("CREATE INDEX idx_vec_gt ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT Id
            FROM Embeddings
            WHERE Emb <=> '[0.95,0.05,0]' > 0.6
            ORDER BY Id ASC");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal(2, result.Data[0]["Id"]);
    }

    [Fact]
    [BrowserTranslateNeedsSpecificCode("Relies on Engine.Config vector fast-path expansion knobs not expressible via SQL-only browser scenarios")]
    public void Select_VectorDistanceWherePredicate_WithLimit_ExpandsCandidatesWhenPostFilterUnderfills()
    {
        Engine.Config.VectorPredicateFastPathMinRows = 1;
        Engine.Config.VectorPredicateFastPathMaxExpansionPasses = 8;
        Engine.Config.VectorPredicateFastPathExpansionFactor = 2;

        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");

        for (int id = 1; id <= 130; id++)
        {
            Execute($"INSERT INTO Embeddings (Id, Emb, Status) VALUES ({id}, '[1,0,0]', 'inactive')");
        }

        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (131, '[1,0,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (132, '[1,0,0]', 'active')");

        ExecuteAndReturn("CREATE INDEX idx_vec_expand ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT Id
            FROM Embeddings
            WHERE Emb <=> '[1,0,0]' < 0.1 AND Status = 'active'
            ORDER BY Id ASC
            LIMIT 2");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        var ids = result.Data
            .Select(row => Convert.ToInt32(row["Id"]))
            .OrderBy(id => id)
            .ToArray();

        Assert.Equal([131, 132], ids);
    }

    [Fact]
    public void Select_VectorDistanceWherePredicate_WithLimit_NoExpansionPasses_CanUnderfillAfterPostFilter()
    {
        Engine.Config.VectorPredicateFastPathMinRows = 1;
        Engine.Config.VectorPredicateFastPathMaxExpansionPasses = 0;
        Engine.Config.VectorPredicateFastPathExpansionFactor = 2;

        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");

        for (int id = 1; id <= 130; id++)
        {
            Execute($"INSERT INTO Embeddings (Id, Emb, Status) VALUES ({id}, '[1,0,0]', 'inactive')");
        }

        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (131, '[0.95,0.05,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (132, '[0.95,0.05,0]', 'active')");

        ExecuteAndReturn("CREATE INDEX idx_vec_no_expand ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT Id
            FROM Embeddings
            WHERE Emb <=> '[1,0,0]' < 0.1 AND Status = 'active'
            ORDER BY Id ASC
            LIMIT 2");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Empty(result.Data);
    }

    [Fact]
    [BrowserTranslateNeedsSpecificCode("Relies on Engine.Config vector fast-path expansion knobs not expressible via SQL-only browser scenarios")]
    public void Select_VectorOrderByWithWhereLimit_ExpandsCandidatesToSatisfyFilteredLimit()
    {
        Engine.Config.VectorPredicateFastPathMaxExpansionPasses = 8;
        Engine.Config.VectorPredicateFastPathExpansionFactor = 2;

        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");

        for (int id = 1; id <= 130; id++)
        {
            Execute($"INSERT INTO Embeddings (Id, Emb, Status) VALUES ({id}, '[1,0,0]', 'inactive')");
        }

        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (131, '[0.95,0.05,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (132, '[0.95,0.05,0]', 'active')");

        ExecuteAndReturn("CREATE INDEX idx_vec_order_expand ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT Id, Emb <=> '[1,0,0]' AS rank
            FROM Embeddings
            WHERE Status = 'active'
            ORDER BY rank ASC
            LIMIT 2");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        var ids = result.Data
            .Select(row => Convert.ToInt32(row["Id"]))
            .OrderBy(id => id)
            .ToArray();

        Assert.Equal([131, 132], ids);
    }

    [Fact]
    public void Select_VectorOrderByWithWhereLimit_NoExpansionCanUnderfill()
    {
        Engine.Config.VectorPredicateFastPathMaxExpansionPasses = 0;
        Engine.Config.VectorPredicateFastPathExpansionFactor = 2;

        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");

        for (int id = 1; id <= 130; id++)
        {
            Execute($"INSERT INTO Embeddings (Id, Emb, Status) VALUES ({id}, '[1,0,0]', 'inactive')");
        }

        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (131, '[0.95,0.05,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (132, '[0.95,0.05,0]', 'active')");

        ExecuteAndReturn("CREATE INDEX idx_vec_order_no_expand ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT Id, Emb <=> '[1,0,0]' AS rank
            FROM Embeddings
            WHERE Status = 'active'
            ORDER BY rank ASC
            LIMIT 2");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Empty(result.Data);
    }

    [Fact]
    public void Select_VectorOrderByMixedPredicate_HybridRouteAcceptBucket_Increments()
    {
        Engine.Config.EnableHybridRoutingTelemetryCounters = true;
        Engine.Config.ResetHybridRoutingCounters();

        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (1, '[1,0,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (2, '[0.9,0.1,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (3, '[0,1,0]', 'inactive')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (4, '[0,0,1]', 'inactive')");
        ExecuteAndReturn("CREATE INDEX idx_vec_hybrid_accept ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT Id, Emb <=> '[1,0,0]' AS rank
            FROM Embeddings
            WHERE Status = 'active'
            ORDER BY rank ASC
            LIMIT 2");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal(1, Engine.Config.GetHybridRoutingCounter("hybrid.orderby.accept"));
        Assert.Equal(0, Engine.Config.GetHybridRoutingCounter("hybrid.orderby.reject.topk_ge_total_rows"));
    }

    [Fact]
    public void Select_VectorOrderByMixedPredicate_HybridRouteRejectBucket_Increments_WhenTopKHitsTotalRows()
    {
        Engine.Config.EnableHybridRoutingTelemetryCounters = true;
        Engine.Config.ResetHybridRoutingCounters();

        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (1, '[1,0,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (2, '[0.9,0.1,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (3, '[0,1,0]', 'inactive')");
        ExecuteAndReturn("CREATE INDEX idx_vec_hybrid_reject ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT Id, Emb <=> '[1,0,0]' AS rank
            FROM Embeddings
            WHERE Status = 'active'
            ORDER BY rank ASC
            LIMIT 3");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal(1, Engine.Config.GetHybridRoutingCounter("hybrid.orderby.reject.topk_ge_total_rows"));
    }

    [Fact]
    [BrowserTranslateNeedsSpecificCode("Relies on Engine.Config adaptive hybrid routing knobs not expressible via SQL-only browser scenarios")]
    public void Select_VectorOrderByMixedPredicate_HybridInitialTopKAdaptiveBucket_Increments()
    {
        Engine.Config.EnableHybridRoutingTelemetryCounters = true;
        Engine.Config.ResetHybridRoutingCounters();
        Engine.Config.VectorPredicateFastPathMaxExpansionPasses = 8;
        Engine.Config.VectorPredicateFastPathExpansionFactor = 2;

        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");

        for (int id = 1; id <= 130; id++)
        {
            Execute($"INSERT INTO Embeddings (Id, Emb, Status) VALUES ({id}, '[1,0,0]', 'inactive')");
        }

        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (131, '[0.95,0.05,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (132, '[0.95,0.05,0]', 'active')");
        ExecuteAndReturn("CREATE INDEX idx_vec_hybrid_adaptive ON Embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT Id, Emb <=> '[1,0,0]' AS rank
            FROM Embeddings
            WHERE Status = 'active'
            ORDER BY rank ASC
            LIMIT 2");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(2, result.Data.Count);
        Assert.Equal(1, Engine.Config.GetHybridRoutingCounter("hybrid.orderby.initial_topk.adaptive"));
    }

    [Fact]
    [BrowserTranslateNeedsSpecificCode("Relies on Engine.Config telemetry snapshot controls not expressible via SQL-only browser scenarios")]
    public void Select_HybridRoutingTelemetry_AggregatesPerQueryAndPeriodicSnapshot()
    {
        Engine.Config.EnableHybridRoutingTelemetryCounters = true;
        Engine.Config.EnableHybridRoutingPerQueryTelemetry = true;
        Engine.Config.HybridRoutingTelemetrySnapshotIntervalQueries = 2;
        Engine.Config.EnableHybridOrderByAdaptiveInitialTopK = true;
        Engine.Config.VectorPredicateFastPathMaxExpansionPasses = 6;
        Engine.Config.VectorPredicateFastPathExpansionFactor = 2;
        Engine.Config.ResetHybridRoutingCounters();

        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
        for (int id = 1; id <= 120; id++)
        {
            Execute($"INSERT INTO Embeddings (Id, Emb, Status) VALUES ({id}, '[1,0,0]', 'inactive')");
        }

        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (121, '[0.95,0.05,0]', 'active')");
        Execute("INSERT INTO Embeddings (Id, Emb, Status) VALUES (122, '[0.95,0.05,0]', 'active')");
        ExecuteAndReturn("CREATE INDEX idx_vec_hybrid_snapshot ON Embeddings (Emb) USING HNSW");

        const string query = @"
            SELECT Id, Emb <=> '[1,0,0]' AS rank
            FROM Embeddings
            WHERE Status = 'active'
            ORDER BY rank ASC
            LIMIT 2";

        var first = ExecuteAndReturn(query);
        var second = ExecuteAndReturn(query);

        Assert.False(first.IsError, string.Join(" | ", first.Messages));
        Assert.False(second.IsError, string.Join(" | ", second.Messages));
        Assert.Equal(2, first.Data.Count);
        Assert.Equal(2, second.Data.Count);

        Assert.Equal(2, Engine.Config.GetHybridRoutingProcessedQueryCount());
        Assert.Equal(2, Engine.Config.GetHybridRoutingUsedQueryCount());
        Assert.True(Engine.Config.GetHybridRoutingTotalExpansionPasses() > 0);
        Assert.Equal(1, Engine.Config.GetHybridRoutingSnapshotEmissions());
        Assert.True(Engine.Config.GetHybridRoutingCounter("hybrid.orderby.accept") >= 2);
    }

    [Fact]
    [BrowserTranslateNeedsSpecificCode("Benchmark depends on runtime Engine.Config toggles and multi-iteration telemetry not representable in SQL-only browser scenarios")]
    public void Benchmark_HybridOrderByAdaptiveInitialTopK_ReducesExpansionPasses_OnMixedWorkload()
    {
        Engine.Config.EnableHybridRoutingTelemetryCounters = true;
        Engine.Config.EnableHybridRoutingPerQueryTelemetry = false;
        Engine.Config.VectorPredicateFastPathMaxExpansionPasses = 8;
        Engine.Config.VectorPredicateFastPathExpansionFactor = 2;

        Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Status VARCHAR)");
        for (int id = 1; id <= 480; id++)
        {
            Execute($"INSERT INTO Embeddings (Id, Emb, Status) VALUES ({id}, '[1,0,0]', 'inactive')");
        }

        for (int id = 481; id <= 500; id++)
        {
            Execute($"INSERT INTO Embeddings (Id, Emb, Status) VALUES ({id}, '[0.95,0.05,0]', 'active')");
        }

        ExecuteAndReturn("CREATE INDEX idx_vec_hybrid_bench ON Embeddings (Emb) USING HNSW");

        const string query = @"
            SELECT Id, Emb <=> '[1,0,0]' AS rank
            FROM Embeddings
            WHERE Status = 'active'
            ORDER BY rank ASC
            LIMIT 5";

        const int iterations = 40;

        Engine.Config.EnableHybridOrderByAdaptiveInitialTopK = false;
        Engine.Config.HybridRoutingTelemetrySnapshotIntervalQueries = iterations;
        Engine.Config.ResetHybridRoutingCounters();
        var baselineStopwatch = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            var result = ExecuteAndReturn(query);
            Assert.False(result.IsError, string.Join(" | ", result.Messages));
            Assert.Equal(5, result.Data.Count);
        }
        baselineStopwatch.Stop();
        long baselineExpansionPasses = Engine.Config.GetHybridRoutingTotalExpansionPasses();

        Engine.Config.EnableHybridOrderByAdaptiveInitialTopK = true;
        Engine.Config.HybridRoutingTelemetrySnapshotIntervalQueries = iterations;
        Engine.Config.ResetHybridRoutingCounters();
        var adaptiveStopwatch = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            var result = ExecuteAndReturn(query);
            Assert.False(result.IsError, string.Join(" | ", result.Messages));
            Assert.Equal(5, result.Data.Count);
        }
        adaptiveStopwatch.Stop();
        long adaptiveExpansionPasses = Engine.Config.GetHybridRoutingTotalExpansionPasses();

        Assert.Equal(iterations, Engine.Config.GetHybridRoutingUsedQueryCount());
        Assert.True(
            adaptiveExpansionPasses < baselineExpansionPasses,
            $"Expected adaptive initial topK to reduce expansion passes. Baseline={baselineExpansionPasses}, Adaptive={adaptiveExpansionPasses}, BaselineMs={baselineStopwatch.ElapsedMilliseconds}, AdaptiveMs={adaptiveStopwatch.ElapsedMilliseconds}");
    }

}

public class InMemoryVectorIndexTests : VectorIndexTestsBase
{
    public InMemoryVectorIndexTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "VectorDB_Mem") { }
}

public class DiskVectorIndexTests : VectorIndexTestsBase
{
    public DiskVectorIndexTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_vector" }, "VectorDB_Disk") { }
}

public class WasmVectorPlannerIntegrationTests : SqlExecutionTestsBase
{
    public WasmVectorPlannerIntegrationTests() : base(new DataVoConfig { StorageMode = StorageMode.Wasm }, "VectorDB_Wasm") { }

    [Fact]
    public void Select_NearestJoin_AutomaticPlannerPath_WorksOnWasmBackend()
    {
        Execute("CREATE TABLE p_embeddings (product_id INT PRIMARY KEY, Emb VECTOR(3))");
        Execute("CREATE TABLE products (Id INT PRIMARY KEY, Name VARCHAR)");

        Execute("INSERT INTO p_embeddings (product_id, Emb) VALUES (1, '[1,0,0]')");
        Execute("INSERT INTO p_embeddings (product_id, Emb) VALUES (2, '[0,1,0]')");
        Execute("INSERT INTO products (Id, Name) VALUES (1, 'Chair')");
        Execute("INSERT INTO products (Id, Name) VALUES (2, 'Table')");
        ExecuteAndReturn("CREATE INDEX idx_p_emb_wasm ON p_embeddings (Emb) USING HNSW");

        var result = ExecuteAndReturn(@"
            SELECT p.Name, p.Id, a.Emb <=> '[0.95,0.05,0]' AS rank
            FROM p_embeddings a
            JOIN products p ON a.product_id = p.Id
            ORDER BY rank ASC
            LIMIT 1");

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        Assert.Equal("Chair", result.Data[0]["p.Name"]);
    }
}
