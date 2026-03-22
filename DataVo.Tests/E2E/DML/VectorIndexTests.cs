using DataVo.Core.StorageEngine.Config;

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
        Assert.IsType<float[]>(result.Data[0]["Emb"]);

        var vector = (float[])result.Data[0]["Emb"];
        Assert.Equal(3, vector.Length);
        Assert.Equal(1f, vector[0]);
        Assert.Equal(0f, vector[1]);
        Assert.Equal(0f, vector[2]);
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
}

public class InMemoryVectorIndexTests : VectorIndexTestsBase
{
    public InMemoryVectorIndexTests() : base(new DataVoConfig { StorageMode = StorageMode.InMemory }, "VectorDB_Mem") { }
}

public class DiskVectorIndexTests : VectorIndexTestsBase
{
    public DiskVectorIndexTests() : base(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = "./test_datavo_vector" }, "VectorDB_Disk") { }
}
