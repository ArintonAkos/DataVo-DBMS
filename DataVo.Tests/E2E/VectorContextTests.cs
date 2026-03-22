using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class VectorContextTests
{
    [Fact]
    public void DataVoContext_SearchNearest_ReturnsRankedRows()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

        context.Execute("CREATE DATABASE VecCtx");
        context.Execute("USE VecCtx");
        context.Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR)");
        context.Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (1, '[1,0,0]', 'A')");
        context.Execute("INSERT INTO Embeddings (Id, Emb, Label) VALUES (2, '[0,1,0]', 'B')");
        context.Execute("CREATE INDEX idx_emb ON Embeddings (Emb) USING HNSW");

        List<Dictionary<string, dynamic>> results = context.SearchNearest("Embeddings", "idx_emb", "[0.9,0.1,0]", topK: 1);

        Assert.Single(results);
        Assert.Equal("A", results[0]["Label"]);
    }
}
