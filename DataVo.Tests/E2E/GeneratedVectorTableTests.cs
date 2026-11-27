using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.GeneratedTables;

[DataVoTable("embeddings")]
public sealed class EmbeddedDoc
{
    [PrimaryKey] public int Id { get; set; }

    public string Title { get; set; } = "";

    [VectorDimensions(3), Indexed] public ReadOnlyMemory<float> Embedding { get; set; }

    [VectorDimensions(2)] public ReadOnlyMemory<float>? Summary { get; set; }

    [VectorDimensions(2)] public float[]? Raw { get; set; }
}

/// <summary>
/// <c>[DataVoTable]</c> models declare vectors natively as <see cref="ReadOnlyMemory{T}"/> (or nullable) and
/// <c>float[]</c> with <see cref="VectorDimensionsAttribute"/>; <c>[Indexed]</c> on a vector creates a cosine HNSW
/// index; and vectors held in memory bind as parameters and query <see cref="DataVoContext.SearchNearest(string, string, ReadOnlyMemory{float}, int)"/>
/// without an array copy.
/// </summary>
public sealed class GeneratedVectorTableTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_genvec_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void VectorProperties_RoundTrip_AndAreSearchable(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureEmbeddedDocTable();

        IndexFileAssertions(db);
        db.Insert(new EmbeddedDoc { Id = 1, Title = "x", Embedding = new float[] { 1, 0, 0 }, Summary = new float[] { 0.5f, 0.5f }, Raw = [1, 2] });
        db.Insert(new EmbeddedDoc { Id = 2, Title = "y", Embedding = new float[] { 0, 1, 0 }, Summary = null, Raw = null });

        EmbeddedDoc one = db.GetEmbeddedDocById(1)!;
        Assert.Equal([1f, 0f, 0f], one.Embedding.ToArray());
        Assert.Equal([0.5f, 0.5f], one.Summary!.Value.ToArray());
        Assert.Equal([1f, 2f], one.Raw);

        EmbeddedDoc two = db.GetEmbeddedDocById(2)!;
        Assert.Null(two.Summary);
        Assert.Null(two.Raw);

        Assert.True(db.Update(new EmbeddedDoc { Id = 2, Title = "y", Embedding = new float[] { 0, 0, 1 }, Summary = new float[] { 1, 1 } }));
        Assert.Equal([0f, 0f, 1f], db.GetEmbeddedDocById(2)!.Embedding.ToArray());

        ReadOnlyMemory<float> query = new float[] { 0.9f, 0.1f, 0 };
        List<Dictionary<string, object?>> nearest = db.SearchNearest("embeddings", "ix_embeddings_Embedding", query, topK: 1);
        Assert.Equal(1, Assert.Single(nearest)["Id"]);

        // A ReadOnlyMemory<float> parameter binds as a vector, in a distance expression and in an INSERT.
        DataVoPreparedStatement ranked = db.Prepare("SELECT Id, Embedding <=> @q AS d FROM embeddings ORDER BY d ASC LIMIT 1");
        Assert.Equal(2, Assert.Single(ranked.Execute(DataVoQueryParameter.FromObject("q", new ReadOnlyMemory<float>([0, 0.1f, 1]))).Data)["Id"]);
        db.Prepare("INSERT INTO embeddings (Id, Title, Embedding) VALUES (@id, 'z', @v)")
            .Execute(DataVoQueryParameter.FromObject("id", 3), new DataVoQueryParameter("v", new float[] { 0, 1, 0 }.AsSpan()));
        Assert.Equal([0f, 1f, 0f], db.GetEmbeddedDocById(3)!.Embedding.ToArray());
    }

    private static void IndexFileAssertions(DataVoContext db)
    {
        var index = Assert.Single(db.Engine.Catalog.GetTableIndexes("embeddings", "G"), file => file.AttributeNames.Contains("Embedding"));
        Assert.Equal("HNSW", index.IndexKind, ignoreCase: true);
    }

    private DataVoContext Open(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = mode == StorageMode.Lsm ? _directory : null,
            LsmStrictFsync = false,
        });
        Ok(db, "CREATE DATABASE G");
        Ok(db, "USE G");
        return db;
    }

    private static void Ok(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }
}
