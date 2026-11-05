using DataVo.Core;
using DataVo.Core.Indexing.HNSW;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Lsm;

/// <summary>
/// HNSW graph snapshots in LSM mode: a clean close saves the graph, open loads it instead of rebuilding,
/// a stale snapshot (as after a crash) is caught up from the rows, and a damaged one is ignored.
/// </summary>
/// <remarks>
/// "Rebuilt or not" is read from the graph's mutation count: a loaded snapshot starts at zero, a rebuild
/// inserts every row. A crash is simulated by putting back an older snapshot: the rows then hold
/// changes the snapshot never saw, exactly the state a crash before the next clean close leaves.
/// </remarks>
public sealed class LsmVectorSnapshotTests : IDisposable
{
    private const int Rows = 40;
    private readonly string _root = TestStorageDirectories.Create("lsm_vector_snapshot");

    private string SnapshotPath => Path.Combine(_root, "V", "Items", HnswSnapshotFile.DirectoryName, "idx_emb" + HnswSnapshotFile.Extension);

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
    public void CleanClose_SavesTheGraph_AndReopenLoadsItWithoutRebuilding()
    {
        Seed();
        Assert.True(File.Exists(SnapshotPath));

        using DataVoContext reopened = Open();
        Execute(reopened, "USE V");
        Assert.Equal(7, NearestId(reopened, Vector(7)));
        Assert.Equal(0, Graph(reopened).MutationCount);
        Assert.Equal(Rows, Graph(reopened).Count);
    }

    [Fact]
    public void StaleSnapshot_IsCaughtUpWithRowsChangedSince()
    {
        Seed();
        byte[] stale = File.ReadAllBytes(SnapshotPath);

        using (DataVoContext context = Open())
        {
            Execute(context, "USE V");
            Execute(context, $"INSERT INTO Items (Id, Emb) VALUES (100, '{Literal(Vector(100))}')");
            Execute(context, $"UPDATE Items SET Emb = '{Literal(Opposite(Vector(3)))}' WHERE Id = 3");
            Execute(context, "DELETE FROM Items WHERE Id = 5");
        }

        File.WriteAllBytes(SnapshotPath, stale);

        using DataVoContext reopened = Open();
        Execute(reopened, "USE V");
        HNSWIndex graph = Graph(reopened);
        Assert.Equal(100, NearestId(reopened, Vector(100)));
        Assert.Equal(3, NearestId(reopened, Opposite(Vector(3))));
        Assert.NotEqual(3, NearestId(reopened, Vector(3)));
        Assert.Equal(Rows, graph.Count); // +1 inserted, -1 deleted
        Assert.DoesNotContain(5, NearestIds(reopened, Vector(5), Rows));

        // Caught up, not rebuilt: one delete pass plus the two changed rows (a re-insert also deletes).
        Assert.InRange(graph.MutationCount, 1, 5);
    }

    [Fact]
    public void RowsWrittenAfterADropMarker_AreNotMistakenForSnapshottedRows()
    {
        // The last commit before the clean close is a DROP TABLE marker, so the snapshot's watermark is
        // above every LSN any table flushed. New commits must still number above the watermark.
        using (DataVoContext context = Open())
        {
            CreateAndFill(context);
            Execute(context, "CREATE TABLE Other (Id INT PRIMARY KEY)");
            Execute(context, "DROP TABLE Other");
        }

        byte[] stale = File.ReadAllBytes(SnapshotPath);
        using (DataVoContext context = Open())
        {
            Execute(context, "USE V");
            Execute(context, $"INSERT INTO Items (Id, Emb) VALUES (100, '{Literal(Vector(100))}')");
        }

        File.WriteAllBytes(SnapshotPath, stale);

        using DataVoContext reopened = Open();
        Execute(reopened, "USE V");
        Assert.Equal(100, NearestId(reopened, Vector(100)));
        Assert.Equal(Rows + 1, Graph(reopened).Count);
    }

    [Fact]
    public void DamagedSnapshot_IsIgnored_AndTheIndexRebuilt()
    {
        Seed();
        byte[] bytes = File.ReadAllBytes(SnapshotPath);
        bytes[bytes.Length / 2] ^= 0x5A;
        File.WriteAllBytes(SnapshotPath, bytes);

        using DataVoContext reopened = Open();
        Execute(reopened, "USE V");
        Assert.Equal(7, NearestId(reopened, Vector(7)));
        Assert.True(Graph(reopened).MutationCount >= Rows); // rebuilt from the rows
    }

    [Fact]
    public void DropIndex_DeletesTheSnapshot()
    {
        Seed();

        using (DataVoContext context = Open())
        {
            Execute(context, "USE V");
            Execute(context, "DROP INDEX idx_emb ON Items");
            Assert.False(File.Exists(SnapshotPath));

            Execute(context, "CREATE INDEX idx_emb ON Items (Emb) USING HNSW");
        }

        using DataVoContext reopened = Open();
        Execute(reopened, "USE V");
        Assert.Equal(7, NearestId(reopened, Vector(7)));
        Assert.Equal(0, Graph(reopened).MutationCount);
    }

    [Fact]
    public void SnapshotRoundTrip_PreservesTheGraphExactly()
    {
        var graph = new HNSWIndex();
        for (int i = 0; i < 300; i++)
        {
            graph.Insert(i, Vector(i));
        }

        graph.Delete([10, 20, 30]);
        string path = Path.Combine(_root, "roundtrip" + HnswSnapshotFile.Extension);
        HnswSnapshotFile.Write(path, graph, watermark: 42, column: "Emb");

        Assert.True(HnswSnapshotFile.TryRead(path, "Emb", out HNSWIndex? loaded, out long watermark));
        Assert.Equal(42, watermark);
        Assert.Equal(graph.Count, loaded!.Count);
        for (int i = 0; i < 300; i += 7)
        {
            Assert.Equal(graph.SearchTopK(Vector(i), 5), loaded.SearchTopK(Vector(i), 5));
        }

        Assert.False(HnswSnapshotFile.TryRead(path, "OtherColumn", out _, out _));

        // Inserting after a load works (node locks and free slots are restored).
        loaded.Insert(1000, Vector(1000));
        Assert.Equal(1000, loaded.SearchTopK(Vector(1000), 1)[0]);
    }

    private void Seed()
    {
        using DataVoContext context = Open();
        CreateAndFill(context);
    }

    private static void CreateAndFill(DataVoContext context)
    {
        Execute(context, "CREATE DATABASE V");
        Execute(context, "USE V");
        Execute(context, "CREATE TABLE Items (Id INT PRIMARY KEY, Emb VECTOR(8))");
        Execute(context, "CREATE INDEX idx_emb ON Items (Emb) USING HNSW");
        for (int i = 1; i <= Rows; i++)
        {
            Execute(context, $"INSERT INTO Items (Id, Emb) VALUES ({i}, '{Literal(Vector(i))}')");
        }
    }

    // Distinct directions per id, so the nearest neighbour of Vector(i) is row i.
    private static float[] Vector(int seed)
    {
        var random = new Random(seed);
        var vector = new float[8];
        for (int d = 0; d < vector.Length; d++)
        {
            vector[d] = (float)(random.NextDouble() * 2 - 1);
        }

        return vector;
    }

    private static float[] Opposite(float[] vector) => [.. vector.Select(v => -v)];

    private static string Literal(float[] vector) =>
        "[" + string.Join(",", vector.Select(v => v.ToString("R", System.Globalization.CultureInfo.InvariantCulture))) + "]";

    private static HNSWIndex Graph(DataVoContext context) =>
        context.Engine.IndexManager.TryGetLoadedHnswIndex("idx_emb", "Items", "V")
        ?? throw new InvalidOperationException("The vector index is not loaded.");

    private static int NearestId(DataVoContext context, float[] query) => NearestIds(context, query, 1)[0];

    private static List<int> NearestIds(DataVoContext context, float[] query, int topK) =>
        [.. context.SearchNearest("Items", "idx_emb", query, topK).Select(row => Convert.ToInt32(row["Id"]))];

    private DataVoContext Open() => new(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = _root, LsmStrictFsync = false });

    private static void Execute(DataVoContext context, string sql)
    {
        var result = context.Execute(sql);
        Assert.False(result[0].IsError, $"{sql}: {string.Join(" | ", result[0].Messages)}");
    }
}
