using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Performance;

[DataVoTable("documents")]
public sealed class Document
{
    [PrimaryKey] public int Id { get; set; }

    public int Kind { get; set; }

    public string Body { get; set; } = "";
}

/// <summary>
/// A compiled <c>Select</c> reads only the columns it projects: a projection that leaves out a wide text column never
/// decodes it, whether the engine streams, sorts or keeps a top-N, and paging stops the scan early.
/// </summary>
[Collection(AllocationSensitiveCollection.Name)]
public sealed class LinqProjectionMemoryTests : IDisposable
{
    private const int Rows = 2_000;
    private const int BodyLength = 2_048;

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_linq_projection_{Guid.NewGuid():N}");

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
    public void StreamedProjection_NeverDecodesTheTextColumn(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Document> documents = db.Table<Document>();

        long models = Measure(() => documents.Where(0, static (d, min) => d.Kind >= min).ToList().Count);
        long ids = Measure(() => documents.Where(0, static (d, min) => d.Kind >= min).Select(static d => d.Id).ToList().Count);

        Assert.True(models > Rows * BodyLength * 2L, $"models allocated {models:N0} B");
        Assert.True(ids * 8 < models, $"the projection allocated {ids:N0} B against {models:N0} B for models");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SortedProjection_DecodesOnlyTheSelectedAndSortedColumns(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        DataVoTable<Document> documents = db.Table<Document>();

        long models = Measure(() => documents.OrderByDescending(static d => d.Kind).ThenBy(static d => d.Id).ToList().Count);
        long ids = Measure(() => documents.OrderByDescending(static d => d.Kind).ThenBy(static d => d.Id).Select(static d => d.Id).ToList().Count);
        long top = Measure(() => documents.OrderByDescending(static d => d.Kind).ThenBy(static d => d.Id).Take(10).Select(static d => d.Id).ToList().Count);

        long bodies = Rows * BodyLength * 2L;
        Assert.Equal(
            Enumerable.Range(1, Rows).OrderByDescending(static id => id % 7).ThenBy(static id => id).Take(10),
            documents.OrderByDescending(static d => d.Kind).ThenBy(static d => d.Id).Take(10).Select(static d => d.Id).ToList());
        Assert.True(models - ids > bodies * 9 / 10, $"the sorted projection allocated {ids:N0} B against {models:N0} B for models: the text column was decoded");
        // The legacy Disk engine copies each stored row's bytes while it scans, so its top-N saves less.
        long budget = models / 20;
        Assert.True(top < budget, $"the top-10 projection allocated {top:N0} B against {models:N0} B for models");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void ProjectionInsideATransaction_SeesTheTransactionsOwnWrites(StorageMode mode)
    {
        // Inside a transaction the rows come from its snapshot with its own writes overlaid; the projection
        // still decodes only the columns it reads, so the wide text column is never turned into strings.
        using DataVoContext db = Open(mode);
        DataVoTable<Document> documents = db.Table<Document>();
        db.Execute("BEGIN");
        try
        {
            Assert.Equal(Rows / 7, documents.Where(0, static (d, kind) => d.Kind == kind).ExecuteUpdate(static set => set.Set(d => d.Kind, 9)));
            Assert.Equal(1, documents.Where(1995, static (d, id) => d.Id == id).ExecuteDelete());
            db.Insert(new Document { Id = Rows + 1, Kind = 9, Body = "inserted" });
            var query = documents.Where(8, static (d, kind) => d.Kind > kind).OrderByDescending(static d => d.Id).Select(static d => d.Id).Take(3);

            Assert.True(query.IsCompiled);
            Assert.Equal([Rows + 1, 1988, 1981], query.ToList());
            Assert.Equal(Rows / 7, documents.Where(9, static (d, kind) => d.Kind == kind).Select(static d => d.Body.Length).Count());

            long models = Measure(() => documents.Where(0, static (d, min) => d.Kind >= min).ToList().Count);
            long ids = Measure(() => documents.Where(0, static (d, min) => d.Kind >= min).Select(static d => d.Id).ToList().Count);
            long bodies = Rows * BodyLength * 2L;
            Assert.True(models > bodies, $"models allocated {models:N0} B");
            Assert.True(models - ids > bodies * 9 / 10, $"the projection allocated {ids:N0} B against {models:N0} B for models: the text column was decoded");
        }
        finally
        {
            db.Execute("ROLLBACK");
        }

        Assert.Equal(0, documents.Where(9, static (d, kind) => d.Kind == kind).Count());
        Assert.Equal(Rows, documents.Select(static d => d.Id).Count());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void TakeOnAStreamedQuery_StopsTheScanEarly(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        var query = db.Table<Document>().Where(0, static (d, min) => d.Kind >= min);

        long all = Measure(() => query.Select(static d => d.Body.Length).ToList().Count);
        long ten = Measure(() => query.Skip(5).Take(10).Select(static d => d.Body.Length).ToList().Count);

        Assert.Equal(10, query.Skip(5).Take(10).Select(static d => d.Body.Length).Count());
        Assert.True(ten * 20 < all, $"ten rows allocated {ten:N0} B against {all:N0} B for all of them");
    }

    private static long Measure(Func<int> run)
    {
        run(); // prepare statements and warm the path
        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.True(run() > 0);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private DataVoContext Open(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
        });
        db.Execute("CREATE DATABASE P");
        db.Execute("USE P");
        db.EnsureDocumentTable();
        string body = new('x', BodyLength);
        for (int id = 1; id <= Rows; id++)
        {
            db.Insert(new Document { Id = id, Kind = id % 7, Body = body });
        }

        return db;
    }
}
