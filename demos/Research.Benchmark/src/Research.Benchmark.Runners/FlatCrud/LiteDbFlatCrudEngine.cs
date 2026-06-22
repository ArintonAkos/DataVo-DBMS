using LiteDB;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.FlatCrud;

/// <summary>
/// LiteDB flat-CRUD engine over an in-memory <see cref="MemoryStream"/> (so BSON serialize/deserialize is
/// still measured — that is the comparison point). Uses the idiomatic keyed collection + <c>FindById</c>.
/// </summary>
public sealed class LiteDbFlatCrudEngine : IFlatCrudEngine
{
    private LiteDatabase? _database;
    private ILiteCollection<FlatDocument>? _collection;

    public string Name => "LiteDB";

    public void Initialize()
    {
        _database?.Dispose();
        _database = new LiteDatabase(new MemoryStream());
        _collection = _database.GetCollection<FlatDocument>("records");
    }

    public void BeginBatch() => Database().BeginTrans();

    public void CompleteBatch() => Database().Commit();

    public void Insert(FlatRecord record)
    {
        Collection().Insert(new FlatDocument
        {
            Id = record.Id,
            Name = record.Name,
            Value = record.Value,
            Score = record.Score,
        });
    }

    public FlatRecord? GetById(long id)
    {
        FlatDocument? document = Collection().FindById(id);
        return document is null
            ? null
            : new FlatRecord(document.Id, document.Name, document.Value, document.Score);
    }

    public void Dispose()
    {
        _database?.Dispose();
        _database = null;
        _collection = null;
    }

    private ILiteCollection<FlatDocument> Collection() =>
        _collection ?? throw new InvalidOperationException("LiteDB flat-CRUD engine has not been initialized.");

    private LiteDatabase Database() =>
        _database ?? throw new InvalidOperationException("LiteDB flat-CRUD engine has not been initialized.");

    private sealed class FlatDocument
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
        public double Score { get; set; }
    }
}
