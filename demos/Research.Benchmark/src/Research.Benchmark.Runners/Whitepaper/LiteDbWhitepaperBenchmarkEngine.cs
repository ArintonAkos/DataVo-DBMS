using LiteDB;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.Whitepaper;

public sealed class LiteDbWhitepaperBenchmarkEngine : IWhitepaperBenchmarkEngine
{
    private readonly object _gate = new();
    private string? _workingDirectory;
    private string? _dbPath;
    private LiteDatabase? _database;
    private ILiteCollection<FlatDocument>? _collection;

    public string Name => "LiteDB";

    public string WorkingDirectory => _workingDirectory
        ?? throw new InvalidOperationException("LiteDB whitepaper engine has not been initialized.");

    public void Initialize(string workingDirectory, bool fresh)
    {
        DisposeDatabase();
        _workingDirectory = workingDirectory;
        Directory.CreateDirectory(workingDirectory);
        _dbPath = Path.Combine(workingDirectory, "whitepaper-litedb.db");
        if (fresh && File.Exists(_dbPath))
        {
            File.Delete(_dbPath);
        }

        _database = new LiteDatabase(_dbPath);
        _collection = _database.GetCollection<FlatDocument>("records");
        _collection.EnsureIndex(static document => document.Id, unique: true);
    }

    public void Preload(int records)
    {
        lock (_gate)
        {
            Database().BeginTrans();
            try
            {
                for (int i = 1; i <= records; i++)
                {
                    Collection().Insert(new FlatDocument
                    {
                        Id = i,
                        Name = $"name-{i}",
                        Value = i,
                        Score = i * 1.5d,
                    });
                }

                Database().Commit();
            }
            catch
            {
                Database().Rollback();
                throw;
            }
        }
    }

    public FlatRecord? Read(long id)
    {
        lock (_gate)
        {
            FlatDocument? document = Collection().FindById(id);
            return document is null
                ? null
                : new FlatRecord(document.Id, document.Name, document.Value, document.Score);
        }
    }

    public void Update(long id, int newValue, double newScore)
    {
        lock (_gate)
        {
            FlatDocument? document = Collection().FindById(id);
            if (document is null)
            {
                throw new InvalidOperationException($"LiteDB whitepaper update could not find Id={id}.");
            }

            document.Value = newValue;
            document.Score = newScore;
            if (!Collection().Update(document))
            {
                throw new InvalidOperationException($"LiteDB whitepaper update failed for Id={id}.");
            }
        }
    }

    public void CloseForRecovery() => DisposeDatabase();

    public void OpenExisting()
    {
        string directory = WorkingDirectory;
        Initialize(directory, fresh: false);
    }

    public void Dispose() => DisposeDatabase();

    private ILiteCollection<FlatDocument> Collection() =>
        _collection ?? throw new InvalidOperationException("LiteDB whitepaper collection has not been initialized.");

    private LiteDatabase Database() =>
        _database ?? throw new InvalidOperationException("LiteDB whitepaper database has not been initialized.");

    private void DisposeDatabase()
    {
        _database?.Dispose();
        _database = null;
        _collection = null;
    }

    private sealed class FlatDocument
    {
        [BsonId]
        public long Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public int Value { get; set; }

        public double Score { get; set; }
    }
}
