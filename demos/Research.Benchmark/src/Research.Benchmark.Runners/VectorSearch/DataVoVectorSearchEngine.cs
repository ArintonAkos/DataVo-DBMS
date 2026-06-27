using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.VectorSearch;

/// <summary>
/// DataVo vector-search engine: a <c>VECTOR</c> column backed by a DataVo vector index, queried with
/// <c>SearchNearest</c>.
/// </summary>
public sealed class DataVoVectorSearchEngine : IVectorSearchEngine
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Emb");
    private readonly CellValue[] _cells = new CellValue[2];
    private readonly string _indexType;
    private readonly string _name;
    private readonly int _expectedVectors;
    private DataVoContext? _context;

    public DataVoVectorSearchEngine()
        : this("HNSW", "DataVo")
    {
    }

    public DataVoVectorSearchEngine(string indexType, string name, int expectedVectors = 0)
    {
        if (string.IsNullOrWhiteSpace(indexType))
        {
            throw new ArgumentException("Index type cannot be blank.", nameof(indexType));
        }

        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Engine name cannot be blank.", nameof(name));
        }

        _indexType = indexType.ToUpperInvariant();
        _name = name;
        _expectedVectors = Math.Max(0, expectedVectors);
    }

    public string Name => _name;

    public void Initialize(int dimensions)
    {
        _context?.Dispose();
        _context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk("CREATE DATABASE VectorBenchmark");
        ExecuteOk("USE VectorBenchmark");
        ExecuteOk($"CREATE TABLE Vectors (Id INT PRIMARY KEY, Emb VECTOR({dimensions}))");
        ExecuteOk($"CREATE INDEX vidx ON Vectors (Emb) USING {_indexType}");
        Ctx().Engine.IndexManager.ReserveVectorIndex("vidx", "Vectors", "VectorBenchmark", _indexType, _expectedVectors, dimensions);
    }

    public void BeginBatch() { }

    public void CompleteBatch() { }

    public void Insert(long id, float[] vector)
    {
        _cells[0] = CellValue.From(checked((int)id));
        _cells[1] = CellValue.FromVectorOwned(vector);
        Ctx().InsertTyped("Vectors", Schema, _cells);
    }

    public IReadOnlyList<long> Search(float[] query, int k)
    {
        return Ctx().Engine.IndexManager.SearchVector(query, k, "vidx", "Vectors", "VectorBenchmark", _indexType);
    }

    public void Dispose()
    {
        _context?.Dispose();
        _context = null;
    }

    private DataVoContext Ctx() =>
        _context ?? throw new InvalidOperationException("DataVo vector engine has not been initialized.");

    private void ExecuteOk(string sql)
    {
        QueryResult result = Ctx().Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }
}
