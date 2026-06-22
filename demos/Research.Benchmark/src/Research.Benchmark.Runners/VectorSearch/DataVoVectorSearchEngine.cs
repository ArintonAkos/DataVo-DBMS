using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.VectorSearch;

/// <summary>
/// DataVo vector-search engine: a <c>VECTOR</c> column backed by the built-in HNSW index, queried with
/// <c>SearchNearest</c> (approximate nearest neighbour).
/// </summary>
public sealed class DataVoVectorSearchEngine : IVectorSearchEngine
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Emb");
    private readonly CellValue[] _cells = new CellValue[2];
    private DataVoContext? _context;

    public string Name => "DataVo";

    public void Initialize(int dimensions)
    {
        _context?.Dispose();
        _context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk("CREATE DATABASE VectorBenchmark");
        ExecuteOk("USE VectorBenchmark");
        ExecuteOk($"CREATE TABLE Vectors (Id INT PRIMARY KEY, Emb VECTOR({dimensions}))");
        ExecuteOk("CREATE INDEX vidx ON Vectors (Emb) USING HNSW");
    }

    public void BeginBatch() { }

    public void CompleteBatch() { }

    public void Insert(long id, float[] vector)
    {
        _cells[0] = CellValue.From(checked((int)id));
        _cells[1] = CellValue.From(vector);
        Ctx().InsertTyped("Vectors", Schema, _cells);
    }

    public IReadOnlyList<long> Search(float[] query, int k)
    {
        List<Dictionary<string, object?>> rows = Ctx().SearchNearest("Vectors", "vidx", query, k);
        var ids = new long[rows.Count];
        for (int i = 0; i < rows.Count; i++)
        {
            ids[i] = Convert.ToInt64(rows[i]["Id"]);
        }

        return ids;
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
