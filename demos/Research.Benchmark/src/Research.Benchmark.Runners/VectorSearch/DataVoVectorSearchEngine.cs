using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Indexing.HNSW;
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
    private readonly bool _enableDiversityHeuristic;
    private readonly bool _enableBuildDiagnostics;
    private DataVoContext? _context;
    private List<CellValue[]>? _batchRows;

    public DataVoVectorSearchEngine()
        : this("HNSW", "DataVo")
    {
    }

    public DataVoVectorSearchEngine(
        string indexType,
        string name,
        int expectedVectors = 0,
        bool enableDiversityHeuristic = false,
        bool enableBuildDiagnostics = false)
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
        _enableDiversityHeuristic = enableDiversityHeuristic;
        _enableBuildDiagnostics = enableBuildDiagnostics;
    }

    public string Name => DataVoBenchmarkName.Format(_name);

    public void Initialize(int dimensions)
    {
        _context?.Dispose();
        _context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk("CREATE DATABASE VectorBenchmark");
        ExecuteOk("USE VectorBenchmark");
        ExecuteOk($"CREATE TABLE Vectors (Id INT PRIMARY KEY, Emb VECTOR({dimensions}))");
        ExecuteOk($"CREATE INDEX vidx ON Vectors (Emb) USING {_indexType}");
        Ctx().Engine.IndexManager.ReserveVectorIndex("vidx", "Vectors", "VectorBenchmark", _indexType, _expectedVectors, dimensions);
        if (_indexType == "HNSW" && (_enableDiversityHeuristic || _enableBuildDiagnostics))
        {
            var hnsw = Ctx().Engine.IndexManager.TryGetHnswVectorIndex("vidx", "Vectors", "VectorBenchmark", _indexType)
                ?? throw new InvalidOperationException("DataVo HNSW benchmark index was not available after creation.");
            hnsw.EnableDiversityHeuristic = _enableDiversityHeuristic;
            hnsw.EnableBuildDiagnostics = _enableBuildDiagnostics;
            hnsw.ResetBuildDiagnostics();
        }
    }

    public void BeginBatch()
    {
        _batchRows = new List<CellValue[]>(_expectedVectors > 0 ? _expectedVectors : 0);
    }

    public void CompleteBatch()
    {
        if (_batchRows is null)
        {
            return;
        }

        Ctx().InsertTypedBatch("Vectors", Schema, _batchRows);
        _batchRows = null;
    }

    public void Insert(long id, float[] vector)
    {
        CellValue idCell = CellValue.From(checked((int)id));
        CellValue vectorCell = CellValue.FromVectorOwned(vector);
        if (_batchRows is not null)
        {
            _batchRows.Add([idCell, vectorCell]);
            return;
        }

        _cells[0] = idCell;
        _cells[1] = vectorCell;
        Ctx().InsertTyped("Vectors", Schema, _cells);
    }

    public IReadOnlyList<long> Search(float[] query, int k)
    {
        return Ctx().Engine.IndexManager.SearchVector(query, k, "vidx", "Vectors", "VectorBenchmark", _indexType);
    }

    public bool TryFormatBuildDiagnostics(out string diagnostics)
    {
        diagnostics = string.Empty;
        if (!_enableBuildDiagnostics || _indexType != "HNSW" || _context is null)
        {
            return false;
        }

        var hnsw = Ctx().Engine.IndexManager.TryGetHnswVectorIndex("vidx", "Vectors", "VectorBenchmark", _indexType);
        if (hnsw is null)
        {
            return false;
        }

        HNSWBuildDiagnosticsSnapshot snapshot = hnsw.GetBuildDiagnosticsSnapshot();
        diagnostics =
            $"searchLayerCalls={snapshot.SearchLayerCalls}, " +
            $"searchLayerNeighborVisits={snapshot.SearchLayerNeighborVisits}, " +
            $"edgeAddCalls={snapshot.EdgeAddCalls}, " +
            $"edgeDuplicateSkips={snapshot.EdgeDuplicateSkips}, " +
            $"edgeAppends={snapshot.EdgeAppends}, " +
            $"fullNeighborPrunes={snapshot.FullNeighborPrunes}, " +
            $"selectNeighborsCalls={snapshot.SelectNeighborsCalls}, " +
            $"selectNeighborCandidates={snapshot.SelectNeighborCandidates}, " +
            $"diversityComparisons={snapshot.DiversityComparisons}, " +
            $"diversityOcclusions={snapshot.DiversityOcclusions}, " +
            $"incrementalDiversityPrunes={snapshot.IncrementalDiversityPrunes}, " +
            $"nonDiverseReplacementScans={snapshot.NonDiverseReplacementScans}, " +
            $"nonDiverseReplacements={snapshot.NonDiverseReplacements}, " +
            $"distanceToOrdinalCalls={snapshot.DistanceToOrdinalCalls}, " +
            $"distanceBetweenOrdinalCalls={snapshot.DistanceBetweenOrdinalCalls}";
        return true;
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
