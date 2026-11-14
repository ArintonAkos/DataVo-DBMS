using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace Research.Benchmark.Runners.HybridVector;

/// <summary>
/// DataVo on its LSM engine (Relaxed: no flush per commit) with a FLAT or HNSW vector index, queried through
/// prepared SQL: <c>SELECT Id, Emb &lt;=&gt; @q AS dist ... ORDER BY dist LIMIT 10</c>. The query vector is bound as
/// a <c>float[]</c>; DataVo's parameter binder turns it into vector text, which is parsed back on each call.
/// </summary>
public sealed class DataVoHybridVectorEngine(string indexType) : IHybridVectorEngine
{
    private const string Database = "Hybrid";
    private const int BatchSize = 4_096;
    private readonly Dictionary<HybridShape, DataVoPreparedStatement> _prepared = [];
    private DataVoContext? _context;

    public string Name => DataVoBenchmarkName.Format($"DataVo {(indexType == "FLAT" ? "Flat" : "HNSW")}");

    public string Notes => $"LSM, LsmStrictFsync=false; CREATE INDEX ... USING {indexType} (default parameters); InsertTypedBatch load; prepared SQL with the query vector bound as float[].";

    public void Initialize(string directory, int dimensions)
    {
        _context = new DataVoContext(new DataVoConfig
        {
            StorageMode = StorageMode.Lsm,
            DiskStoragePath = directory,
            LsmStrictFsync = false,
        });
        Ok($"CREATE DATABASE {Database}");
        Ok($"USE {Database}");
        Ok("CREATE TABLE Authors (Id INT PRIMARY KEY, Name VARCHAR(16), Country VARCHAR(4))");
        Ok($"CREATE TABLE Docs (Id INT PRIMARY KEY, Category INT, Price DOUBLE, AuthorId INT, Emb VECTOR({dimensions}))");
        Ok($"CREATE INDEX vx ON Docs (Emb) USING {indexType}");
    }

    public void Load(HybridVectorData data)
    {
        Ctx.Engine.IndexManager.ReserveVectorIndex("vx", "Docs", Database, indexType, data.Documents, data.Dimensions);
        var authors = new List<CellValue[]>(data.Authors);
        for (int id = 1; id <= data.Authors; id++)
        {
            authors.Add([CellValue.From(id), CellValue.From(HybridVectorData.AuthorName(id)), CellValue.From(HybridVectorData.AuthorCountry(id))]);
        }

        Ctx.InsertTypedBatch("Authors", new ReactiveRowSchema("Id", "Name", "Country"), authors);

        var schema = new ReactiveRowSchema("Id", "Category", "Price", "AuthorId", "Emb");
        var rows = new List<CellValue[]>(BatchSize);
        for (int id = 1; id <= data.Documents; id++)
        {
            rows.Add([
                CellValue.From(id),
                CellValue.From(data.Category[id]),
                CellValue.From(data.Price[id]),
                CellValue.From(data.AuthorId[id]),
                CellValue.FromVectorOwned((float[])data.Embeddings[id].Clone()),
            ]);
            if (rows.Count == BatchSize || id == data.Documents)
            {
                Ctx.InsertTypedBatch("Docs", schema, rows);
                rows = new List<CellValue[]>(BatchSize);
            }
        }
    }

    public string SqlFor(HybridShape shape) => shape switch
    {
        HybridShape.V0Unfiltered => "SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist LIMIT 10",
        HybridShape.V1Selective => "SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Category = @c AND Price < @p ORDER BY dist LIMIT 10",
        HybridShape.V2Broad => "SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Price < @p ORDER BY dist LIMIT 10",
        HybridShape.V3JoinDocFilter => "SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE d.Category = @c AND d.Price < @p ORDER BY dist LIMIT 10",
        HybridShape.V4JoinAuthorFilter => "SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE a.Country = @k ORDER BY dist LIMIT 10",
        _ => throw new ArgumentOutOfRangeException(nameof(shape)),
    };

    public IReadOnlyList<long> Search(HybridShape shape, float[] query, int queryIndex)
    {
        QueryResult result = Run(shape, query, queryIndex);
        var ids = new List<long>(result.Data.Count);
        foreach (Dictionary<string, object?> row in result.Data)
        {
            ids.Add(Convert.ToInt64(row.Values.First()));
        }

        return ids;
    }

    public HybridRouting? Explain(HybridShape shape, float[] query, int queryIndex)
    {
        DataVoDiagnostics diagnostics = Ctx.Diagnostics;
        diagnostics.Enabled = true;
        Ctx.Engine.Config.ResetHybridRoutingCounters();
        try
        {
            Run(shape, query, queryIndex);
            RuntimeQueryStats? stats = diagnostics.LastQuery;
            string[] buckets = [.. Ctx.Engine.Config.GetHybridRoutingCountersSnapshot().Where(static kv => kv.Value > 0).Select(static kv => kv.Key).Order(StringComparer.Ordinal)];
            return new HybridRouting(stats?.VectorIndexUsed ?? false, stats?.VectorTopK ?? 0, stats?.VectorExpansionPasses ?? 0, stats?.FullTableScan ?? false, buckets);
        }
        finally
        {
            diagnostics.Enabled = false;
        }
    }

    public void Dispose()
    {
        _prepared.Clear();
        _context?.Dispose();
        _context = null;
    }

    private QueryResult Run(HybridShape shape, float[] query, int queryIndex)
    {
        if (!_prepared.TryGetValue(shape, out DataVoPreparedStatement? statement))
        {
            statement = Ctx.Prepare(SqlFor(shape));
            _prepared[shape] = statement;
        }

        (int category, double price, string country) = HybridVectorData.ParametersFor(queryIndex);
        DataVoQueryParameter vector = DataVoQueryParameter.FromObject("q", query);
        QueryResult result = shape switch
        {
            HybridShape.V0Unfiltered => statement.Execute(vector),
            HybridShape.V1Selective or HybridShape.V3JoinDocFilter => statement.Execute(vector, ("c", category), ("p", price)),
            HybridShape.V2Broad => statement.Execute(vector, ("p", price)),
            HybridShape.V4JoinAuthorFilter => statement.Execute(vector, ("k", country)),
            _ => throw new ArgumentOutOfRangeException(nameof(shape)),
        };

        return result.IsError ? throw new InvalidOperationException("DataVo failed: " + string.Join("; ", result.Messages)) : result;
    }

    private DataVoContext Ctx => _context ?? throw new InvalidOperationException("DataVo is not open.");

    private void Ok(string sql)
    {
        QueryResult result = Ctx.Execute(sql)[^1];
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}: {string.Join("; ", result.Messages)}");
        }
    }
}
