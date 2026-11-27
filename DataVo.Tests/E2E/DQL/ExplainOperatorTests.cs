using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

/// <summary>
/// EXPLAIN reports the operator that actually produced a SELECT's rows, whether it was a zero-allocation fast path,
/// and the exact reason the statement fell back to the general dictionary evaluator when it did.
/// </summary>
public sealed class ExplainOperatorTests : IDisposable
{
    private const int Players = 200;
    private readonly DataVoContext _db;

    public ExplainOperatorTests()
    {
        _db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        Ok("CREATE DATABASE ExplainOps");
        Ok("USE ExplainOps");
        Ok("CREATE TABLE teams (Id INT PRIMARY KEY, Name VARCHAR(40))");
        Ok("CREATE TABLE players (Id INT PRIMARY KEY, TeamId INT, Name VARCHAR(40), Score INT)");
        Ok("INSERT INTO teams (Id, Name) VALUES " + string.Join(", ", Enumerable.Range(1, 10).Select(i => $"({i}, 'team-{i}')")));
        Ok("INSERT INTO players (Id, TeamId, Name, Score) VALUES "
            + string.Join(", ", Enumerable.Range(1, Players).Select(i => $"({i}, {(i % 10) + 1}, 'player-{i}', {i % 50})")));
        Ok("CREATE INDEX idx_players_name ON players (Name)");
    }

    public void Dispose() => _db.Dispose();

    [Fact]
    public void PrimaryKeyEquality_IsAPrimaryKeyPointLookup()
    {
        Dictionary<string, object?> plan = Explain("SELECT Name FROM players WHERE Id = 42");

        Assert.Equal("PrimaryKeyPointLookup", plan["Operator"]);
        Assert.Equal(true, plan["FastPath"]);
        Assert.Null(plan["FallbackReason"]);
        Assert.Equal(1L, plan["RowsScanned"]);
        Assert.Equal(1L, plan["RowsReturned"]);
        Assert.Equal((long)Players, plan["EstimatedRows"]);
        Assert.Contains("_PK_players", (string)plan["IndexesUsed"]!);
    }

    [Fact]
    public void SecondaryIndexEquality_IsACompiledIndexLookup()
    {
        Dictionary<string, object?> plan = Explain("SELECT Id, Score FROM players WHERE Name = 'player-7'");

        Assert.Equal("CompiledIndexLookup", plan["Operator"]);
        Assert.Equal(true, plan["FastPath"]);
        Assert.Equal(1L, plan["RowsReturned"]);
        Assert.Contains("idx_players_name", (string)plan["IndexesUsed"]!);
    }

    [Fact]
    public void UnindexedFilter_IsAStreamingTableScan_ThatReadsEveryRow()
    {
        Dictionary<string, object?> plan = Explain("SELECT Id FROM players WHERE Score >= 40");

        Assert.Equal("StreamingTableScan", plan["Operator"]);
        Assert.Equal(true, plan["FastPath"]);
        Assert.Null(plan["FallbackReason"]);
        Assert.Equal((long)Players, plan["RowsScanned"]);
        Assert.Equal((long)(Players / 50 * 10), plan["RowsReturned"]);
    }

    [Fact]
    public void IntegerEquiJoin_IsAnIntegerHashJoin()
    {
        Dictionary<string, object?> plan = Explain(
            "SELECT p.Name, t.Name FROM players p INNER JOIN teams t ON p.TeamId = t.Id WHERE p.Score > 45");

        Assert.Equal("IntegerHashJoin", plan["Operator"]);
        Assert.Equal(true, plan["FastPath"]);
        Assert.Contains("teams", (string)plan["Details"]!);
    }

    [Fact]
    public void Distinct_FallsBackToTheGeneralEvaluator_AndSaysWhy()
    {
        Dictionary<string, object?> plan = Explain("SELECT DISTINCT TeamId FROM players");

        Assert.Equal("GeneralDictionaryEvaluator", plan["Operator"]);
        Assert.Equal(false, plan["FastPath"]);
        Assert.Equal("TypedScan: SELECT DISTINCT", plan["FallbackReason"]);
        Assert.Equal(10L, plan["RowsReturned"]);
    }

    [Fact]
    public void ComputedProjection_FallsBack_AndSaysWhy()
    {
        Dictionary<string, object?> plan = Explain("SELECT Score * 2 AS Doubled FROM players WHERE Id < 5");

        Assert.Equal("GeneralDictionaryEvaluator", plan["Operator"]);
        Assert.Equal("TypedScan: a computed projection", plan["FallbackReason"]);
    }

    [Fact]
    public void OrAcrossJoinedTables_FallsBack_AndSaysWhy()
    {
        Dictionary<string, object?> plan = Explain(
            "SELECT p.Name FROM players p INNER JOIN teams t ON p.TeamId = t.Id WHERE p.Score > 48 OR t.Name = 'team-3'");

        Assert.Equal("GeneralDictionaryEvaluator", plan["Operator"]);
        Assert.Equal(false, plan["FastPath"]);
        Assert.Equal("TypedJoin: OR across joined tables", plan["FallbackReason"]);
    }

    [Fact]
    public void SubqueryInWhere_FallsBack_AndSaysWhy()
    {
        Dictionary<string, object?> plan = Explain("SELECT Name FROM players WHERE TeamId IN (SELECT Id FROM teams WHERE Name = 'team-2')");

        Assert.Equal("GeneralDictionaryEvaluator", plan["Operator"]);
        Assert.StartsWith("TypedScan: ", (string)plan["FallbackReason"]!);
        Assert.Contains("subquery", (string)plan["FallbackReason"]!);
    }

    [Fact]
    public void JoinInsideAnExplicitTransaction_FallsBack_WithTheMvccReason()
    {
        Ok("BEGIN TRANSACTION");
        try
        {
            Dictionary<string, object?> join = Explain("SELECT p.Name, t.Name FROM players p INNER JOIN teams t ON p.TeamId = t.Id");
            Assert.Equal("GeneralDictionaryEvaluator", join["Operator"]);
            Assert.Equal("TypedJoin: Explicit transaction MVCC scan", join["FallbackReason"]);

            // A single-table scan stays typed inside the transaction: it streams the snapshot.
            Dictionary<string, object?> scan = Explain("SELECT Id FROM players WHERE Score = 3");
            Assert.Equal("MvccSnapshotScan", scan["Operator"]);
            Assert.Equal(true, scan["FastPath"]);
        }
        finally
        {
            Ok("ROLLBACK");
        }
    }

    [Fact]
    public void NearestNeighbourQuery_IsAVectorHnswScan()
    {
        Ok("CREATE TABLE docs (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(20))");
        Ok("INSERT INTO docs (Id, Emb, Label) VALUES (1, '[1,0,0]', 'a'), (2, '[0,1,0]', 'b'), (3, '[0,0,1]', 'c')");
        Ok("CREATE INDEX idx_docs_emb ON docs (Emb) USING HNSW");

        Dictionary<string, object?> plan = Explain("SELECT Id, Emb <=> '[0.9,0.1,0]' AS rank FROM docs ORDER BY rank ASC LIMIT 1");

        Assert.Equal("VectorHnswScan", plan["Operator"]);
        Assert.Equal(true, plan["FastPath"]);
        Assert.Null(plan["FallbackReason"]);
        Assert.Equal(1L, plan["RowsReturned"]);
        Assert.Contains("TypedScan: a computed projection", (string)plan["DeclinedPaths"]!);
    }

    [Fact]
    public void AggregateOverAColumnarIndex_IsAColumnarSimdAggregate()
    {
        Ok("CREATE INDEX cx_players ON players (Score, TeamId) USING COLUMNAR");

        Dictionary<string, object?> plan = Explain("SELECT COUNT(*), SUM(Score) FROM players WHERE TeamId = 3");

        Assert.Equal("ColumnarSimdAggregate", plan["Operator"]);
        Assert.Equal(true, plan["FastPath"]);
        Assert.Equal(0L, plan["RowsScanned"]);
    }

    [Fact]
    public void Explain_StillReturnsThePlannerColumns()
    {
        QueryResult result = _db.Execute("EXPLAIN SELECT * FROM players WHERE Id = 1").Last();

        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Equal(
            ["Plan", "Physical", "EstimatedCost", "Reason", "Operator", "FastPath", "FallbackReason", "DeclinedPaths",
             "EstimatedRows", "RowsScanned", "RowsReturned", "SegmentsRead", "SegmentsSkipped", "IndexesUsed", "Details"],
            result.Fields);
        Assert.Single(result.Data);
    }

    [Fact]
    public void ExplainingAQuery_DoesNotLeakTheTraceIntoLaterQueries()
    {
        Explain("SELECT DISTINCT TeamId FROM players");

        // An ordinary query afterwards runs untraced and still returns its rows.
        QueryResult result = _db.Execute("SELECT Name FROM players WHERE Id = 1").Last();
        Assert.False(result.IsError);
        Assert.Equal("player-1", Assert.Single(result.Data)["Name"]);
    }

    private Dictionary<string, object?> Explain(string select)
    {
        QueryResult result = _db.Execute("EXPLAIN " + select).Last();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return Assert.Single(result.Data);
    }

    private void Ok(string sql)
    {
        QueryResult result = _db.Execute(sql).Last();
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }
}

/// <summary>On the LSM engine, EXPLAIN counts the columnar segments a flushed aggregate read and those its zone maps skipped.</summary>
public sealed class LsmExplainSegmentTests : IDisposable
{
    private const int Rows = 50_000;
    private readonly string _root = TestStorageDirectories.Create("explain_segments");

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
    public void FlushedColumnarAggregate_ReportsSegmentsReadAndSkippedByZoneMaps()
    {
        using (DataVoContext load = Open())
        {
            Ok(load, "CREATE DATABASE Seg");
            Ok(load, "USE Seg");
            Ok(load, "CREATE TABLE readings (Id INT PRIMARY KEY, Reading INT)");
            Ok(load, "CREATE INDEX cx_readings ON readings (Reading) USING COLUMNAR");
            for (int start = 1; start <= Rows; start += 5_000)
            {
                Ok(load, "INSERT INTO readings (Id, Reading) VALUES "
                    + string.Join(", ", Enumerable.Range(start, 5_000).Select(i => $"({i}, {i})")));
            }
        } // shutdown flushes every row into columnar SSTables

        using DataVoContext db = Open();
        Ok(db, "USE Seg");
        QueryResult result = db.Execute("EXPLAIN SELECT COUNT(*) FROM readings WHERE Reading > 49000").Last();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Dictionary<string, object?> plan = Assert.Single(result.Data);

        Assert.Equal("ColumnarSimdAggregate", plan["Operator"]);
        Assert.True((long)plan["SegmentsSkipped"]! > 0, $"skipped {plan["SegmentsSkipped"]}, read {plan["SegmentsRead"]}");
        Assert.True((long)plan["SegmentsRead"]! >= 1);
        Assert.True((long)plan["SegmentsSkipped"]! > (long)plan["SegmentsRead"]!);
    }

    private DataVoContext Open() => new(new DataVoConfig { StorageMode = StorageMode.Lsm, DataDirectory = _root, LsmStrictFsync = false });

    private static void Ok(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql).Last();
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }
}
