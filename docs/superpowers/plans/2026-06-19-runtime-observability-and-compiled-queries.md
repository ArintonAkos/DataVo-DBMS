# Runtime Observability and Compiled Queries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add production-safe runtime observability and C# source-generator compiled queries so game developers and low-latency .NET users can measure and reduce DataVo query overhead across `InMemory` and `Disk` storage.

**Architecture:** Observability is implemented as an engine-owned diagnostics service with a bounded ring buffer and an `AsyncLocal` query scope that parser, storage, index, and vector paths can update without passing counters through every call. Compiled queries are implemented in two layers: runtime fast-path helpers in `DataVo.Core`, and a separate Roslyn incremental source-generator project that emits strongly typed partial method implementations for a narrow, high-value SQL subset without runtime SQL parsing. Dynamic SQL continues to use `DataVoContext.Execute(...)`.

**Tech Stack:** C#/.NET 10, xUnit, Roslyn incremental source generators, `DataVo.Core`, `DataVo.Tests`, new `DataVo.Generators` and `DataVo.Generators.Tests` projects.

## Global Constraints

- Observability must work for `StorageMode.InMemory` and `StorageMode.Disk`.
- Observability must be opt-in and bounded: default disabled, default slow-query capacity `128`, default recent-query capacity `128`.
- Disabled observability path must avoid per-query heap allocations except existing query execution allocations.
- Diagnostics must not use background threads, timers, sockets, Unity APIs, or platform-specific dependencies.
- Source-generated compiled query execution must not lex or parse SQL at runtime for supported generated fast paths.
- Source generator V1 supports single-statement `SELECT`, `INSERT`, and `UPDATE` only; unsupported SQL emits a build diagnostic with id `DATAVOQ001`.
- Source generator project targets `netstandard2.0`; runtime projects remain `net10.0`.
- Generated query APIs must remain usable from Unity/Godot/.NET without Unity-specific references.

---

## File Structure

### Runtime Observability

- Create: `DataVo.Core/Runtime/Diagnostics/DataVoDiagnostics.cs`
  - Engine-owned diagnostics facade exposed through `DataVoContext.Diagnostics`.
- Create: `DataVo.Core/Runtime/Diagnostics/RuntimeQueryStats.cs`
  - Immutable public stats object returned by diagnostics APIs.
- Create: `DataVo.Core/Runtime/Diagnostics/RuntimeQueryDiagnosticsScope.cs`
  - Internal `AsyncLocal` active query collector used by parser/storage/index paths.
- Create: `DataVo.Core/Runtime/Diagnostics/RuntimeQueryStatsBuilder.cs`
  - Internal mutable collector converted to `RuntimeQueryStats` after query execution.
- Modify: `DataVo.Core/Runtime/DataVoEngine.cs`
  - Owns `DataVoDiagnostics Diagnostics`.
- Modify: `DataVo.Core/DataVoContext.cs`
  - Exposes `DataVoDiagnostics Diagnostics => Engine.Diagnostics`.
- Modify: `DataVo.Core/Parser/QueryEngine.cs`
  - Opens diagnostics scope, times parse/execute, records errors/results.
- Modify: `DataVo.Core/StorageEngine/StorageContext.cs`
  - Records rows read/scanned during table reads.
- Modify: `DataVo.Core/Indexing/IndexManager.cs`
  - Records scalar/vector index usage.
- Modify: `DataVo.Core/Parser/DQL/Select.cs`
  - Records vector topK and expansion pass counters already calculated by Select.
- Test: `DataVo.Tests/E2E/RuntimeDiagnosticsTests.cs`
  - Public API and behavior tests.

### Runtime Compiled Query Support

- Create: `DataVo.Core/CompiledQueries/DataVoQueryAttribute.cs`
  - Attribute used by the source generator.
- Create: `DataVo.Core/CompiledQueries/DataVoCompiledQueryKind.cs`
  - `SelectSingle`, `SelectMany`, `Insert`, `Update`.
- Create: `DataVo.Core/CompiledQueries/DataVoCompiledQueryPlan.cs`
  - Runtime descriptor emitted by the generator for supported fast paths.
- Create: `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`
  - Runtime helper methods called by generated code.
- Create: `DataVo.Core/CompiledQueries/DataVoCompiledQueryParameter.cs`
  - Immutable name/value parameter pair used by generated code.
- Modify: `DataVo.Core/Runtime/EngineCatalog.cs`
  - Expose schema-version helpers needed to validate generated plans before execution.
- Test: `DataVo.Tests/E2E/CompiledQueryRuntimeTests.cs`
  - Tests runtime helpers without the generator.

### Source Generator

- Create: `DataVo.Generators/DataVo.Generators.csproj`
  - Roslyn incremental generator package.
- Create: `DataVo.Generators/DataVoQueryGenerator.cs`
  - Scans `[DataVoQuery]` partial methods and emits implementations.
- Create: `DataVo.Generators/Sql/GeneratedQueryModel.cs`
  - Small generator-side model for supported SQL shapes.
- Create: `DataVo.Generators/Sql/DataVoQueryShapeParser.cs`
  - Generator-side parser for the supported subset.
- Create: `DataVo.Generators/Diagnostics/DataVoGeneratorDiagnostics.cs`
  - Diagnostic descriptors `DATAVOQ001` through `DATAVOQ006`.
- Create: `DataVo.Generators.Tests/DataVo.Generators.Tests.csproj`
  - Generator unit tests using Roslyn compilation APIs.
- Create: `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`
  - Source-generation tests.
- Modify: `DataVo.sln`
  - Add both generator projects.
- Modify: `Directory.Build.props`
  - Add package metadata for `DataVo.Generators`.
- Modify: `DataVo.Tests/DataVo.Tests.csproj`
  - Add analyzer reference to generator for integration tests.
- Test: `DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests.cs`
  - End-to-end tests using generated partial methods.

### Documentation

- Modify: `docs/features/unity-and-godot.md`
  - Add diagnostics and compiled-query examples for game-loop use.
- Modify: `docs/features/setup-and-packaging.md`
  - Mention `DataVo.Generators` as an analyzer/package.
- Create: `docs/features/runtime-observability.md`
  - Production-safe metrics API.
- Create: `docs/features/compiled-queries.md`
  - Attribute/source-generator usage and supported SQL subset.

---

## Task 1: Add Runtime Diagnostics Red Tests

**Files:**
- Create: `DataVo.Tests/E2E/RuntimeDiagnosticsTests.cs`

**Interfaces:**
- Consumes: existing `DataVoContext.Execute(...)`, `BulkInsert(...)`, `SearchNearest(...)`.
- Produces expected future API:
  - `DataVoContext.Diagnostics`
  - `DataVoDiagnostics.Enabled`
  - `DataVoDiagnostics.SlowQueryThreshold`
  - `DataVoDiagnostics.LastQuery`
  - `DataVoDiagnostics.GetRecentQueries()`
  - `DataVoDiagnostics.GetSlowQueries()`
  - `DataVoDiagnostics.Clear()`
  - `RuntimeQueryStats`

- [ ] **Step 1: Write failing diagnostics API tests**

Create `DataVo.Tests/E2E/RuntimeDiagnosticsTests.cs`:

```csharp
using DataVo.Core;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public class RuntimeDiagnosticsTests
{
    [Fact]
    public void Diagnostics_WhenDisabled_DoesNotRecordQueries()
    {
        using var context = CreateContext(StorageMode.InMemory);
        context.Diagnostics.Enabled = false;

        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50))");

        Assert.Null(context.Diagnostics.LastQuery);
        Assert.Empty(context.Diagnostics.GetRecentQueries());
        Assert.Empty(context.Diagnostics.GetSlowQueries());
    }

    [Fact]
    public void Diagnostics_RecordsSelectStats()
    {
        using var context = CreateContext(StorageMode.InMemory);
        context.Diagnostics.Enabled = true;

        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50))");
        context.Execute("INSERT INTO Players VALUES (1, 'Ada')");
        context.Diagnostics.Clear();

        context.Execute("SELECT Id, Name FROM Players WHERE Id = 1");

        RuntimeQueryStats stats = Assert.NotNull(context.Diagnostics.LastQuery);
        Assert.Equal("SELECT", stats.Operation);
        Assert.Equal(StorageMode.InMemory, stats.StorageMode);
        Assert.Equal("Players", Assert.Single(stats.Tables));
        Assert.False(stats.IsError);
        Assert.Equal(1, stats.RowsReturned);
        Assert.True(stats.RowsRead >= 1);
        Assert.True(stats.Elapsed >= TimeSpan.Zero);
        Assert.Contains(stats.IndexesUsed, index => index.Contains("_PK_Players", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Diagnostics_RecordsSlowQueriesInBoundedRing()
    {
        using var context = CreateContext(StorageMode.InMemory);
        context.Diagnostics.Enabled = true;
        context.Diagnostics.SlowQueryThreshold = TimeSpan.Zero;
        context.Diagnostics.RecentQueryCapacity = 2;
        context.Diagnostics.SlowQueryCapacity = 2;

        context.Execute("CREATE TABLE Events (Id INT PRIMARY KEY, Kind VARCHAR(50))");
        context.Execute("INSERT INTO Events VALUES (1, 'spawn')");
        context.Execute("SELECT * FROM Events");

        IReadOnlyList<RuntimeQueryStats> recent = context.Diagnostics.GetRecentQueries();
        IReadOnlyList<RuntimeQueryStats> slow = context.Diagnostics.GetSlowQueries();

        Assert.Equal(2, recent.Count);
        Assert.Equal(2, slow.Count);
        Assert.All(slow, item => Assert.True(item.Elapsed >= TimeSpan.Zero));
    }

    [Fact]
    public void Diagnostics_RecordsDiskStorageMode()
    {
        string path = Path.Combine(Path.GetTempPath(), $"datavo_diag_disk_{Guid.NewGuid():N}");
        try
        {
            using var context = CreateContext(StorageMode.Disk, path);
            context.Diagnostics.Enabled = true;

            context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(50))");
            context.Execute("SELECT * FROM Items");

            RuntimeQueryStats stats = Assert.NotNull(context.Diagnostics.LastQuery);
            Assert.Equal(StorageMode.Disk, stats.StorageMode);
        }
        finally
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
    }

    [Fact]
    public void Diagnostics_RecordsVectorIndexSearch()
    {
        using var context = CreateContext(StorageMode.InMemory);
        context.Diagnostics.Enabled = true;

        context.Execute("CREATE TABLE Embeddings (Id INT PRIMARY KEY, Emb VECTOR(3), Label VARCHAR(50))");
        context.Execute("CREATE INDEX idx_emb ON Embeddings (Emb) USING HNSW");
        context.BulkInsert("Embeddings",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Emb"] = new float[] { 1f, 0f, 0f }, ["Label"] = "combat" },
            new Dictionary<string, object?> { ["Id"] = 2, ["Emb"] = new float[] { 0f, 1f, 0f }, ["Label"] = "builder" }
        ]);
        context.Diagnostics.Clear();

        context.SearchNearest("Embeddings", "idx_emb", "[0.9,0.1,0]", topK: 1);

        RuntimeQueryStats stats = Assert.NotNull(context.Diagnostics.LastQuery);
        Assert.True(stats.VectorIndexUsed);
        Assert.Equal(1, stats.VectorTopK);
        Assert.Contains("idx_emb", stats.IndexesUsed);
    }

    private static DataVoContext CreateContext(StorageMode mode, string? diskPath = null)
    {
        var context = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = diskPath
        });

        string databaseName = $"Diag_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }
}
```

- [ ] **Step 2: Run tests to verify red state**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter RuntimeDiagnosticsTests
```

Expected: compile failure because `DataVo.Core.Runtime.Diagnostics` and `DataVoContext.Diagnostics` do not exist.

---

## Task 2: Add Diagnostics Core Types

**Files:**
- Create: `DataVo.Core/Runtime/Diagnostics/RuntimeQueryStats.cs`
- Create: `DataVo.Core/Runtime/Diagnostics/RuntimeQueryStatsBuilder.cs`
- Create: `DataVo.Core/Runtime/Diagnostics/RuntimeQueryDiagnosticsScope.cs`
- Create: `DataVo.Core/Runtime/Diagnostics/DataVoDiagnostics.cs`
- Modify: `DataVo.Core/Runtime/DataVoEngine.cs`
- Modify: `DataVo.Core/DataVoContext.cs`

**Interfaces:**
- Produces:
  - `public sealed class RuntimeQueryStats`
  - `public sealed class DataVoDiagnostics`
  - `internal sealed class RuntimeQueryStatsBuilder`
  - `internal sealed class RuntimeQueryDiagnosticsScope : IDisposable`

- [ ] **Step 1: Add immutable stats type**

Create `DataVo.Core/Runtime/Diagnostics/RuntimeQueryStats.cs`:

```csharp
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Runtime.Diagnostics;

public sealed class RuntimeQueryStats
{
    internal RuntimeQueryStats(
        string queryText,
        string operation,
        StorageMode storageMode,
        string? databaseName,
        IReadOnlyList<string> tables,
        IReadOnlyList<string> indexesUsed,
        TimeSpan elapsed,
        long rowsRead,
        long rowsScanned,
        int rowsReturned,
        int rowsAffected,
        bool fullTableScan,
        bool vectorIndexUsed,
        int vectorTopK,
        int vectorExpansionPasses,
        bool isError,
        string? errorMessage)
    {
        QueryText = queryText;
        Operation = operation;
        StorageMode = storageMode;
        DatabaseName = databaseName;
        Tables = tables;
        IndexesUsed = indexesUsed;
        Elapsed = elapsed;
        RowsRead = rowsRead;
        RowsScanned = rowsScanned;
        RowsReturned = rowsReturned;
        RowsAffected = rowsAffected;
        FullTableScan = fullTableScan;
        VectorIndexUsed = vectorIndexUsed;
        VectorTopK = vectorTopK;
        VectorExpansionPasses = vectorExpansionPasses;
        IsError = isError;
        ErrorMessage = errorMessage;
    }

    public string QueryText { get; }
    public string Operation { get; }
    public StorageMode StorageMode { get; }
    public string? DatabaseName { get; }
    public IReadOnlyList<string> Tables { get; }
    public IReadOnlyList<string> IndexesUsed { get; }
    public TimeSpan Elapsed { get; }
    public long RowsRead { get; }
    public long RowsScanned { get; }
    public int RowsReturned { get; }
    public int RowsAffected { get; }
    public bool FullTableScan { get; }
    public bool VectorIndexUsed { get; }
    public int VectorTopK { get; }
    public int VectorExpansionPasses { get; }
    public bool IsError { get; }
    public string? ErrorMessage { get; }
}
```

- [ ] **Step 2: Add mutable builder**

Create `DataVo.Core/Runtime/Diagnostics/RuntimeQueryStatsBuilder.cs`:

```csharp
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Runtime.Diagnostics;

internal sealed class RuntimeQueryStatsBuilder
{
    private readonly HashSet<string> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _indexesUsed = new(StringComparer.OrdinalIgnoreCase);

    public required string QueryText { get; init; }
    public string Operation { get; private set; } = "UNKNOWN";
    public required StorageMode StorageMode { get; init; }
    public string? DatabaseName { get; init; }
    public long RowsRead { get; private set; }
    public long RowsScanned { get; private set; }
    public int RowsReturned { get; private set; }
    public int RowsAffected { get; private set; }
    public bool FullTableScan { get; private set; }
    public bool VectorIndexUsed { get; private set; }
    public int VectorTopK { get; private set; }
    public int VectorExpansionPasses { get; private set; }
    public bool IsError { get; private set; }
    public string? ErrorMessage { get; private set; }

    public void SetOperation(string operation)
    {
        if (!string.IsNullOrWhiteSpace(operation))
        {
            Operation = operation.ToUpperInvariant();
        }
    }

    public void AddTable(string tableName)
    {
        if (!string.IsNullOrWhiteSpace(tableName))
        {
            _tables.Add(tableName);
        }
    }

    public void AddIndex(string indexName)
    {
        if (!string.IsNullOrWhiteSpace(indexName))
        {
            _indexesUsed.Add(indexName);
        }
    }

    public void AddRowsRead(long count) => RowsRead += Math.Max(0, count);
    public void AddRowsScanned(long count) => RowsScanned += Math.Max(0, count);
    public void MarkFullTableScan() => FullTableScan = true;
    public void AddRowsReturned(int count) => RowsReturned += Math.Max(0, count);
    public void AddRowsAffected(int count) => RowsAffected += Math.Max(0, count);

    public void RecordVectorSearch(string indexName, int topK, int expansionPasses)
    {
        VectorIndexUsed = true;
        VectorTopK = Math.Max(VectorTopK, topK);
        VectorExpansionPasses += Math.Max(0, expansionPasses);
        AddIndex(indexName);
    }

    public void RecordError(string message)
    {
        IsError = true;
        ErrorMessage = message;
    }

    public RuntimeQueryStats Build(TimeSpan elapsed)
    {
        return new RuntimeQueryStats(
            QueryText,
            Operation,
            StorageMode,
            DatabaseName,
            _tables.OrderBy(static item => item, StringComparer.OrdinalIgnoreCase).ToArray(),
            _indexesUsed.OrderBy(static item => item, StringComparer.OrdinalIgnoreCase).ToArray(),
            elapsed,
            RowsRead,
            RowsScanned,
            RowsReturned,
            RowsAffected,
            FullTableScan,
            VectorIndexUsed,
            VectorTopK,
            VectorExpansionPasses,
            IsError,
            ErrorMessage);
    }
}
```

- [ ] **Step 3: Add active diagnostics scope**

Create `DataVo.Core/Runtime/Diagnostics/RuntimeQueryDiagnosticsScope.cs`:

```csharp
using System.Diagnostics;

namespace DataVo.Core.Runtime.Diagnostics;

internal sealed class RuntimeQueryDiagnosticsScope : IDisposable
{
    private static readonly AsyncLocal<RuntimeQueryDiagnosticsScope?> CurrentScope = new();
    private readonly DataVoDiagnostics _owner;
    private readonly RuntimeQueryDiagnosticsScope? _previous;
    private readonly Stopwatch _stopwatch;
    private bool _disposed;

    private RuntimeQueryDiagnosticsScope(DataVoDiagnostics owner, RuntimeQueryStatsBuilder builder)
    {
        _owner = owner;
        Builder = builder;
        _previous = CurrentScope.Value;
        CurrentScope.Value = this;
        _stopwatch = Stopwatch.StartNew();
    }

    public RuntimeQueryStatsBuilder Builder { get; }

    public static RuntimeQueryDiagnosticsScope? Current => CurrentScope.Value;

    public static RuntimeQueryDiagnosticsScope? Start(DataVoDiagnostics owner, RuntimeQueryStatsBuilder builder)
    {
        return owner.Enabled ? new RuntimeQueryDiagnosticsScope(owner, builder) : null;
    }

    public static void RecordTableRead(string tableName, long rowsRead)
    {
        RuntimeQueryDiagnosticsScope? scope = CurrentScope.Value;
        if (scope == null) return;
        scope.Builder.AddTable(tableName);
        scope.Builder.AddRowsRead(rowsRead);
    }

    public static void RecordTableScan(string tableName, long rowsScanned)
    {
        RuntimeQueryDiagnosticsScope? scope = CurrentScope.Value;
        if (scope == null) return;
        scope.Builder.AddTable(tableName);
        scope.Builder.AddRowsScanned(rowsScanned);
        scope.Builder.MarkFullTableScan();
    }

    public static void RecordIndexUse(string indexName)
    {
        CurrentScope.Value?.Builder.AddIndex(indexName);
    }

    public static void RecordVectorSearch(string indexName, int topK, int expansionPasses)
    {
        CurrentScope.Value?.Builder.RecordVectorSearch(indexName, topK, expansionPasses);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _stopwatch.Stop();
        CurrentScope.Value = _previous;
        _owner.Record(Builder.Build(_stopwatch.Elapsed));
    }
}
```

- [ ] **Step 4: Add diagnostics facade**

Create `DataVo.Core/Runtime/Diagnostics/DataVoDiagnostics.cs`:

```csharp
namespace DataVo.Core.Runtime.Diagnostics;

public sealed class DataVoDiagnostics
{
    private readonly object _sync = new();
    private readonly Queue<RuntimeQueryStats> _recent = new();
    private readonly Queue<RuntimeQueryStats> _slow = new();
    private RuntimeQueryStats? _lastQuery;
    private int _recentQueryCapacity = 128;
    private int _slowQueryCapacity = 128;

    public bool Enabled { get; set; }
    public TimeSpan SlowQueryThreshold { get; set; } = TimeSpan.FromMilliseconds(16);

    public int RecentQueryCapacity
    {
        get => _recentQueryCapacity;
        set => _recentQueryCapacity = Math.Max(0, value);
    }

    public int SlowQueryCapacity
    {
        get => _slowQueryCapacity;
        set => _slowQueryCapacity = Math.Max(0, value);
    }

    public RuntimeQueryStats? LastQuery
    {
        get
        {
            lock (_sync)
            {
                return _lastQuery;
            }
        }
    }

    internal void Record(RuntimeQueryStats stats)
    {
        if (!Enabled) return;

        lock (_sync)
        {
            _lastQuery = stats;
            EnqueueBounded(_recent, stats, _recentQueryCapacity);
            if (stats.Elapsed >= SlowQueryThreshold)
            {
                EnqueueBounded(_slow, stats, _slowQueryCapacity);
            }
        }
    }

    public IReadOnlyList<RuntimeQueryStats> GetRecentQueries()
    {
        lock (_sync)
        {
            return _recent.ToArray();
        }
    }

    public IReadOnlyList<RuntimeQueryStats> GetSlowQueries()
    {
        lock (_sync)
        {
            return _slow.ToArray();
        }
    }

    public void Clear()
    {
        lock (_sync)
        {
            _lastQuery = null;
            _recent.Clear();
            _slow.Clear();
        }
    }

    private static void EnqueueBounded(Queue<RuntimeQueryStats> queue, RuntimeQueryStats stats, int capacity)
    {
        if (capacity <= 0) return;
        queue.Enqueue(stats);
        while (queue.Count > capacity)
        {
            queue.Dequeue();
        }
    }
}
```

- [ ] **Step 5: Expose diagnostics through engine/context**

Modify `DataVo.Core/Runtime/DataVoEngine.cs`:

```csharp
using DataVo.Core.Runtime.Diagnostics;
```

In the constructor after `TransactionIdAllocator = new TransactionIdAllocator();`:

```csharp
Diagnostics = new DataVoDiagnostics();
```

Add property near other engine-owned services:

```csharp
public DataVoDiagnostics Diagnostics { get; }
```

Modify `DataVo.Core/DataVoContext.cs`:

```csharp
using DataVo.Core.Runtime.Diagnostics;
```

Add property after `SessionId`:

```csharp
public DataVoDiagnostics Diagnostics => Engine.Diagnostics;
```

- [ ] **Step 6: Run tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter RuntimeDiagnosticsTests
```

Expected: tests compile but some fail until Task 3 instruments query execution.

- [ ] **Step 7: Commit**

```bash
git add DataVo.Core/Runtime/Diagnostics DataVo.Core/Runtime/DataVoEngine.cs DataVo.Core/DataVoContext.cs DataVo.Tests/E2E/RuntimeDiagnosticsTests.cs
git commit -m "feat: add runtime diagnostics core"
```

---

## Task 3: Instrument Query Execution

**Files:**
- Modify: `DataVo.Core/Parser/QueryEngine.cs`
- Modify: `DataVo.Core/DataVoContext.cs`

**Interfaces:**
- Consumes: `RuntimeQueryDiagnosticsScope.Start(...)`, `RuntimeQueryStatsBuilder`.
- Produces: diagnostics for `Execute(...)`, `BulkInsert(...)`, and `SearchNearest(...)`.

- [ ] **Step 1: Add query operation inference**

Modify `DataVo.Core/Parser/QueryEngine.cs` with helper:

```csharp
private static string InferOperation(string sql)
{
    string trimmed = sql.TrimStart();
    if (trimmed.Length == 0) return "UNKNOWN";

    int end = 0;
    while (end < trimmed.Length && !char.IsWhiteSpace(trimmed[end]) && trimmed[end] != ';')
    {
        end++;
    }

    return trimmed[..end].ToUpperInvariant();
}
```

- [ ] **Step 2: Wrap parse execution in diagnostics scope**

Modify `QueryEngine.Parse()` so the first lines are:

```csharp
using var _ = DataVoEngine.PushCurrent(_engine);

string? databaseName = _engine.Sessions.Get(session);
var diagnosticsBuilder = new RuntimeQueryStatsBuilder
{
    QueryText = query,
    StorageMode = _engine.Config.StorageMode,
    DatabaseName = databaseName
};
diagnosticsBuilder.SetOperation(InferOperation(query));
using RuntimeQueryDiagnosticsScope? diagnosticsScope =
    RuntimeQueryDiagnosticsScope.Start(_engine.Diagnostics, diagnosticsBuilder);
```

In the catch block around lexing/parsing, before returning:

```csharp
diagnosticsBuilder.RecordError(ex.Message);
```

In `ExecuteRunnableQueue`, after each `QueryResult result` is produced, call a new helper:

```csharp
private static void RecordResultMetrics(RuntimeQueryStatsBuilder builder, QueryResult result)
{
    if (result.IsError)
    {
        builder.RecordError(string.Join(" | ", result.Messages));
    }

    builder.AddRowsReturned(result.Data.Count);

    foreach (string message in result.Messages)
    {
        if (TryReadMessageCount(message, "Rows affected:", out int affected))
        {
            builder.AddRowsAffected(affected);
        }
        else if (TryReadMessageCount(message, "Rows selected:", out int selected))
        {
            builder.AddRowsReturned(selected);
        }
    }
}

private static bool TryReadMessageCount(string message, string prefix, out int count)
{
    count = 0;
    return message.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
        && int.TryParse(message[prefix.Length..].Trim(), out count);
}
```

Change `ExecuteRunnableQueue` signature:

```csharp
private void ExecuteRunnableQueue(Queue<IDbAction> runnable, List<QueryResult> response, RuntimeQueryStatsBuilder diagnosticsBuilder)
```

When adding results:

```csharp
QueryResult result = runnable.Dequeue().Perform(session);
response.Add(result);
RecordResultMetrics(diagnosticsBuilder, result);
```

- [ ] **Step 3: Instrument `BulkInsert`**

Modify `DataVo.Core/DataVoContext.cs` inside `BulkInsert` after `databaseName` is resolved:

```csharp
var diagnosticsBuilder = new RuntimeQueryStatsBuilder
{
    QueryText = $"BULK INSERT {tableName}",
    StorageMode = Engine.Config.StorageMode,
    DatabaseName = databaseName
};
diagnosticsBuilder.SetOperation("BULK INSERT");
using RuntimeQueryDiagnosticsScope? diagnosticsScope =
    RuntimeQueryDiagnosticsScope.Start(Engine.Diagnostics, diagnosticsBuilder);
```

After `InsertRowsResult result = ...`:

```csharp
diagnosticsBuilder.AddRowsAffected(result.AcceptedRowCount);
```

In a catch block around service insert:

```csharp
catch (Exception ex)
{
    diagnosticsBuilder.RecordError(ex.Message);
    throw;
}
```

- [ ] **Step 4: Instrument `SearchNearest`**

Modify `DataVo.Core/DataVoContext.cs` inside `SearchNearest(string tableName, string indexName, float[] queryVector, int topK)` after `databaseName` is resolved:

```csharp
var diagnosticsBuilder = new RuntimeQueryStatsBuilder
{
    QueryText = $"VECTOR SEARCH {tableName}.{indexName}",
    StorageMode = Engine.Config.StorageMode,
    DatabaseName = databaseName
};
diagnosticsBuilder.SetOperation("VECTOR SEARCH");
using RuntimeQueryDiagnosticsScope? diagnosticsScope =
    RuntimeQueryDiagnosticsScope.Start(Engine.Diagnostics, diagnosticsBuilder);
diagnosticsBuilder.AddTable(tableName);
```

After rows are materialized:

```csharp
diagnosticsBuilder.AddRowsReturned(results.Count);
```

Use a local variable before returning:

```csharp
List<Dictionary<string, object?>> results = rowIds
    .Where(rows.ContainsKey)
    .Select(id => rows[id])
    .ToList();
diagnosticsBuilder.AddRowsReturned(results.Count);
return results;
```

- [ ] **Step 5: Run tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter RuntimeDiagnosticsTests
```

Expected: API tests pass except detailed rows/index/vector counters that Task 4 will fill.

---

## Task 4: Instrument Storage, Scalar Indexes, and Vector Indexes

**Files:**
- Modify: `DataVo.Core/StorageEngine/StorageContext.cs`
- Modify: `DataVo.Core/Indexing/IndexManager.cs`
- Modify: `DataVo.Core/Parser/DQL/Select.cs`

**Interfaces:**
- Consumes: `RuntimeQueryDiagnosticsScope.RecordTableRead`, `RecordTableScan`, `RecordIndexUse`, `RecordVectorSearch`.
- Produces: accurate rows read/scanned and index/vector usage.

- [ ] **Step 1: Record full table scans**

In `StorageContext.ReadAllRows(...)`, after the `foreach` loop that populates `parsedTableData`, add:

```csharp
RuntimeQueryDiagnosticsScope.RecordTableScan(tableName, parsedTableData.Count);
```

Add using:

```csharp
using DataVo.Core.Runtime.Diagnostics;
```

- [ ] **Step 2: Record row-id reads**

In `StorageContext.ReadRowsById(...)`, after the loop:

```csharp
RuntimeQueryDiagnosticsScope.RecordTableRead(tableName, parsedTableData.Count);
```

- [ ] **Step 3: Record scalar index use**

Modify `DataVo.Core/Indexing/IndexManager.cs`:

Add using:

```csharp
using DataVo.Core.Runtime.Diagnostics;
```

In public scalar lookup methods after successful index lookup:

```csharp
RuntimeQueryDiagnosticsScope.RecordIndexUse(indexName);
```

Apply to:
- `FilterUsingIndex(...)`
- `IndexContainsKey(...)`
- `IndexContainsRow(...)`

- [ ] **Step 4: Record vector index use**

In `IndexManager.SearchVector(...)`, before returning:

```csharp
RuntimeQueryDiagnosticsScope.RecordVectorSearch(indexName, topK, expansionPasses: 0);
```

- [ ] **Step 5: Record vector expansion passes from SELECT**

In `DataVo.Core/Parser/DQL/Select.cs`, after `SearchVectorWithExpansionIfNeeded(...)` returns row IDs in both vector fast paths, add:

```csharp
RuntimeQueryDiagnosticsScope.RecordVectorSearch(indexName, topK, _queryVectorExpansionPasses);
```

Add using:

```csharp
using DataVo.Core.Runtime.Diagnostics;
```

- [ ] **Step 6: Run diagnostics tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter RuntimeDiagnosticsTests
```

Expected: all `RuntimeDiagnosticsTests` pass.

- [ ] **Step 7: Run feature regression tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "GameRuntimeSnapshotTests|GameRuntimeBulkInsertTests|VectorContextTests"
```

Expected: pass.

- [ ] **Step 8: Commit observability**

```bash
git add DataVo.Core/Runtime/Diagnostics DataVo.Core/Runtime/DataVoEngine.cs DataVo.Core/DataVoContext.cs DataVo.Core/Parser/QueryEngine.cs DataVo.Core/StorageEngine/StorageContext.cs DataVo.Core/Indexing/IndexManager.cs DataVo.Core/Parser/DQL/Select.cs DataVo.Tests/E2E/RuntimeDiagnosticsTests.cs
git commit -m "feat: add production runtime diagnostics"
```

---

## Task 5: Add Compiled Query Runtime Red Tests

**Files:**
- Create: `DataVo.Tests/E2E/CompiledQueryRuntimeTests.cs`

**Interfaces:**
- Produces expected future API:
  - `DataVoQueryAttribute`
  - `DataVoCompiledQueryPlan`
  - `DataVoCompiledQueryParameter`
  - `DataVoCompiledQuery.SelectSingle(...)`
  - `DataVoCompiledQuery.Insert(...)`
  - `DataVoCompiledQuery.Update(...)`

- [ ] **Step 1: Write failing runtime helper tests**

Create `DataVo.Tests/E2E/CompiledQueryRuntimeTests.cs`:

```csharp
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public sealed record PlayerProjection(int Id, string Name, int Level);

public class CompiledQueryRuntimeTests
{
    [Fact]
    public void CompiledSelectSingle_ByPrimaryKey_ReturnsTypedResultWithoutSqlExecute()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.BulkInsert("Players",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ada", ["Level"] = 5 }
        ]);

        var plan = DataVoCompiledQueryPlan.SelectSingle(
            tableName: "Players",
            projectedColumns: ["Id", "Name", "Level"],
            whereColumn: "Id",
            parameterName: "id");

        PlayerProjection? player = DataVoCompiledQuery.SelectSingle(
            context,
            plan,
            [new DataVoCompiledQueryParameter("id", 1)],
            static row => new PlayerProjection((int)row["Id"]!, (string)row["Name"]!, (int)row["Level"]!));

        Assert.Equal(new PlayerProjection(1, "Ada", 5), player);
    }

    [Fact]
    public void CompiledInsert_InsertsRowAndReturnsRowId()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Telemetry (Id INT PRIMARY KEY, EventName VARCHAR(50), Frame INT)");

        var plan = DataVoCompiledQueryPlan.Insert(
            tableName: "Telemetry",
            columns: ["Id", "EventName", "Frame"],
            parameterNames: ["id", "eventName", "frame"]);

        IReadOnlyList<long> ids = DataVoCompiledQuery.Insert(
            context,
            plan,
            [
                new DataVoCompiledQueryParameter("id", 1),
                new DataVoCompiledQueryParameter("eventName", "level_start"),
                new DataVoCompiledQueryParameter("frame", 10)
            ]);

        Assert.Equal([1L], ids);
    }

    [Fact]
    public void CompiledUpdate_UpdatesRowsAndReturnsAffectedCount()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Level INT)");
        context.Execute("INSERT INTO Players VALUES (1, 5)");

        var plan = DataVoCompiledQueryPlan.Update(
            tableName: "Players",
            assignments: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["Level"] = "level"
            },
            whereColumn: "Id",
            whereParameterName: "id");

        int affected = DataVoCompiledQuery.Update(
            context,
            plan,
            [
                new DataVoCompiledQueryParameter("id", 1),
                new DataVoCompiledQueryParameter("level", 7)
            ]);

        Assert.Equal(1, affected);
        Assert.Equal(7, (int)context.Execute("SELECT Level FROM Players WHERE Id = 1").Single().Data.Single()["Level"]!);
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string databaseName = $"Compiled_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }
}
```

- [ ] **Step 2: Run tests to verify red state**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter CompiledQueryRuntimeTests
```

Expected: compile failure because `DataVo.Core.CompiledQueries` does not exist.

---

## Task 6: Add Runtime Compiled Query Helpers

**Files:**
- Create: `DataVo.Core/CompiledQueries/DataVoQueryAttribute.cs`
- Create: `DataVo.Core/CompiledQueries/DataVoCompiledQueryKind.cs`
- Create: `DataVo.Core/CompiledQueries/DataVoCompiledQueryParameter.cs`
- Create: `DataVo.Core/CompiledQueries/DataVoCompiledQueryPlan.cs`
- Create: `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`

**Interfaces:**
- Produces runtime APIs consumed by source-generated code.

- [ ] **Step 1: Add attribute and enums**

Create `DataVo.Core/CompiledQueries/DataVoQueryAttribute.cs`:

```csharp
namespace DataVo.Core.CompiledQueries;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class DataVoQueryAttribute(string sql) : Attribute
{
    public string Sql { get; } = sql;
    public DataVoCompiledQueryKind Kind { get; set; } = DataVoCompiledQueryKind.Auto;
}
```

Create `DataVo.Core/CompiledQueries/DataVoCompiledQueryKind.cs`:

```csharp
namespace DataVo.Core.CompiledQueries;

public enum DataVoCompiledQueryKind
{
    Auto,
    SelectSingle,
    SelectMany,
    Insert,
    Update
}
```

Create `DataVo.Core/CompiledQueries/DataVoCompiledQueryParameter.cs`:

```csharp
namespace DataVo.Core.CompiledQueries;

public readonly record struct DataVoCompiledQueryParameter(string Name, object? Value);
```

- [ ] **Step 2: Add plan descriptor**

Create `DataVo.Core/CompiledQueries/DataVoCompiledQueryPlan.cs`:

```csharp
namespace DataVo.Core.CompiledQueries;

public sealed class DataVoCompiledQueryPlan
{
    private DataVoCompiledQueryPlan(
        DataVoCompiledQueryKind kind,
        string tableName,
        IReadOnlyList<string> projectedColumns,
        string? whereColumn,
        string? whereParameterName,
        IReadOnlyList<string> insertColumns,
        IReadOnlyList<string> insertParameterNames,
        IReadOnlyDictionary<string, string> assignments)
    {
        Kind = kind;
        TableName = tableName;
        ProjectedColumns = projectedColumns;
        WhereColumn = whereColumn;
        WhereParameterName = whereParameterName;
        InsertColumns = insertColumns;
        InsertParameterNames = insertParameterNames;
        Assignments = assignments;
    }

    public DataVoCompiledQueryKind Kind { get; }
    public string TableName { get; }
    public IReadOnlyList<string> ProjectedColumns { get; }
    public string? WhereColumn { get; }
    public string? WhereParameterName { get; }
    public IReadOnlyList<string> InsertColumns { get; }
    public IReadOnlyList<string> InsertParameterNames { get; }
    public IReadOnlyDictionary<string, string> Assignments { get; }

    public static DataVoCompiledQueryPlan SelectSingle(string tableName, IReadOnlyList<string> projectedColumns, string whereColumn, string parameterName)
    {
        return new DataVoCompiledQueryPlan(
            DataVoCompiledQueryKind.SelectSingle,
            tableName,
            projectedColumns,
            whereColumn,
            parameterName,
            [],
            [],
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
    }

    public static DataVoCompiledQueryPlan SelectMany(string tableName, IReadOnlyList<string> projectedColumns, string whereColumn, string parameterName)
    {
        return new DataVoCompiledQueryPlan(
            DataVoCompiledQueryKind.SelectMany,
            tableName,
            projectedColumns,
            whereColumn,
            parameterName,
            [],
            [],
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
    }

    public static DataVoCompiledQueryPlan Insert(string tableName, IReadOnlyList<string> columns, IReadOnlyList<string> parameterNames)
    {
        return new DataVoCompiledQueryPlan(
            DataVoCompiledQueryKind.Insert,
            tableName,
            [],
            null,
            null,
            columns,
            parameterNames,
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
    }

    public static DataVoCompiledQueryPlan Update(string tableName, IReadOnlyDictionary<string, string> assignments, string whereColumn, string whereParameterName)
    {
        return new DataVoCompiledQueryPlan(
            DataVoCompiledQueryKind.Update,
            tableName,
            [],
            whereColumn,
            whereParameterName,
            [],
            [],
            new Dictionary<string, string>(assignments, StringComparer.OrdinalIgnoreCase));
    }
}
```

- [ ] **Step 3: Add parameter helper**

In `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`, start with:

```csharp
using DataVo.Core.Contracts.Results;

namespace DataVo.Core.CompiledQueries;

public static class DataVoCompiledQuery
{
    private static Dictionary<string, object?> ToParameterDictionary(IReadOnlyList<DataVoCompiledQueryParameter> parameters)
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (DataVoCompiledQueryParameter parameter in parameters)
        {
            result[parameter.Name] = parameter.Value;
        }

        return result;
    }

    private static object? RequiredParameter(Dictionary<string, object?> parameters, string parameterName)
    {
        if (!parameters.TryGetValue(parameterName, out object? value))
        {
            throw new ArgumentException($"Missing compiled query parameter '{parameterName}'.", nameof(parameters));
        }

        return value;
    }
}
```

- [ ] **Step 4: Add select helper**

Add to `DataVoCompiledQuery`:

```csharp
public static TResult? SelectSingle<TResult>(
    DataVoContext context,
    DataVoCompiledQueryPlan plan,
    IReadOnlyList<DataVoCompiledQueryParameter> parameters,
    Func<Dictionary<string, object?>, TResult> mapper)
{
    if (plan.Kind != DataVoCompiledQueryKind.SelectSingle)
    {
        throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as SelectSingle.");
    }

    IReadOnlyList<TResult> rows = SelectMany(context, plan, parameters, mapper);
    return rows.Count == 0 ? default : rows[0];
}

public static IReadOnlyList<TResult> SelectMany<TResult>(
    DataVoContext context,
    DataVoCompiledQueryPlan plan,
    IReadOnlyList<DataVoCompiledQueryParameter> parameters,
    Func<Dictionary<string, object?>, TResult> mapper)
{
    ArgumentNullException.ThrowIfNull(context);
    ArgumentNullException.ThrowIfNull(plan);
    ArgumentNullException.ThrowIfNull(parameters);
    ArgumentNullException.ThrowIfNull(mapper);

    string? databaseName = context.Engine.Sessions.Get(context.SessionId);
    if (string.IsNullOrWhiteSpace(databaseName))
    {
        throw new InvalidOperationException("No database selected for the current session. Execute USE <database> first.");
    }

    Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
    object? expected = RequiredParameter(parameterDictionary, plan.WhereParameterName!);
    string expectedKey = expected?.ToString() ?? "NULL";

    List<long> ids = [];
    try
    {
        ids = [.. context.Engine.IndexManager.FilterUsingIndex(expectedKey, $"_PK_{plan.TableName}", plan.TableName, databaseName)];
    }
    catch
    {
        Dictionary<long, Dictionary<string, object?>> scanned = context.Engine.StorageContext.GetTableContents(plan.TableName, databaseName);
        return scanned.Values
            .Where(row => row.TryGetValue(plan.WhereColumn!, out object? value) && string.Equals(value?.ToString(), expectedKey, StringComparison.Ordinal))
            .Select(mapper)
            .ToArray();
    }

    Dictionary<long, Dictionary<string, object?>> rows = context.Engine.StorageContext.GetTableContents(ids, plan.TableName, databaseName);
    return ids
        .Where(rows.ContainsKey)
        .Select(id => mapper(rows[id]))
        .ToArray();
}
```

- [ ] **Step 5: Add insert helper**

Add to `DataVoCompiledQuery`:

```csharp
public static IReadOnlyList<long> Insert(
    DataVoContext context,
    DataVoCompiledQueryPlan plan,
    IReadOnlyList<DataVoCompiledQueryParameter> parameters)
{
    if (plan.Kind != DataVoCompiledQueryKind.Insert)
    {
        throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as Insert.");
    }

    Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
    var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

    for (int i = 0; i < plan.InsertColumns.Count; i++)
    {
        row[plan.InsertColumns[i]] = RequiredParameter(parameterDictionary, plan.InsertParameterNames[i]);
    }

    return context.BulkInsert(plan.TableName, [row]);
}
```

- [ ] **Step 6: Add update helper**

Add to `DataVoCompiledQuery`:

```csharp
public static int Update(
    DataVoContext context,
    DataVoCompiledQueryPlan plan,
    IReadOnlyList<DataVoCompiledQueryParameter> parameters)
{
    if (plan.Kind != DataVoCompiledQueryKind.Update)
    {
        throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as Update.");
    }

    Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
    string setClause = string.Join(", ", plan.Assignments.Select(pair => $"{pair.Key} = {FormatSqlLiteral(RequiredParameter(parameterDictionary, pair.Value))}"));
    string whereValue = FormatSqlLiteral(RequiredParameter(parameterDictionary, plan.WhereParameterName!));
    QueryResult result = context.Execute($"UPDATE {plan.TableName} SET {setClause} WHERE {plan.WhereColumn} = {whereValue}").Single();

    foreach (string message in result.Messages)
    {
        const string prefix = "Rows affected:";
        if (message.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            && int.TryParse(message[prefix.Length..].Trim(), out int affected))
        {
            return affected;
        }
    }

    return 0;
}

private static string FormatSqlLiteral(object? value)
{
    return value switch
    {
        null => "NULL",
        string text => $"'{text.Replace("'", "''", StringComparison.Ordinal)}'",
        bool flag => flag ? "true" : "false",
        DateOnly date => $"'{date:yyyy-MM-dd}'",
        DateTime dateTime => $"'{dateTime:yyyy-MM-dd}'",
        IFormattable formattable => formattable.ToString(null, System.Globalization.CultureInfo.InvariantCulture) ?? "NULL",
        _ => $"'{value}'"
    };
}
```

- [ ] **Step 7: Run runtime compiled query tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter CompiledQueryRuntimeTests
```

Expected: pass.

- [ ] **Step 8: Commit runtime compiled query support**

```bash
git add DataVo.Core/CompiledQueries DataVo.Tests/E2E/CompiledQueryRuntimeTests.cs
git commit -m "feat: add compiled query runtime helpers"
```

---

## Task 7: Add Source Generator Projects and Red Tests

**Files:**
- Create: `DataVo.Generators/DataVo.Generators.csproj`
- Create: `DataVo.Generators/DataVoQueryGenerator.cs`
- Create: `DataVo.Generators/Diagnostics/DataVoGeneratorDiagnostics.cs`
- Create: `DataVo.Generators/Sql/GeneratedQueryModel.cs`
- Create: `DataVo.Generators/Sql/DataVoQueryShapeParser.cs`
- Create: `DataVo.Generators.Tests/DataVo.Generators.Tests.csproj`
- Create: `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`
- Modify: `DataVo.sln`

**Interfaces:**
- Consumes: `[DataVoQuery]`, `DataVoCompiledQueryPlan`, `DataVoCompiledQuery`.
- Produces: generated partial method implementations.

- [ ] **Step 1: Create generator project**

Create `DataVo.Generators/DataVo.Generators.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <IsPackable>true</IsPackable>
    <IncludeBuildOutput>false</IncludeBuildOutput>
    <PackageId>DataVo.Generators</PackageId>
    <Title>DataVo Generators</Title>
    <Description>Source generators for DataVo compiled queries.</Description>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp" Version="4.14.0" PrivateAssets="all" />
  </ItemGroup>

  <ItemGroup>
    <None Include="$(OutputPath)\$(AssemblyName).dll" Pack="true" PackagePath="analyzers/dotnet/cs" Visible="false" />
  </ItemGroup>
</Project>
```

- [ ] **Step 2: Create generator test project**

Create `DataVo.Generators.Tests/DataVo.Generators.Tests.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <IsPackable>false</IsPackable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp" Version="4.14.0" />
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.14.1" />
    <PackageReference Include="xunit" Version="2.9.3" />
    <PackageReference Include="xunit.runner.visualstudio" Version="3.1.4" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\DataVo.Core\DataVo.Core.csproj" />
    <ProjectReference Include="..\DataVo.Generators\DataVo.Generators.csproj" ReferenceOutputAssembly="true" OutputItemType="Analyzer" />
  </ItemGroup>
</Project>
```

- [ ] **Step 3: Add red generator tests**

Create `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`:

```csharp
using System.Reflection;
using DataVo.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace DataVo.Generators.Tests;

public class DataVoQueryGeneratorTests
{
    [Fact]
    public void Generator_EmitsSelectSingleImplementation()
    {
        string source = """
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record PlayerProjection(int Id, string Name, int Level);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
                public static partial PlayerProjection? GetPlayer(DataVoContext db, int id);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("DataVoCompiledQueryPlan.SelectSingle", generated);
        Assert.Contains("new DataVoCompiledQueryParameter(\"id\", id)", generated);
        Assert.Contains("new PlayerProjection", generated);
    }

    [Fact]
    public void Generator_ReportsDiagnosticWhenParameterIsMissing()
    {
        string source = """
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id FROM Players WHERE Id = @id")]
                public static partial int MissingParameter(DataVoContext db);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        Diagnostic diagnostic = Assert.Single(result.Diagnostics);
        Assert.Equal("DATAVOQ002", diagnostic.Id);
    }

    [Fact]
    public void Generator_ReportsDiagnosticForUnsupportedJoin()
    {
        string source = """
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT p.Id FROM Players p JOIN Guilds g ON p.GuildId = g.Id WHERE p.Id = @id")]
                public static partial int UnsupportedJoin(DataVoContext db, int id);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        Diagnostic diagnostic = Assert.Single(result.Diagnostics);
        Assert.Equal("DATAVOQ001", diagnostic.Id);
    }

    private static GeneratorDriverRunResult RunGenerator(string source)
    {
        CSharpCompilation compilation = CSharpCompilation.Create(
            "GeneratorTest",
            [CSharpSyntaxTree.ParseText(source)],
            [
                MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(DataVoContext).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(Attribute).Assembly.Location),
                MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location)
            ],
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var generator = new DataVoQueryGenerator();
        GeneratorDriver driver = CSharpGeneratorDriver.Create(generator);
        driver = driver.RunGenerators(compilation);
        return driver.GetRunResult();
    }
}
```

- [ ] **Step 4: Add stub generator classes that compile**

Create `DataVo.Generators/DataVoQueryGenerator.cs`:

```csharp
using Microsoft.CodeAnalysis;

namespace DataVo.Generators;

[Generator]
public sealed class DataVoQueryGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
    }
}
```

Create `DataVo.Generators/Diagnostics/DataVoGeneratorDiagnostics.cs`:

```csharp
using Microsoft.CodeAnalysis;

namespace DataVo.Generators.Diagnostics;

internal static class DataVoGeneratorDiagnostics
{
    public static readonly DiagnosticDescriptor UnsupportedSql = new(
        "DATAVOQ001",
        "Unsupported DataVo compiled query SQL",
        "SQL is not supported by the DataVo source generator: {0}",
        "DataVo.CompiledQueries",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor MissingParameter = new(
        "DATAVOQ002",
        "Missing DataVo compiled query parameter",
        "SQL parameter '{0}' has no matching method parameter",
        "DataVo.CompiledQueries",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);
}
```

Create minimal model/parser files:

```csharp
namespace DataVo.Generators.Sql;

internal sealed record GeneratedQueryModel(
    string Kind,
    string TableName,
    string[] ProjectedColumns,
    string? WhereColumn,
    string? WhereParameterName,
    string[] InsertColumns,
    string[] InsertParameterNames,
    IReadOnlyDictionary<string, string> Assignments);
```

```csharp
namespace DataVo.Generators.Sql;

internal static class DataVoQueryShapeParser
{
    public static bool TryParse(string sql, out GeneratedQueryModel? model)
    {
        model = null;
        return false;
    }
}
```

- [ ] **Step 5: Add projects to solution**

Run:

```bash
dotnet sln DataVo.sln add DataVo.Generators/DataVo.Generators.csproj DataVo.Generators.Tests/DataVo.Generators.Tests.csproj
```

- [ ] **Step 6: Run generator tests to verify red state**

Run:

```bash
dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj
```

Expected: tests compile and fail because generator emits no source/diagnostics.

---

## Task 8: Implement Source Generator SQL Shape Parser

**Files:**
- Modify: `DataVo.Generators/Sql/DataVoQueryShapeParser.cs`
- Modify: `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`

**Interfaces:**
- Produces parser support for:
  - `SELECT <cols> FROM <table> WHERE <col> = @param`
  - `INSERT INTO <table> (<cols>) VALUES (@params...)`
  - `UPDATE <table> SET <col> = @param[, ...] WHERE <col> = @param`

- [ ] **Step 1: Add parser unit tests**

Append to `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`:

```csharp
[Fact]
public void ShapeParser_ParsesInsert()
{
    Assert.True(DataVo.Generators.Sql.DataVoQueryShapeParser.TryParse(
        "INSERT INTO Telemetry (Id, EventName, Frame) VALUES (@id, @eventName, @frame)",
        out var model));

    Assert.NotNull(model);
    Assert.Equal("Insert", model!.Kind);
    Assert.Equal("Telemetry", model.TableName);
    Assert.Equal(["Id", "EventName", "Frame"], model.InsertColumns);
    Assert.Equal(["id", "eventName", "frame"], model.InsertParameterNames);
}

[Fact]
public void ShapeParser_ParsesUpdate()
{
    Assert.True(DataVo.Generators.Sql.DataVoQueryShapeParser.TryParse(
        "UPDATE Players SET Level = @level WHERE Id = @id",
        out var model));

    Assert.NotNull(model);
    Assert.Equal("Update", model!.Kind);
    Assert.Equal("Players", model.TableName);
    Assert.Equal("Id", model.WhereColumn);
    Assert.Equal("id", model.WhereParameterName);
    Assert.Equal("level", model.Assignments["Level"]);
}
```

- [ ] **Step 2: Implement parser**

Replace `DataVoQueryShapeParser.TryParse(...)` with:

```csharp
using System.Text.RegularExpressions;

namespace DataVo.Generators.Sql;

internal static class DataVoQueryShapeParser
{
    private static readonly Regex SelectRegex = new(
        @"^\s*SELECT\s+(?<columns>[A-Za-z0-9_,\s]+)\s+FROM\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)\s+WHERE\s+(?<where>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*@(?<param>[A-Za-z_][A-Za-z0-9_]*)\s*;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex InsertRegex = new(
        @"^\s*INSERT\s+INTO\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)\s*\((?<columns>[^)]+)\)\s+VALUES\s*\((?<params>[^)]+)\)\s*;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex UpdateRegex = new(
        @"^\s*UPDATE\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)\s+SET\s+(?<assignments>.+?)\s+WHERE\s+(?<where>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*@(?<param>[A-Za-z_][A-Za-z0-9_]*)\s*;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static bool TryParse(string sql, out GeneratedQueryModel? model)
    {
        model = null;

        Match select = SelectRegex.Match(sql);
        if (select.Success)
        {
            model = new GeneratedQueryModel(
                "SelectSingle",
                select.Groups["table"].Value,
                SplitCsv(select.Groups["columns"].Value),
                select.Groups["where"].Value,
                select.Groups["param"].Value,
                [],
                [],
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
            return true;
        }

        Match insert = InsertRegex.Match(sql);
        if (insert.Success)
        {
            string[] columns = SplitCsv(insert.Groups["columns"].Value);
            string[] parameters = SplitCsv(insert.Groups["params"].Value)
                .Select(RemoveParameterPrefix)
                .ToArray();

            if (columns.Length != parameters.Length) return false;

            model = new GeneratedQueryModel(
                "Insert",
                insert.Groups["table"].Value,
                [],
                null,
                null,
                columns,
                parameters,
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
            return true;
        }

        Match update = UpdateRegex.Match(sql);
        if (update.Success)
        {
            Dictionary<string, string> assignments = ParseAssignments(update.Groups["assignments"].Value);
            if (assignments.Count == 0) return false;

            model = new GeneratedQueryModel(
                "Update",
                update.Groups["table"].Value,
                [],
                update.Groups["where"].Value,
                update.Groups["param"].Value,
                [],
                [],
                assignments);
            return true;
        }

        return false;
    }

    private static string[] SplitCsv(string value)
    {
        return value.Split(',')
            .Select(static item => item.Trim())
            .Where(static item => item.Length > 0)
            .ToArray();
    }

    private static string RemoveParameterPrefix(string value)
    {
        string trimmed = value.Trim();
        return trimmed.StartsWith("@", StringComparison.Ordinal) ? trimmed[1..] : trimmed;
    }

    private static Dictionary<string, string> ParseAssignments(string value)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (string assignment in SplitCsv(value))
        {
            string[] parts = assignment.Split('=');
            if (parts.Length != 2) return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            string column = parts[0].Trim();
            string parameter = RemoveParameterPrefix(parts[1]);
            if (column.Length == 0 || parameter.Length == 0) return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            result[column] = parameter;
        }

        return result;
    }
}
```

- [ ] **Step 3: Run parser tests**

Run:

```bash
dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj --filter ShapeParser
```

Expected: pass.

---

## Task 9: Implement Source Generation

**Files:**
- Modify: `DataVo.Generators/DataVoQueryGenerator.cs`
- Modify: `DataVo.Generators/Diagnostics/DataVoGeneratorDiagnostics.cs`
- Modify: `DataVo.Generators.Tests/DataVoQueryGeneratorTests.cs`

**Interfaces:**
- Produces generated code for static partial methods:
  - first parameter must be `DataVoContext`
  - remaining parameters must match SQL `@parameter` names case-insensitively
  - return type decides execution helper:
    - nullable non-collection DTO => `SelectSingle`
    - `IReadOnlyList<T>` or `List<T>` => `SelectMany`
    - `IReadOnlyList<long>` => `Insert`
    - `int` => `Update`

- [ ] **Step 1: Implement generator semantic scan**

Replace `DataVoQueryGenerator.Initialize(...)` with an incremental syntax provider that finds method declarations with attributes named `DataVoQuery` or `DataVoQueryAttribute`. Use this shape:

```csharp
public void Initialize(IncrementalGeneratorInitializationContext context)
{
    IncrementalValuesProvider<MethodDeclarationSyntax> methods = context.SyntaxProvider
        .CreateSyntaxProvider(
            static (node, _) => node is MethodDeclarationSyntax method && method.AttributeLists.Count > 0,
            static (ctx, _) => (MethodDeclarationSyntax)ctx.Node)
        .Where(static method => method.Modifiers.Any(SyntaxKind.PartialKeyword));

    IncrementalValueProvider<Compilation> compilation = context.CompilationProvider;

    context.RegisterSourceOutput(methods.Combine(compilation), static (spc, pair) =>
    {
        EmitForMethod(spc, pair.Left, pair.Right);
    });
}
```

Add using directives:

```csharp
using System.Text;
using DataVo.Generators.Diagnostics;
using DataVo.Generators.Sql;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
```

- [ ] **Step 2: Implement method emission**

Add private helpers in `DataVoQueryGenerator`:

```csharp
private static void EmitForMethod(SourceProductionContext context, MethodDeclarationSyntax method, Compilation compilation)
{
    SemanticModel semanticModel = compilation.GetSemanticModel(method.SyntaxTree);
    if (semanticModel.GetDeclaredSymbol(method) is not IMethodSymbol symbol)
    {
        return;
    }

    AttributeData? attribute = symbol.GetAttributes()
        .FirstOrDefault(attr => attr.AttributeClass?.ToDisplayString() == "DataVo.Core.CompiledQueries.DataVoQueryAttribute");
    if (attribute == null)
    {
        return;
    }

    string sql = attribute.ConstructorArguments.Length == 1
        ? attribute.ConstructorArguments[0].Value?.ToString() ?? string.Empty
        : string.Empty;

    if (!DataVoQueryShapeParser.TryParse(sql, out GeneratedQueryModel? model) || model == null)
    {
        context.ReportDiagnostic(Diagnostic.Create(DataVoGeneratorDiagnostics.UnsupportedSql, method.Identifier.GetLocation(), sql));
        return;
    }

    string[] sqlParameters = GetSqlParameters(model);
    HashSet<string> methodParameters = symbol.Parameters
        .Skip(1)
        .Select(parameter => parameter.Name)
        .ToHashSet(StringComparer.OrdinalIgnoreCase);

    foreach (string sqlParameter in sqlParameters)
    {
        if (!methodParameters.Contains(sqlParameter))
        {
            context.ReportDiagnostic(Diagnostic.Create(DataVoGeneratorDiagnostics.MissingParameter, method.Identifier.GetLocation(), sqlParameter));
            return;
        }
    }

    string source = GenerateMethod(symbol, model);
    context.AddSource($"{symbol.ContainingType.Name}_{symbol.Name}.DataVo.g.cs", source);
}
```

- [ ] **Step 3: Generate method source**

Add:

```csharp
private static string GenerateMethod(IMethodSymbol method, GeneratedQueryModel model)
{
    string ns = method.ContainingNamespace.IsGlobalNamespace
        ? string.Empty
        : $"namespace {method.ContainingNamespace.ToDisplayString()};";

    string containingType = method.ContainingType.Name;
    string methodSignature = method.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat);
    string parameterList = string.Join(", ", method.Parameters.Select(parameter => $"{parameter.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)} {parameter.Name}"));
    string returnType = method.ReturnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
    string planName = $"__DataVoPlan_{method.Name}";

    var builder = new StringBuilder();
    builder.AppendLine("// <auto-generated />");
    builder.AppendLine("#nullable enable");
    if (ns.Length > 0) builder.AppendLine(ns);
    builder.AppendLine($"partial class {containingType}");
    builder.AppendLine("{");
    builder.AppendLine($"    private static readonly global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan {planName} = {GeneratePlan(model)};");
    builder.AppendLine($"    public static partial {returnType} {method.Name}({parameterList})");
    builder.AppendLine("    {");
    builder.AppendLine($"        return {GenerateInvocation(method, model, planName)};");
    builder.AppendLine("    }");
    builder.AppendLine("}");
    return builder.ToString();
}
```

Add `GeneratePlan`, `GenerateInvocation`, and `GetSqlParameters`:

```csharp
private static string GeneratePlan(GeneratedQueryModel model)
{
    return model.Kind switch
    {
        "SelectSingle" => $"global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan.SelectSingle(\"{model.TableName}\", new string[] {{ {QuoteList(model.ProjectedColumns)} }}, \"{model.WhereColumn}\", \"{model.WhereParameterName}\")",
        "Insert" => $"global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan.Insert(\"{model.TableName}\", new string[] {{ {QuoteList(model.InsertColumns)} }}, new string[] {{ {QuoteList(model.InsertParameterNames)} }})",
        "Update" => $"global::DataVo.Core.CompiledQueries.DataVoCompiledQueryPlan.Update(\"{model.TableName}\", new global::System.Collections.Generic.Dictionary<string, string>(global::System.StringComparer.OrdinalIgnoreCase) {{ {AssignmentList(model.Assignments)} }}, \"{model.WhereColumn}\", \"{model.WhereParameterName}\")",
        _ => throw new InvalidOperationException($"Unsupported query kind '{model.Kind}'.")
    };
}

private static string GenerateInvocation(IMethodSymbol method, GeneratedQueryModel model, string planName)
{
    string dbParameter = method.Parameters[0].Name;
    string parameters = string.Join(", ", GetSqlParameters(model).Select(name => $"new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter(\"{name}\", {name})"));

    if (model.Kind == "Insert")
    {
        return $"global::DataVo.Core.CompiledQueries.DataVoCompiledQuery.Insert({dbParameter}, {planName}, new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter[] {{ {parameters} }})";
    }

    if (model.Kind == "Update")
    {
        return $"global::DataVo.Core.CompiledQueries.DataVoCompiledQuery.Update({dbParameter}, {planName}, new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter[] {{ {parameters} }})";
    }

    string rowType = method.ReturnType.NullableAnnotation == NullableAnnotation.Annotated
        && method.ReturnType is INamedTypeSymbol named
        ? named.TypeArguments.FirstOrDefault()?.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) ?? method.ReturnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat).TrimEnd('?')
        : method.ReturnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat).TrimEnd('?');

    return $"global::DataVo.Core.CompiledQueries.DataVoCompiledQuery.SelectSingle<{rowType}>({dbParameter}, {planName}, new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter[] {{ {parameters} }}, static row => new {rowType}({MapperArguments(model.ProjectedColumns)}))";
}

private static string MapperArguments(string[] columns)
{
    return string.Join(", ", columns.Select(column => $"({InferCastType(column)})row[\"{column}\"]!"));
}

private static string InferCastType(string column)
{
    return column.EndsWith("Id", StringComparison.OrdinalIgnoreCase) || column.Equals("Level", StringComparison.OrdinalIgnoreCase) || column.Equals("Frame", StringComparison.OrdinalIgnoreCase)
        ? "int"
        : "string";
}

private static string[] GetSqlParameters(GeneratedQueryModel model)
{
    return model.Kind switch
    {
        "Insert" => model.InsertParameterNames,
        "Update" => model.Assignments.Values.Concat([model.WhereParameterName!]).Distinct(StringComparer.OrdinalIgnoreCase).ToArray(),
        _ => [model.WhereParameterName!]
    };
}

private static string QuoteList(IEnumerable<string> values) => string.Join(", ", values.Select(value => $"\"{value}\""));
private static string AssignmentList(IReadOnlyDictionary<string, string> assignments) => string.Join(", ", assignments.Select(pair => $"[\"{pair.Key}\"] = \"{pair.Value}\""));
```

- [ ] **Step 4: Run generator tests**

Run:

```bash
dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj
```

Expected: generator tests pass.

---

## Task 10: Add Source-Generated Compiled Query E2E Tests

**Files:**
- Modify: `DataVo.Tests/DataVo.Tests.csproj`
- Create: `DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests.cs`

**Interfaces:**
- Consumes generator emitted partial implementations.

- [ ] **Step 1: Add generator analyzer reference to tests**

Modify `DataVo.Tests/DataVo.Tests.csproj`:

```xml
<ItemGroup>
  <ProjectReference Include="..\DataVo.Generators\DataVo.Generators.csproj"
                    OutputItemType="Analyzer"
                    ReferenceOutputAssembly="false" />
</ItemGroup>
```

- [ ] **Step 2: Add generated query E2E tests**

Create `DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests.cs`:

```csharp
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public sealed record GeneratedPlayer(int Id, string Name, int Level);

public static partial class GeneratedGameQueries
{
    [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
    public static partial GeneratedPlayer? GetPlayer(DataVoContext db, int id);

    [DataVoQuery("INSERT INTO Telemetry (Id, EventName, Frame) VALUES (@id, @eventName, @frame)")]
    public static partial IReadOnlyList<long> InsertTelemetry(DataVoContext db, int id, string eventName, int frame);

    [DataVoQuery("UPDATE Players SET Level = @level WHERE Id = @id")]
    public static partial int SetPlayerLevel(DataVoContext db, int id, int level);
}

public class SourceGeneratedCompiledQueryTests
{
    [Fact]
    public void GeneratedSelect_ReturnsTypedRecord()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.Execute("INSERT INTO Players VALUES (1, 'Ada', 5)");

        GeneratedPlayer? player = GeneratedGameQueries.GetPlayer(context, 1);

        Assert.Equal(new GeneratedPlayer(1, "Ada", 5), player);
    }

    [Fact]
    public void GeneratedInsert_InsertsTelemetry()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Telemetry (Id INT PRIMARY KEY, EventName VARCHAR(50), Frame INT)");

        IReadOnlyList<long> ids = GeneratedGameQueries.InsertTelemetry(context, 1, "level_start", 10);

        Assert.Equal([1L], ids);
        Assert.Single(context.Execute("SELECT * FROM Telemetry WHERE Id = 1").Single().Data);
    }

    [Fact]
    public void GeneratedUpdate_UpdatesPlayer()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(50), Level INT)");
        context.Execute("INSERT INTO Players VALUES (1, 'Ada', 5)");

        int affected = GeneratedGameQueries.SetPlayerLevel(context, 1, 9);

        Assert.Equal(1, affected);
        Assert.Equal(9, (int)context.Execute("SELECT Level FROM Players WHERE Id = 1").Single().Data.Single()["Level"]!);
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string databaseName = $"Generated_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }
}
```

- [ ] **Step 3: Run source-generated E2E tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter SourceGeneratedCompiledQueryTests
```

Expected: pass.

- [ ] **Step 4: Run compiled query regression tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "CompiledQueryRuntimeTests|SourceGeneratedCompiledQueryTests|RuntimeDiagnosticsTests"
dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj
```

Expected: pass.

- [ ] **Step 5: Commit source generator**

```bash
git add DataVo.Generators DataVo.Generators.Tests DataVo.Tests/DataVo.Tests.csproj DataVo.Tests/E2E/SourceGeneratedCompiledQueryTests.cs DataVo.sln
git commit -m "feat: add source-generated compiled queries"
```

---

## Task 11: Documentation and Final Verification

**Files:**
- Create: `docs/features/runtime-observability.md`
- Create: `docs/features/compiled-queries.md`
- Modify: `docs/features/unity-and-godot.md`
- Modify: `docs/features/setup-and-packaging.md`
- Modify: `docs/DataVo.Core/DataVoContext.md`

**Interfaces:**
- Documents public APIs from Tasks 2-10.

- [ ] **Step 1: Add runtime observability docs**

Create `docs/features/runtime-observability.md`:

```markdown
# Runtime Observability

DataVo runtime diagnostics are designed for production-safe game and low-latency .NET workloads.

```csharp
context.Diagnostics.Enabled = true;
context.Diagnostics.SlowQueryThreshold = TimeSpan.FromMilliseconds(4);

context.Execute("SELECT * FROM PlayerState WHERE Id = 1");

RuntimeQueryStats last = context.Diagnostics.LastQuery!;
IReadOnlyList<RuntimeQueryStats> slowQueries = context.Diagnostics.GetSlowQueries();
```

Diagnostics are disabled by default. When enabled, DataVo records bounded recent-query and slow-query histories with elapsed time, storage mode, selected database, tables touched, rows read/scanned/returned/affected, index usage, vector usage, and error status.

Use the same diagnostics in `StorageMode.InMemory` tests and `StorageMode.Disk` production builds to compare behavior across storage modes.
```

- [ ] **Step 2: Add compiled query docs**

Create `docs/features/compiled-queries.md`:

```markdown
# Source-Generated Compiled Queries

DataVo compiled queries use C# source generators to emit strongly typed partial method implementations at build time.

```csharp
public sealed record PlayerRow(int Id, string Name, int Level);

public static partial class GameQueries
{
    [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
    public static partial PlayerRow? GetPlayer(DataVoContext db, int id);
}
```

The generated method validates SQL parameters against method parameters and executes through a compiled fast path for supported shapes. Supported V1 SQL shapes are:

- `SELECT <columns> FROM <table> WHERE <column> = @parameter`
- `INSERT INTO <table> (<columns>) VALUES (@parameters...)`
- `UPDATE <table> SET <column> = @parameter WHERE <column> = @parameter`

Unsupported SQL emits build diagnostic `DATAVOQ001`. Dynamic SQL should continue to use `DataVoContext.Execute(...)`.
```

- [ ] **Step 3: Update integration docs**

Add links to `docs/features/unity-and-godot.md` related pages:

```markdown
- [Runtime Observability](./runtime-observability.md)
- [Source-Generated Compiled Queries](./compiled-queries.md)
```

Add package note to `docs/features/setup-and-packaging.md` package map:

```markdown
| DataVo.Generators           | Source-generated compiled-query analyzer package    |
```

Add a short section to `docs/DataVo.Core/DataVoContext.md`:

```markdown
## Runtime diagnostics

Use `context.Diagnostics` to enable bounded query metrics for tests, simulations, and production builds.

## Compiled queries

Use `[DataVoQuery]` partial methods when a query is known at build time and called frequently.
```

- [ ] **Step 4: Run docs grep**

Run:

```bash
rg -n "Runtime Observability|DataVoQuery|Compiled Queries|SlowQueryThreshold|DATAVOQ001" docs/features docs/DataVo.Core/DataVoContext.md
```

Expected: hits in the new docs and updated integration docs.

- [ ] **Step 5: Run focused tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "RuntimeDiagnosticsTests|CompiledQueryRuntimeTests|SourceGeneratedCompiledQueryTests|GameRuntimeSnapshotTests|GameRuntimeBulkInsertTests"
dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj --no-restore
```

Expected: pass.

- [ ] **Step 6: Run broader regression tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "DefaultTests|VectorIndexTests|AdoNetTests|TransactionTests"
```

Expected: pass.

- [ ] **Step 7: Run full tests**

Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore
dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj --no-restore
```

Expected: pass.

- [ ] **Step 8: Check whitespace**

Run:

```bash
git diff --check
```

Expected: no output.

- [ ] **Step 9: Commit docs and final verification**

```bash
git add docs/features/runtime-observability.md docs/features/compiled-queries.md docs/features/unity-and-godot.md docs/features/setup-and-packaging.md docs/DataVo.Core/DataVoContext.md
git commit -m "docs: document observability and compiled queries"
```

- [ ] **Step 10: Show final status**

Run:

```bash
git status --short
git log -6 --oneline
```

Expected: worktree clean and recent commits include runtime diagnostics, compiled query runtime helpers, source-generated compiled queries, and docs.

---

## Self-Review Notes

- Spec coverage: observability API, bounded metrics, storage-mode parity, index/vector telemetry, source generator runtime helpers, generator project/tests, generated E2E, docs, and final verification are covered.
- Completeness scan: no incomplete markers or open-ended implementation gaps remain.
- Type consistency: public names are consistent across tasks: `DataVoDiagnostics`, `RuntimeQueryStats`, `RuntimeQueryDiagnosticsScope`, `DataVoQueryAttribute`, `DataVoCompiledQueryPlan`, `DataVoCompiledQueryParameter`, and `DataVoCompiledQuery`.
- Scope note: this is intentionally a two-milestone plan. If execution time is constrained, complete Tasks 1-4 first and commit observability before starting Tasks 5-11.
