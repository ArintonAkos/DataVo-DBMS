# Benchmark Methodology

> Source route: /manual/performance/methodology
> Source file: manual/performance/methodology.md

DataVo's benchmark methodology is designed to make the engine's tradeoffs visible. The harness does not try to prove that DataVo is better than mature databases in every setting. It isolates specific embedded workloads: point lookups, WAL-backed updates, LSM write behavior, vector search, reactive query maintenance, disk footprint, and recovery.

Every benchmark starts from an explicit scenario name. Run the benchmark host from the repository root and choose the scenario with `--scenario`.

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario thread-scaling --format markdown
```

The published results were collected on macOS arm64 / Apple Silicon with `.NET 10.0.103`. That hardware baseline matters. Storage engines are sensitive to CPU cache behavior, filesystem sync behavior, kernel buffering, and native extension performance.

Durability setting is recorded with the result because it changes the meaning of the number. Strict LSM waits for WAL fsync before acknowledging a write. Relaxed LSM acknowledges after the OS-buffered append path and can lose recent writes on power or kernel failure.

```csharp
using var strict = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./strict_lsm",
    LsmStrictFsync = true
});

using var relaxed = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./relaxed_lsm",
    LsmStrictFsync = false
});
```

The logical record workload uses a compact table shape so the benchmark measures storage and execution overhead instead of application complexity.

```sql
CREATE TABLE Records (
  Id INT PRIMARY KEY,
  Name VARCHAR(40),
  Value INT,
  Score FLOAT
);

SELECT Id, Name, Value, Score
FROM Records
WHERE Id = @id;

UPDATE Records
SET Value = @value, Score = @score
WHERE Id = @id;
```

The vector workload uses 1536-dimensional embeddings because that shape resembles real embedding applications more than toy three-dimensional vectors.

```sql
CREATE TABLE Vectors (
  Id INT PRIMARY KEY,
  Emb VECTOR(1536)
);

CREATE INDEX vidx ON Vectors (Emb) USING HNSW;
```

The charts are interactive so exact values are available on hover. Log scale is used only when a linear chart would hide smaller bars, such as allocation comparisons where one engine allocates orders of magnitude more than another.

## Methodology Summary

| Feature | Status | Notes |
| --- | --- | --- |
| Apple Silicon macOS baseline | Supported | Published results were collected in macOS arm64 / Apple Silicon context. |
| Benchmark host commands | Supported | Scenarios run through `demos/Research.Benchmark/src/Research.Benchmark.Host`. |
| Durability labels | Supported | Results distinguish strict LSM, relaxed LSM, SQLite WAL normal, and SQLite WAL full. |
| Interactive exact-value charts | Supported | Hover reveals exact numbers; log scale is used when needed for readability. |
| Workload SQL disclosure | Supported | Benchmark pages include representative schemas and query shapes. |
| Cross-machine universal ranking | Not Supported | Results depend on hardware, OS, storage mode, durability, indexes, and native extensions. |
