# Query Planner And Fast Paths

> Source route: /manual/storage-engine/query-planner-fast-paths
> Source file: manual/storage-engine/query-planner-fast-paths.md

DataVo has a general SQL path and a set of narrower fast paths. The rule is simple: a fast path must either produce the same result as the normal path or decline and fall back.

The general path is what you use first.

```sql
SELECT Id, Name, Score
FROM Users
WHERE Score > 90
ORDER BY Score DESC
LIMIT 10;
```

Source-generated queries remove repeated parse and mapping work for supported query patterns.

```csharp
public sealed record PlayerRow(int Id, string Name, int Level);

public static partial class PlayerQueries
{
    [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
    public static partial PlayerRow? GetPlayer(DataVoContext db, int id);
}
```

The fixed-width update fast path is intentionally narrow. It is for compiled single-row updates that can be found through an integer primary-key lane and patched safely.

```csharp
public static partial class PlayerWrites
{
    [DataVoQuery("UPDATE Players SET Level = @level WHERE Id = @id")]
    public static partial int UpdateLevel(DataVoContext db, int id, int level);
}
```

Vector predicate fast paths can use ANN prefiltering when the planner estimates that an index will reduce work.

```sql
SELECT Id, Label, Emb <=> '[1,0,0]' AS distance
FROM Embeddings
WHERE Emb <=> '[1,0,0]' < 0.25
ORDER BY distance ASC
LIMIT 10;
```

The Volcano planner settings are present for limited experiments. Leave them off unless you are specifically testing planner behavior.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory,
    EnableVolcanoExecution = false,
    EnableVolcanoSpillGuardrails = true
});
```

## Planner And Fast Path Support Summary

| Feature | Status | Notes |
| --- | --- | --- |
| General parser/executor | Supported | Default path for SQL execution through `DataVoContext.Execute`. |
| Source-generated SELECT plan | Supported | `[DataVoQuery]` emits static plans for narrow equality SELECT shapes. |
| Typed row reader | Supported | Eligible projections avoid dictionary materialization. |
| Single-column manifest index hint | Supported | Schema manifests can pre-resolve index hints. |
| Fixed-width compiled update | Supported | Narrow disk/LSM single-row update shapes only. |
| LSM MemTable write path | Supported | Arena-backed write path avoids much allocation in steady state. |
| Vector predicate fast path | Supported | ANN prefiltering is controlled by vector planner knobs. |
| Volcano execution | Planned | Present but disabled by default. |
| Arbitrary SQL fast path | Not Supported | Complex queries use the general runtime path. |
| Cost-based optimizer parity with PostgreSQL | Not Supported | Planner work is early and targeted. |
