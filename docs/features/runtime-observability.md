# Runtime Observability

Runtime diagnostics are opt-in. A new `DataVoContext` does not record query history until diagnostics are enabled.

## Enable diagnostics

Use the context diagnostics facade for direct embedding:

```csharp
using var context = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

context.Diagnostics.Enabled = true;
context.Diagnostics.SlowQueryThreshold = TimeSpan.FromMilliseconds(25);
```

Host builders can expose their own diagnostics settings and apply them to `context.Diagnostics` during startup. The runtime collector is the context-owned `DataVoDiagnostics` instance.

## Query stats

`RuntimeQueryStats` captures one executed SQL batch or direct context operation such as `BulkInsert` or `SearchNearest`.

| Field family | What it records |
| :----------- | :-------------- |
| Operation | query text, inferred operation, elapsed time |
| Location | storage mode and selected database |
| Tables | table names referenced during execution |
| Rows | rows read, scanned, returned, and affected |
| Indexes | indexes used and full-table-scan flag |
| Vectors | vector-index usage, top-k, and expansion passes |
| Errors | error state and error message |

`StorageMode.InMemory` and `StorageMode.Disk` both flow through the same diagnostics surface, so storage-mode parity can be checked from `RuntimeQueryStats.StorageMode`.

## Recent and slow histories

Diagnostics keep bounded histories:

```csharp
context.Diagnostics.RecentQueryCapacity = 128;
context.Diagnostics.SlowQueryCapacity = 128;
context.Diagnostics.SlowQueryThreshold = TimeSpan.FromMilliseconds(16);

RuntimeQueryStats? last = context.Diagnostics.LastQuery;
IReadOnlyList<RuntimeQueryStats> recent = context.Diagnostics.GetRecentQueries();
IReadOnlyList<RuntimeQueryStats> slow = context.Diagnostics.GetSlowQueries();
```

`GetRecentQueries()` and `GetSlowQueries()` return snapshots in oldest-to-newest order. Use `Clear()` to reset the last query and both histories.

## Related pages

- [Getting Started](./getting-started.md)
- [Setup and Packaging](./setup-and-packaging.md)
- [Vector Queries Guide](./vector-queries-guide.md)
