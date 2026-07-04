# Roadmap

The DataVo roadmap is deliberately conservative. The goal is not to promise every database feature. The near-term work is to stabilize the embedded API, storage format, NuGet packaging, and public documentation.

The current foundation is the embedded API. That is where DataVo should remain strongest: predictable local storage, clear configuration, and fast hot paths for application-owned workloads.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./app_data",
    LsmStrictFsync = true
});
```

The roadmap focuses on stabilizing the embedded API, storage format, and packaging. A developer should be able to install the right packages, run a quickstart, and understand which parts of the alpha are safe to evaluate.

```bash
dotnet add package DataVo.Core
dotnet add package DataVo.EntityFrameworkCore
dotnet add package DataVo.Generators
```

The SQL roadmap is about clarity before breadth. DataVo should keep adding coverage, but every new claim needs examples and tests. Unsupported SQL should fail clearly instead of looking half-supported.

```sql
CREATE TABLE Tasks (
  Id INT PRIMARY KEY,
  Title VARCHAR(120),
  Done BIT
);

CREATE INDEX ix_tasks_done ON Tasks (Done);

SELECT Id, Title
FROM Tasks
WHERE Done = false
ORDER BY Id ASC
LIMIT 50;
```

The tooling roadmap is separate. Developers will eventually want to inspect DataVo databases through DBeaver, DataGrip, or pgAdmin. That requires a `DataVo.Server` process and enough PostgreSQL-wire compatibility to connect, list tables, and run simple queries. That work is valuable, but it should not distract from stabilizing the embedded engine.

```text
Application process
  -> DataVoContext
  -> embedded storage

Future tooling process
  -> DataVo.Server
  -> PostgreSQL wire protocol subset
  -> embedded storage
```

## Roadmap Summary

| Feature | Status | Notes |
| --- | --- | --- |
| Curated public documentation | Supported | The v0.1 manual and AI export expose only public launch docs. |
| Core embedded API | Supported | `DataVoContext` remains the primary interface. |
| Vector search | Supported | HNSW, FLAT, cosine distance, and L2 distance are documented. |
| NuGet packages | Planned | Public package publishing, version policy, and README polish remain launch tasks. |
| SQL compatibility expansion | Planned | New SQL coverage should come with tests and explicit docs. |
| Storage-format stability | Planned | Requires compatibility tests and migration guidance. |
| PostgreSQL-wire tooling bridge | Planned | Future work for DBeaver/DataGrip/pgAdmin style browsing. |
| EF Core migrations | Not Supported | Requires provider services beyond the current bridge. |
| Replication and high availability | Not Supported | Out of scope for the embedded v0.1 engine. |
