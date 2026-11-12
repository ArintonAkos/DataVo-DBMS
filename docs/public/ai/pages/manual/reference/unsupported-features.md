# Unsupported Features

> Source route: /manual/reference/unsupported-features
> Source file: manual/reference/unsupported-features.md

This page exists so adopters do not have to infer product boundaries from source files, tests, or roadmap notes. If a feature is listed here, treat it as unavailable in DataVo v0.2 Release Candidate (`v0.2.0-rc.3`) even if related internal work exists.

DataVo is not PostgreSQL-compatible in v0.2. You can write SQL, but it is DataVo's SQL dialect.

```sql
-- Supported DataVo-style vector query:
SELECT Id, Label, Emb <=> '[1,0,0]' AS distance
FROM Embeddings
ORDER BY distance ASC
LIMIT 5;
```

PostgreSQL system catalogs are not present. Tools that expect `pg_class`, `pg_attribute`, or PostgreSQL authentication cannot connect directly to the embedded engine.

```sql
-- Not a supported v0.2 DataVo catalog workflow:
SELECT relname
FROM pg_class
WHERE relkind = 'r';
```

EF Core support is an integration bridge, not a full standalone provider. Simple mapped `DbSet` workflows are supported, but migrations and advanced provider behavior are out of scope.

```csharp
// Supported direction: simple mapped entity query.
List<User> users = ctx.Users
    .Where(user => user.IsActive)
    .ToList();

// Not supported as a v0.2 claim: full migration lifecycle.
// dotnet ef migrations add InitialCreate
// dotnet ef database update
```

DataVo has transaction primitives, but it does not claim full serializable isolation for arbitrary multi-row predicates. Design evaluations around the documented isolation contract.

```sql
BEGIN TRANSACTION;

SELECT Id, Name
FROM Users
WHERE IsActive = true;

-- v0.2 does not claim complete phantom-read prevention
-- for arbitrary predicate ranges.

COMMIT;
```

There is no stored procedure or trigger runtime in v0.2. Keep application logic in application code.

```sql
-- Not supported in v0.2:
CREATE TRIGGER update_timestamp
BEFORE UPDATE ON Users
FOR EACH ROW
EXECUTE PROCEDURE set_updated_at();
```

Replication and high availability are also out of scope. DataVo's product shape is embedded local storage.

```text
Supported v0.2 shape:
  application -> DataVoContext -> local storage

Not supported v0.2 shape:
  primary server -> replica server -> failover orchestration
```

## Unsupported Features Summary

| Feature | Status | Notes |
| --- | --- | --- |
| Full PostgreSQL compatibility | Not Supported | DataVo has its own SQL dialect and no PostgreSQL server protocol in v0.2. |
| PostgreSQL system catalogs | Not Supported | No `pg_class`, `pg_attribute`, or related catalog compatibility layer. |
| EF Core migrations | Not Supported | No migration scaffolding, diffing, or apply pipeline. |
| EF Core shadow properties | Not Supported | The bridge expects explicit mapped CLR properties. |
| Complex EF LINQ joins | Not Supported | Broad join translation is outside the v0.2 claim. |
| Multi-row serializability | Not Supported | DataVo does not claim serializable isolation for arbitrary multi-row predicates. |
| Phantom-read prevention | Not Supported | No public predicate-range locking guarantee. |
| Stored procedures | Not Supported | No procedural SQL runtime. |
| Triggers | Not Supported | No trigger system. |
| Replication/high availability | Not Supported | Out of scope for embedded v0.2. |
| PostgreSQL-like role administration | Not Supported | Principal/grant commands exist, but no PostgreSQL role catalog is documented for v0.2. |
| Production WebAssembly / npm package | Not Supported | `DataVo.Browser` is an `Experimental / Playground Lane` runtime for docs only; not published on npm. |
