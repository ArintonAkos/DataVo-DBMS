# Transactions and MVCC

DataVo's concurrency model is designed for embedded workloads where many reads should stay cheap while writes remain coordinated enough to keep table and index state consistent. In plain terms: point reads are optimized so they do not have to serialize through a global lock, while writes use row and table coordination where the engine needs a clear mutation boundary.

The simplest transaction is an explicit group of statements followed by `COMMIT`. DataVo buffers the transaction's changes in the session and applies them when the transaction commits.

```sql
BEGIN TRANSACTION;

INSERT INTO Users (Id, Name, IsActive)
VALUES (1, 'Ada', true);

UPDATE Users
SET IsActive = false
WHERE Id = 1;

COMMIT;
```

If the transaction should not be applied, use `ROLLBACK`. The buffered changes are discarded.

```sql
BEGIN TRANSACTION;

INSERT INTO Users (Id, Name, IsActive)
VALUES (2, 'Grace', true);

ROLLBACK;
```

Transactions are session-bound. If your application uses explicit sessions with `DataVoContext.Execute(sql, sessionId)`, the transaction belongs to that session. This lets one part of an embedded application hold a transaction open without accidentally mixing it with statements from another logical workflow.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});

Guid sessionId = Guid.NewGuid();

db.Execute("CREATE DATABASE App", sessionId);
db.Execute("USE App", sessionId);
db.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(80))", sessionId);

db.Execute("BEGIN TRANSACTION", sessionId);
db.Execute("INSERT INTO Users (Id, Name) VALUES (1, 'Ada')", sessionId);
db.Execute("COMMIT", sessionId);
```

MVCC, or multi-version concurrency control, is the part of the engine that lets reads reason about row visibility without forcing every read to block every writer. A read works from a visibility point: rows created after that point should not appear, and older visible versions can still be read while newer writes are being coordinated.

For a point lookup, this is the behavior you want from an embedded database: reading one row by primary key should be fast and should not make unrelated writers wait behind a table-wide read lock.

```sql
SELECT Id, Name
FROM Users
WHERE Id = 1;
```

Writes are more conservative. Inserts acquire a table-scoped write boundary where needed, and updates/deletes coordinate around the rows they modify. That gives DataVo a practical alpha contract: useful transaction semantics for controlled embedded workloads, without claiming the full isolation guarantees of PostgreSQL or SQL Server.

```sql
BEGIN TRANSACTION;

UPDATE Users
SET Name = 'Ada Lovelace'
WHERE Id = 1;

DELETE FROM Users
WHERE Id = 2;

COMMIT;
```

The important limitation is predicate-level isolation. DataVo v0.1 does not claim serializable behavior for arbitrary multi-row predicates. If one transaction reads "all active users" while another inserts a new active user, v0.1 should not be described as preventing every possible phantom-read shape.

```sql
BEGIN TRANSACTION;

SELECT Id, Name
FROM Users
WHERE IsActive = true;

-- Another session may insert a new IsActive = true row.
-- DataVo v0.1 does not claim full phantom-read prevention.

COMMIT;
```

Durability depends on the storage configuration. For the LSM engine, use `LsmStrictFsync = true` when committed writes must wait for the WAL to reach stable storage.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

Relaxed LSM mode is useful for benchmarks and rebuildable data, but it has a different contract: recent acknowledged writes can be lost on power or kernel failure because the WAL is not synchronously forced to stable storage.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_cache",
    LsmStrictFsync = false
});
```

## ACID & Isolation Contract

| Feature | Status | Notes |
| --- | --- | --- |
| `BEGIN TRANSACTION` / `COMMIT` / `ROLLBACK` | Supported | Transactions are session-bound and buffer DML until commit or rollback. |
| Single-Row Lock-Free Reads | Supported | Point-read hot paths are designed to avoid serializing readers through a global write lock. |
| Per-Table Write Locks | Supported | Inserts and commit paths acquire table-scoped write boundaries where the engine needs table-level mutation coordination. |
| Row-level write coordination | Supported | Updates and deletes coordinate around selected row IDs and revalidate rows before mutation. |
| MVCC snapshot visibility | Supported | Row-version metadata and transaction snapshots are used for visibility decisions in supported paths. |
| Strict LSM commit durability | Supported | `LsmStrictFsync = true` waits for WAL durability before acknowledging writes. |
| Disk sync writes | Supported | `SyncDiskWrites = true` opts into fsync-like disk behavior for disk mode. |
| Multi-Row Serializability | Not Supported | v0.1 does not claim serializable isolation for arbitrary multi-row predicates or complex transaction schedules. |
| Phantom-Read Prevention | Not Supported | Predicate-range locking and complete phantom prevention are not public v0.1 guarantees. |
| Full ACID equivalence to PostgreSQL/SQLite | Not Supported | DataVo has useful transaction primitives, but v0.1 still has explicit isolation and storage-format maturity limits. |
