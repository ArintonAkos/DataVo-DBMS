# INSERT Intent + Key Locking (v0.5.1)

Status: implemented on `fix/insert-intent-row-locks` for v0.5.1. Nothing here changes v0.5.0.

## Why

On 25 November 2026, master CI run `45690201364` failed one job (`Tests (macos-latest, net8.0)`):
`WriterProgressUnderReadLoadTests.Writers_CompleteWritesInEveryWindow_UnderContinuousPointReads(mode: InMemory)`.

| Writer | Writes per 300 ms window |
| --- | --- |
| 0 (`InsertTyped`) | 19570, 22374, 22361, 44884, **0** |
| 1 (compiled `UPDATE`) | 2446, 1757, 3170, 784, 4842 |

The inserting writer completed nothing for a whole window while the updating writer and the readers kept
going. Per-phase timestamps put nearly all of a slow insert's time in `ReaderWriterLockSlim.TryEnterWriteLock`
for the table lock: every single-row insert took an **exclusive table write lock**, while every compiled `UPDATE`
held the table's **intent (shared) lock** for each row lock, and in-memory updates held it for up to ~17 ms while
building dictionaries for the old and new row. The two writers excluded each other through the table lock.

## Root Causes and Fixes

### 1. Table lock entry churn

`LockManager` removed and disposed a table's `ReaderWriterLockSlim` whenever its last user released it. A row
write's intent release did that, so a busy table allocated a new lock object (and churned the lock dictionary)
on almost every update.

- Table lock entries now live as long as the table. `DROP TABLE` / `DROP DATABASE` forget them
  (`ForgetTable` / `ForgetDatabase`), and only when no thread is using them.
- Entries are found without the lifecycle lock (a full-fence retain/forget handshake keeps a retainer from using
  an entry that is being dropped).
- Row and key locks are keyed by a struct id instead of strings built on every acquire and release; idle row
  entries are recycled through a pool; the wait-for graph allocates a blocker set only when a request waits;
  release closures are allocated only when a durable batch defers the release.

### 2. In-memory compiled `UPDATE` off the zero-allocation path

`EnableZeroAllocCompiledUpdate` only took effect in LSM mode. The in-memory branch below the gate was unreachable
(and wrong: it patched the stored row array in place and released a lock on the wrong row after a move).

In-memory rows are never changed in place (typed rows are read without a lock), so the in-memory fast path does
what the dictionary path does, in the same order, without materializing the row: under the row's write lock it
appends a copy with the assigned cells patched (typed rows through a borrowed cell buffer), locks the copy,
re-points the primary key and deletes the old row.

| In-memory compiled `UPDATE` (warm, 1,000 rows) | Allocated | Time |
| --- | --- | --- |
| Dictionary path (before) | ~16.1–16.5 KB/update | ~16–24 us |
| Fast path (after) | 96 B/update (storage growth of the appended row) | ~5 us |

Found on the way and fixed: a lock-free primary-key point read that landed on a row an update had just moved
reported an existing key as missing (both update paths move rows); the read now follows the key. `RemoveVersion`
allocated per call. `VACUUM` renumbered rows without any table lock.

### 3. INSERT on intent + key locks

A single-row `INSERT` (`InsertTyped`, `InsertTypedBatch` with one row, auto-commit SQL `INSERT` of one row whose
values convert to typed cells) takes the table's **intent lock** plus an exclusive **key lock** on each of its key
values: the primary key, then each non-NULL unique column in catalog order. Keys are reserved before they are
checked and held until the row is published (inside a durable LSM batch the releases wait for publication).
Multi-row and bulk inserts escalate to the table write lock instead of taking one key lock per row.

Key lock identity is a function of the value's index key string, which INSERT and UPDATE build the same way
(`IndexKeyEncoder`): a canonical integer key (`[42]`) locks by its number, anything else by its string, so typed
and SQL inserts and key-changing updates of the same value always meet on the same lock.

## Audit: What the Exclusive INSERT Lock Protected

| Concern | Before | After |
| --- | --- | --- |
| Row-id assignment | Already serialized by the storage engine (`TableStore` write lock, LSM memtable) | Unchanged |
| Index writes | Already serialized (`IndexManager` lock); updates already wrote concurrently | Unchanged |
| PK / unique check vs concurrent INSERT of the same value | Table write lock | Key lock on the value, held until publication |
| PK / unique check vs UPDATE giving a row the same value | Table write lock (insert side only); two concurrent key-changing UPDATEs already raced each other | `UpdateKeyValidator` reserves new key values for SQL, compiled and `COMMIT` updates |
| FK check (child insert vs parent delete) | **Not protected**: parent deletes never locked the child table, so the old lock never covered this race | Unchanged; tracked as a follow-up (shared key locks on referenced parent values) |
| Publication order / change capture | Per-statement commit stamps (`CommitClock`); updates already published concurrently | Unchanged |
| Deadlock detection | Table lock in the wait-for graph | Key locks are lock entries in the same wait-for graph; a single-row insert takes its keys in a fixed order and waits on nothing after them |
| DDL exclusivity | `ALTER` took the table write lock; `CREATE INDEX`, `DROP INDEX`, `DROP TABLE` and `VACUUM` took none | All of them take the table write lock and wait for every intent holder |
| Transactions | `COMMIT` takes the table write lock for tables it inserts into | Unchanged; key-changing updates in a `COMMIT` reserve their new values |

## Verification

- `InsertKeyLockTests`: concurrent duplicate PK and unique inserts (typed and SQL, in-memory and LSM) insert each
  value once; inserts racing key-changing updates and a committed key change never duplicate a key; an insert does
  not wait for an update of another row; `ALTER TABLE` waits for an insert holding its keys; key locks exclude only
  the same value; opposite-order key locks are reported as a deadlock. Without key reservation the duplicate-key
  tests fail.
- `InMemoryCompiledUpdateFastPathTests`: allocation and latency against the dictionary path, typed and serialized
  rows, NULL fallback, other fixed-width types, and point reads never missing a row that concurrent updates move.
- `LockManagerRowLevelTests`: table entries stay stable under concurrent row writers and are forgotten on drop.
- `WriterProgressUnderReadLoadTests` keeps its assertions: every writer completes writes in every 300 ms window.
