# Unity Arena Cross-Engine Benchmark Design

## Purpose

Extend the Unity NPC Memory Arena into two complementary, reproducible benchmark lanes:

1. a macOS Unity IL2CPP lane that compares DataVo reactive views, DataVo
   polling, and SQLite polling through a real native SQLite boundary; and
2. a desktop .NET lane that adds DuckDB polling to the same relational
   workload.

The benchmark answers two distinct questions without conflating them:

- Can a Unity IL2CPP Player run each deployable local-database option and
  produce the same game-derived state?
- Given the same data, mutations, and required fresh views, how much time and
  work does DataVo incremental maintenance avoid compared with full-query
  polling?

It is not a general claim that DataVo is faster than SQLite or DuckDB, and it
does not establish support for platforms other than the pinned macOS target.

## Scope and non-goals

### In scope

- Unity 6000.5.4f1 macOS arm64 IL2CPP Player proof for DataVo and SQLite.
- A macOS-only SQLite adapter that crosses into the platform `sqlite3` C API
  through a small Unity-safe binding layer; its native boundary is an explicit
  part of the reported configuration.
- Desktop .NET comparison among DataVo reactive, DataVo polling, SQLite
  polling, and DuckDB polling.
- A common deterministic relational dataset, mutation stream, SQL-equivalent
  views, canonical result hashes, raw JSON artifacts, and documented commands.
- Two profiles: a normal active-gameplay view workload and an IVM stress
  workload.

### Out of scope

- DuckDB in Unity, which is a native analytics-oriented desktop reference, not
  a game-runtime deployment target in this proof.
- Windows, iOS, Android, consoles, Burst, Jobs, persistence/durability, or
  production-save-data claims.
- A cross-engine Unity vector-search comparison. DataVo vector search remains
  an IL2CPP demonstration; the existing desktop vector scenario remains the
  SQLite-vector comparison where its native extension is available.
- Claiming that a desktop host result represents Unity Player performance.

## Shared relational workload

All engines receive the same seeded logical data and mutation batches. The
relational portion intentionally excludes vector search so every engine is
measured against equivalent SQL-visible work.

### Schema and derived views

The existing arena schema remains the source of truth:

- `Players(Id, Team, Zone, Health, Score, Position)`
- `Inventory(Id, PlayerId, ItemId, Slot)`

Every active-view run maintains or queries these exact derived results:

```sql
SELECT Id, Score FROM Players ORDER BY Score DESC LIMIT 20;

SELECT Team, COUNT(*) AS Alive
FROM Players
WHERE Health > 0
GROUP BY Team;

SELECT p.Id, i.ItemId
FROM Players p
JOIN Inventory i ON p.Id = i.PlayerId
WHERE p.Zone = 'arena-7';
```

The canonical output sorts every view deterministically and separately hashes
the encoded leaderboard, team aggregate, and zone-inventory results. Every
engine and mode must match all three DataVo reactive baseline hashes for a run
to be accepted.

### Modes

- **DataVo reactive:** subscribe once to the three active views before the
  measured interval. DataVo incrementally maintains their selected results as
  writes occur. The consumer reads the current selected results after each
  scheduled view demand.
- **DataVo polling:** apply the same write batch, then run the three complete
  SQL queries when the consumer demands a fresh view.
- **SQLite polling / DuckDB polling:** apply the same write batch, then run
  SQL-equivalent prepared queries at the same demand point and materialize the
  returned rows.

No engine receives a cache or a reduced query that another engine does not
receive. Setup, seeding, and warm-up are timed separately from measured batches.

## Profiles

The benchmark never sleeps to simulate real time. A cadence describes the
application semantics and determines when writes and view reads happen; wall
time measures only engine work.

| Profile | Logical NPCs | Simulated cadence | Writes per batch | Active view demand | Measured batches | Warm-up batches |
| --- | ---: | ---: | ---: | --- | ---: | ---: |
| `gameplay` | 1,000 | 10 Hz | 4 | all three views after every batch | 300 | 30 |
| `ivm-stress` | 10,000 | 60 Hz | 24 | all three views after every batch | 1,200 | 120 |

The gameplay profile represents an active UI/tool view receiving forty state
writes per simulated second. It is not a claim that a game should update its
whole database every rendered frame. The stress profile intentionally forces
the “derived result must remain live after writes” requirement that benefits
incremental maintenance.

The visual arena remains a separate demonstration. Its exact local memory
search runs at the existing decision cadence and is reported separately from
the cross-engine relational results.

## Unity IL2CPP lane

### SQLite boundary

The Unity SQLite adapter is a narrow macOS-only benchmark dependency. It uses
the macOS `sqlite3` C API through explicit P/Invoke declarations and prepared
statements; it does not assume that the desktop `Microsoft.Data.Sqlite` NuGet
package is a supported Unity integration. The adapter must prove all of the
following in an actual Medium-stripping IL2CPP Player before benchmark numbers
are accepted:

- open an in-memory SQLite database;
- create the shared schema and seed deterministic rows;
- apply one deterministic mutation batch;
- execute and materialize all three derived views;
- emit a passed JSON artifact identifying `SQLite`, `OSXPlayer`, `IL2CPP`, and
  the SQLite library version; and
- produce canonical hashes equal to DataVo polling for the same profile.

If the platform library or P/Invoke boundary is unavailable, the Unity script
must fail clearly. It must not silently substitute the desktop host, a managed
mock, or DataVo.

### Player executions

The existing Player build remains DataVo-only for its vector-memory proof. A
separate cross-engine Player runner builds the arena benchmark once, runs
DataVo reactive, DataVo polling, and SQLite polling with distinct safe run IDs,
and validates one JSON artifact from each invocation. It checks the requested
mode/engine, Unity metadata, profile, positive measured-batch count, raw-view
hashes, and final hashes.

Each Player process has a 120-second timeout. The runner hashes tracked Unity
settings before and after, preserves package-lock contents, prints Editor and
Player diagnostics on failure, and records the exact native SQLite version.

## Desktop .NET lane

Add an `arena-live-views` scenario to the existing `Research.Benchmark` host.
It reuses the shared deterministic seed and canonical hashing code, then runs:

- `datavo-reactive`
- `datavo-polling`
- `sqlite`
- `duckdb`

SQLite and DuckDB use in-memory databases, prepared write statements, and
prepared or stable SQL query execution where their provider supports it.
DuckDB is included only in this lane and is labeled as an analytical desktop
reference. The host must report an unsupported engine/profile explicitly rather
than omit it from a requested matrix.

## Measurement and artifacts

Every engine/profile pair runs seven independent repetitions after its defined
warm-up, with a fresh database and engine instance for every repetition. The
runner saves one JSON artifact per repetition and a summary JSON plus Markdown
table containing:

- engine, mode, profile, seed, schema/query revision, OS, CPU, .NET/Unity
  version, scripting backend, native SQLite version where applicable;
- total measured time, batch P50/P95/P99, and per-view P50/P95/P99;
- writes applied, reactive changes emitted or polling rows materialized;
- all three final canonical hashes and equivalence status; and
- the median total time across the seven repetitions.

The summary excludes a failed repetition from no calculation: any failed
equivalence, unsupported requested engine, missing artifact, or process timeout
fails the whole command. Reported comparison tables must display all successful
raw repetition paths or a reproducible output directory, not only the median.

## Validation

Tests must cover deterministic seed equivalence, every profile’s expected write
and demand counts, canonical hash equality across engines, rejection of a wrong
engine/mode/profile report, and clear failure when a requested SQLite IL2CPP
boundary is absent. Existing Unity project contracts must assert the new runner
and candidate checks without allowing package-lock or tracked-settings changes.

Actual gates, in order:

```bash
dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release
bash scripts/run-unity-arena-editor-tests.sh
bash scripts/run-unity-macos-il2cpp-arena.sh
bash scripts/run-unity-macos-il2cpp-arena-cross-engine.sh
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- \
  --scenario arena-live-views --profile gameplay --repetitions 7 --format markdown
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- \
  --scenario arena-live-views --profile ivm-stress --repetitions 7 --format markdown
```

The final documentation must state which lane produced each number and must
never compare Unity IL2CPP values directly with desktop .NET values.
