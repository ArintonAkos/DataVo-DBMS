# Unity Arena Cross-Engine Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add reproducible normal-gameplay and IVM-stress arena benchmarks that compare DataVo reactive/polling, SQLite polling, and desktop-only DuckDB polling while proving the Unity SQLite path in macOS IL2CPP.

**Architecture:** A new desktop `arena-live-views` scenario owns the engine-neutral relational fixture, deterministic mutation stream, canonical view hashes, repetition runner, and JSON/Markdown artifact writer. Unity keeps its current vector-memory Player proof and adds a separate, macOS-only direct SQLite C-API adapter plus a cross-engine headless bootstrap and Player script. The two lanes share profile names, seed, SQL semantics, and hash format, but never mix runtime measurements.

**Tech Stack:** .NET 10; DataVo in-memory/reactive APIs; existing `Microsoft.Data.Sqlite` and `DuckDB.NET.Data` desktop dependencies; Unity 6000.5.4f1; macOS arm64 IL2CPP; `System.Runtime.InteropServices.DllImport` against macOS `sqlite3`; NUnit Unity EditMode tests; xUnit desktop tests; Bash and Node artifact validation.

## Global Constraints

- Keep `DataVo.Core` public package `net10.0`-only; Unity continues to consume only the existing generated `netstandard2.1` candidate.
- Unity cross-engine support is macOS arm64 IL2CPP only. Never claim Windows, mobile, console, Burst, Jobs, persistence, or production-save-data support.
- DuckDB appears only in the desktop .NET lane and must be labeled an analytical desktop reference.
- The Unity SQLite adapter uses explicit macOS `sqlite3` P/Invoke. It must fail clearly when unavailable and never substitute DataVo, a mock, or a desktop provider.
- Profiles are fixed: `gameplay` = 1,000 NPCs/4 writes/300 measured/30 warm-up batches; `ivm-stress` = 10,000 NPCs/24 writes/1,200 measured/120 warm-up batches. Both demand all three views after every batch.
- Cross-engine relational work excludes vector search. Existing Unity vector-memory and desktop `vector-search` commands remain separate and must be documented as such.
- Every accepted run must match canonical leaderboard, team-aggregate, and zone-inventory SHA-256 hashes. A missing requested engine, failed repetition, mismatch, timeout, changed package lock, or changed tracked Player settings fails the command.
- Run seven independent repetitions with a fresh engine/database for every repetition. Report raw paths plus median total time and within-run P50/P95/P99; never publish a single run as the result.
- Run Core portable builds serially; never run portable target restores concurrently.

---

## File structure

| Path | Responsibility |
| --- | --- |
| `demos/Research.Benchmark/src/Research.Benchmark.Abstractions/ArenaLiveViewsContracts.cs` | Profiles, mutation/view records, canonical result contract, engine interface, and validation constants. |
| `demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/ArenaLiveViewsFixture.cs` | Deterministic seed data and mutation batches shared by all desktop engines. |
| `demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/ArenaLiveViewsSql.cs` | Schema and the three exact relational SQL queries. |
| `demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/*Engine.cs` | Four focused desktop engines: DataVo reactive, DataVo polling, SQLite polling, and DuckDB polling. |
| `demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/ArenaLiveViewsScenarioRunner.cs` | Per-repetition timing, correctness checks, and result construction. |
| `demos/Research.Benchmark/src/Research.Benchmark.Host/ArenaLiveViewsCommand.cs` | CLI options, seven-repetition orchestration, JSON artifact persistence, and Markdown summary. |
| `demos/Research.Benchmark/src/Research.Benchmark.Host/Program.cs` | Routes `--scenario arena-live-views` to the new command. |
| `demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViews*.cs` | Fixture, engine-equivalence, and artifact/CLI tests. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineContracts.cs` | Unity-safe profile/result DTOs that JsonUtility can serialize. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineWorkload.cs` | Unity deterministic relational seed/mutations and canonical SHA-256 result encoding. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/Sqlite/ArenaSqliteNative.cs` | Small direct P/Invoke declarations for the macOS `sqlite3` C API. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/Sqlite/ArenaSqlitePollingSession.cs` | SQLite schema, prepared statements, mutation application, and materialized view reads. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineRunner.cs` | DataVo reactive, DataVo polling, and SQLite polling execution in Unity. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineBootstrap.cs` | Parses safe Player arguments, writes one result JSON, and exits. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaCrossEngineEditModeTests.cs` | Unity deterministic/hash and SQLite boundary tests. |
| `scripts/run-unity-macos-il2cpp-arena-cross-engine.sh` | Builds once and runs both profiles across all three Unity engines. |
| `scripts/test-unity-arena-project.sh` | Static contract for the new adapter, bootstrap, and runner. |
| `docs/features/unity-and-godot.md` | Narrow support statement and lane separation. |
| `docs/manual/performance/reproducing-benchmarks.md` | Exact desktop/Unity reproduction commands and interpretation rules. |

## Shared interfaces

All desktop engines implement this exact interface from `ArenaLiveViewsContracts.cs`:

```csharp
public interface IArenaLiveViewsEngine : IAsyncDisposable
{
    string EngineId { get; }
    EngineArchitecture Architecture { get; }

    ValueTask InitializeAsync(ArenaLiveViewsProfile profile, CancellationToken cancellationToken = default);
    ValueTask ApplyBatchAsync(IReadOnlyList<ArenaLiveViewsMutation> batch, CancellationToken cancellationToken = default);
    ValueTask<ArenaLiveViewsViews> ReadViewsAsync(CancellationToken cancellationToken = default);
}
```

The fixed profile API is:

```csharp
public sealed record ArenaLiveViewsProfile(
    string Id, int Seed, int LogicalNpcCount, int MutationsPerBatch,
    int WarmupBatches, int MeasuredBatches, int SimulatedCadenceHz)
{
    public static ArenaLiveViewsProfile Gameplay { get; } =
        new("gameplay", 20260721, 1_000, 4, 30, 300, 10);
    public static ArenaLiveViewsProfile IvmStress { get; } =
        new("ivm-stress", 20260721, 10_000, 24, 120, 1_200, 60);

    public static ArenaLiveViewsProfile Parse(string id) => id switch
    {
        "gameplay" => Gameplay,
        "ivm-stress" => IvmStress,
        _ => throw new ArgumentException("Profile must be gameplay or ivm-stress.", nameof(id))
    };
}
```

`ArenaLiveViewsViews` contains ordered `Leaderboard`, `AliveByTeam`, and
`ZoneInventory` arrays. `ArenaLiveViewsHasher.Compute` emits one lowercase
64-character SHA-256 value per array using invariant UTF-8 encodings. The Unity
implementation exposes the same profile IDs, seed, mutation rules, and hash
algorithm through `ArenaCrossEngineWorkload`.

The remaining shared records are deliberately engine-neutral. Define them in
the same contracts file so reporting and tests never depend on an individual
provider's data reader:

```csharp
public sealed record ArenaLiveViewsMutation(
    int PlayerId, int ScoreDelta, int ReplacementHealth, string ReplacementZone,
    int InventoryItemId, int InventorySlot);

public sealed record ArenaLeaderboardRow(int Id, int Score);
public sealed record ArenaAliveByTeamRow(string Team, int Alive);
public sealed record ArenaZoneInventoryRow(int PlayerId, int ItemId);

public sealed record ArenaLiveViewsViews(
    IReadOnlyList<ArenaLeaderboardRow> Leaderboard,
    IReadOnlyList<ArenaAliveByTeamRow> AliveByTeam,
    IReadOnlyList<ArenaZoneInventoryRow> ZoneInventory);

public sealed record ArenaLiveViewsHashes(
    string Leaderboard, string AliveByTeam, string ZoneInventory);

public sealed record ArenaLiveViewsPercentiles(
    int Count, double P50Milliseconds, double P95Milliseconds, double P99Milliseconds,
    double MaxMilliseconds, double MeanMilliseconds);

public sealed record ArenaLiveViewsRepetition(
    string EngineId, int Repetition, double TotalMilliseconds,
    ArenaLiveViewsPercentiles Batch, ArenaLiveViewsPercentiles Leaderboard,
    ArenaLiveViewsPercentiles AliveByTeam, ArenaLiveViewsPercentiles ZoneInventory,
    long WritesApplied, long ReactiveChanges, long PollingRows,
    ArenaLiveViewsHashes Hashes, string ArtifactPath);

public sealed record ArenaLiveViewsEngineSummary(
    string EngineId, EngineArchitecture Architecture,
    IReadOnlyList<ArenaLiveViewsRepetition> Runs,
    ArenaLiveViewsHashes CanonicalHashes, double MedianTotalMilliseconds);

public sealed record ArenaLiveViewsSummary(
    string SchemaRevision, ArenaLiveViewsProfile Profile,
    IReadOnlyList<ArenaLiveViewsEngineSummary> Engines);
```

The exact deterministic fixture is duplicated only because Unity cannot consume
the desktop runner assembly. Keep the two implementations byte-compatible by
using the following algorithm verbatim, with `Teams = ["amber", "violet",
"teal", "coral"]` and `Zones = ["arena-7", "arena-5", "arena-3", "arena-1"]`:

```csharp
private static uint Mix(uint value)
{
    value ^= value >> 16;
    value *= 0x7FEB352Du;
    value ^= value >> 15;
    value *= 0x846CA68Bu;
    value ^= value >> 16;
    return value;
}

private static uint Next(ref uint state)
{
    state += 0x9E3779B9u;
    return Mix(state);
}

public static ArenaLiveViewsMutation[] CreateBatch(ArenaLiveViewsProfile profile, int batchIndex)
{
    uint state = Mix((uint)profile.Seed ^ (uint)batchIndex * 0x9E3779B9u);
    var result = new ArenaLiveViewsMutation[profile.MutationsPerBatch];
    for (int index = 0; index < result.Length; index++)
    {
        int playerId = 1 + (int)(Next(ref state) % (uint)profile.LogicalNpcCount);
        result[index] = new ArenaLiveViewsMutation(
            playerId,
            1 + (int)(Next(ref state) % 19u),
            (int)(Next(ref state) % 101u),
            Zones[Next(ref state) % (uint)Zones.Length],
            100 + (int)(Next(ref state) % 64u),
            (int)(Next(ref state) % 8u));
    }
    return result;
}
```

Initial rows are likewise fixed: player `id` has team
`Teams[(id - 1) % 4]`, zone `Zones[(id - 1) % 4]`, health `60 + id % 41`,
score `(id * 37) % 1_000`; its one inventory row has `InventoryId = PlayerId =
id`, `ItemId = 100 + id % 64`, and `Slot = id % 8`. Apply repeated mutations
to the same player in their array order; do not coalesce them. The relational
workload intentionally omits the visual/vector `Position` column and
`NpcMemories` table, which are covered by the separate existing vector arena
proof.

`ArenaLiveViewsFixture.CreateExpectedViews(profile, appliedBatchCount)` is the
deterministic oracle. It starts from the fixed initial rows, applies batch
indices `0` through `appliedBatchCount - 1` in order, then produces the three
ordered arrays using the exact SQL sort rules. It must reject a negative count.
The desktop runner uses it for batch `profile.WarmupBatches + measuredIndex + 1`;
the Unity workload exposes the same method and uses the same count convention.

### Task 1: Define the desktop arena workload and deterministic oracle

**Files:**
- Create: `demos/Research.Benchmark/src/Research.Benchmark.Abstractions/ArenaLiveViewsContracts.cs`
- Create: `demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/ArenaLiveViewsFixture.cs`
- Create: `demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/ArenaLiveViewsSql.cs`
- Create: `demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsFixtureTests.cs`

**Consumes:** Existing `EngineArchitecture`, xUnit, and the approved profile table.

**Produces:** `ArenaLiveViewsProfile`, `ArenaLiveViewsMutation`, `ArenaLiveViewsViews`, `ArenaLiveViewsHashes`, `IArenaLiveViewsEngine`, deterministic fixture methods, and exact SQL constants.

- [ ] **Step 1: Write failing profile/oracle tests**

```csharp
[Fact]
public void GameplayProfile_HasApprovedRates()
{
    ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Gameplay;
    Assert.Equal((1_000, 4, 30, 300, 10),
        (profile.LogicalNpcCount, profile.MutationsPerBatch, profile.WarmupBatches,
         profile.MeasuredBatches, profile.SimulatedCadenceHz));
}

[Fact]
public void IvmStressProfile_HasApprovedRates()
{
    ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.IvmStress;
    Assert.Equal((10_000, 24, 120, 1_200, 60),
        (profile.LogicalNpcCount, profile.MutationsPerBatch, profile.WarmupBatches,
         profile.MeasuredBatches, profile.SimulatedCadenceHz));
}

[Theory]
[InlineData("gameplay")]
[InlineData("ivm-stress")]
public void EveryProfile_DemandsAllThreePublishedViews(string profileId)
{
    ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Parse(profileId);
    Assert.NotNull(profile);
    Assert.Equal(3, ArenaLiveViewsSql.All.Count);
}

[Fact]
public void SameSeedAndBatchIndex_ProduceIdenticalMutationsAndHashes()
{
    ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Gameplay;
    ArenaLiveViewsMutation[] batch = ArenaLiveViewsFixture.CreateBatch(profile, 17);
    Assert.Equal(batch, ArenaLiveViewsFixture.CreateBatch(profile, 17));
    Assert.Equal(new ArenaLiveViewsMutation(209, 9, 2, "arena-5", 120, 5), batch[0]);
    Assert.Equal(64, ArenaLiveViewsHasher.Compute(
        new ArenaLiveViewsViews([], [], [])).Leaderboard.Length);
}
```

- [ ] **Step 2: Run the tests to verify red**

Run: `dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release --filter ArenaLiveViewsFixtureTests`

Expected: compilation fails because the arena live-view contracts and fixture do not exist.

- [ ] **Step 3: Implement the contracts, deterministic fixture, and hash encoding**

Create the records and interface shown in Shared interfaces. Implement
`ArenaLiveViewsFixture.CreateInitialPlayers(profile)`,
`CreateInitialInventory(profile)`, and `CreateBatch(profile, batchIndex)` with
the exact `Mix`/`Next` algorithm and row formulas in Shared interfaces; do not
call `Random` or read the clock. A mutation contains the player id, score
delta, replacement health, replacement zone, replacement inventory item id,
and replacement inventory slot. Use these exact query constants:

```csharp
public const string CreatePlayers = "CREATE TABLE Players (Id INT PRIMARY KEY, Team VARCHAR(10), Zone VARCHAR(20), Health INT, Score INT)";
public const string CreateInventory = "CREATE TABLE Inventory (InventoryId INT PRIMARY KEY, PlayerId INT, ItemId INT, Slot INT)";
public const string Leaderboard = "SELECT Id, Score FROM Players ORDER BY Score DESC, Id ASC LIMIT 20";
public const string AliveByTeam = "SELECT Team, COUNT(*) AS Alive FROM Players WHERE Health > 0 GROUP BY Team ORDER BY Team ASC";
public const string ZoneInventory = "SELECT p.Id, i.ItemId FROM Players p JOIN Inventory i ON p.Id = i.PlayerId WHERE p.Zone = 'arena-7' ORDER BY p.Id ASC, i.ItemId ASC";
public static readonly IReadOnlyList<string> All = [Leaderboard, AliveByTeam, ZoneInventory];
```

Hash each ordered view with UTF-8 lines `id|score\n`, `team|alive\n`, and
`playerId|itemId\n`, then `SHA256.HashData` and lowercase hexadecimal. Validate
that only `gameplay` and `ivm-stress` are accepted and all profile counts are
positive.

- [ ] **Step 4: Run the focused tests to verify green**

Run: `dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release --filter ArenaLiveViewsFixtureTests`

Expected: tests pass; the same profile/seed/batch produces byte-identical
mutations and each empty-view hash has 64 lowercase hexadecimal characters.

- [ ] **Step 5: Commit**

```bash
git add demos/Research.Benchmark/src/Research.Benchmark.Abstractions/ArenaLiveViewsContracts.cs \
  demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/ArenaLiveViewsFixture.cs \
  demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/ArenaLiveViewsSql.cs \
  demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsFixtureTests.cs
git commit -m "feat: define arena live-view benchmark workload"
```

### Task 2: Implement desktop DataVo reactive and polling engines

**Files:**
- Create: `demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/DataVoReactiveArenaLiveViewsEngine.cs`
- Create: `demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/DataVoPollingArenaLiveViewsEngine.cs`
- Create: `demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsDataVoEngineTests.cs`

**Consumes:** Task 1 contracts, `DataVoContext`, `SubscribeZeroAlloc`, and `ArenaLiveViewsSql`.

**Produces:** `datavo-reactive` and `datavo-polling` desktop engines with equal observable views.

- [ ] **Step 1: Write the failing DataVo equivalence test**

```csharp
[Fact]
public async Task ReactiveAndPolling_ProduceEqualHashesAfterTheSameBatches()
{
    ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Gameplay;
    await using IArenaLiveViewsEngine reactive = new DataVoReactiveArenaLiveViewsEngine();
    await using IArenaLiveViewsEngine polling = new DataVoPollingArenaLiveViewsEngine();
    await reactive.InitializeAsync(profile);
    await polling.InitializeAsync(profile);

    for (int batch = 0; batch < 8; batch++)
    {
        IReadOnlyList<ArenaLiveViewsMutation> mutations = ArenaLiveViewsFixture.CreateBatch(profile, batch);
        await reactive.ApplyBatchAsync(mutations);
        await polling.ApplyBatchAsync(mutations);
    }

    Assert.Equal(ArenaLiveViewsHasher.Compute(await reactive.ReadViewsAsync()),
                 ArenaLiveViewsHasher.Compute(await polling.ReadViewsAsync()));
}
```

- [ ] **Step 2: Run the test to verify red**

Run: `dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release --filter ArenaLiveViewsDataVoEngineTests`

Expected: compilation fails because the two DataVo arena engines do not exist.

- [ ] **Step 3: Implement both DataVo engines**

Both engines create an in-memory `ArenaLiveViews` database with `Players` and
`Inventory`, preload fixture rows before timing, and apply every mutation with
typed or parameter-equivalent DataVo writes. The reactive engine subscribes to
the leaderboard SQL constant before preloading. Current DataVo reactive
aggregate and join operators reject `ORDER BY`, so its aggregate and zone
subscriptions use the same `SELECT`/`WHERE`/`GROUP BY`/`JOIN` expressions with
only the final ordering removed; their maintained projections restore ordinal
team order and `(PlayerId, ItemId)` order from `QueryChangeRef`. Dispatch
notifications after each write batch. `ReadViewsAsync` returns those maintained
arrays without executing SQL.

The polling engine applies the identical writes but executes all three SQL
constants in `ReadViewsAsync`, maps rows, and returns the same ordered arrays.
Expose `EngineId` exactly as `datavo-reactive` and `datavo-polling`, with
`EmbeddedReactiveIvm` and `EmbeddedPollingRecompute` architectures respectively.

- [ ] **Step 4: Run focused tests and the existing benchmark suite**

Run: `dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release --filter "ArenaLiveViewsDataVoEngineTests|ComplexVipExposureEngineTests"`

Expected: arena DataVo hashes match and existing complex VIP engine contracts still pass.

- [ ] **Step 5: Commit**

```bash
git add demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/DataVoReactiveArenaLiveViewsEngine.cs \
  demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/DataVoPollingArenaLiveViewsEngine.cs \
  demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsDataVoEngineTests.cs
git commit -m "feat: add DataVo arena live-view engines"
```

### Task 3: Implement desktop SQLite and DuckDB polling engines

**Files:**
- Create: `demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/SqliteArenaLiveViewsEngine.cs`
- Create: `demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/DuckDbArenaLiveViewsEngine.cs`
- Create: `demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsPollingEngineTests.cs`

**Consumes:** Task 1 fixture and SQL, existing `Microsoft.Data.Sqlite` and `DuckDB.NET.Data` references.

**Produces:** `sqlite` and `duckdb` desktop polling engines whose three hashes match DataVo reactive.

- [ ] **Step 1: Write the failing three-engine equivalence theory**

```csharp
public static IEnumerable<object[]> PollingEngines()
{
    yield return [new SqliteArenaLiveViewsEngine()];
    yield return [new DuckDbArenaLiveViewsEngine()];
}

[Theory]
[MemberData(nameof(PollingEngines))]
public async Task PollingEngine_MatchesDataVoReactiveHashes(IArenaLiveViewsEngine engine)
{
    await using IArenaLiveViewsEngine baseline = new DataVoReactiveArenaLiveViewsEngine();
    await using (engine)
    {
        ArenaLiveViewsProfile profile = ArenaLiveViewsProfile.Gameplay;
        await baseline.InitializeAsync(profile);
        await engine.InitializeAsync(profile);
        IReadOnlyList<ArenaLiveViewsMutation> batch = ArenaLiveViewsFixture.CreateBatch(profile, 4);
        await baseline.ApplyBatchAsync(batch);
        await engine.ApplyBatchAsync(batch);
        Assert.Equal(ArenaLiveViewsHasher.Compute(await baseline.ReadViewsAsync()),
                     ArenaLiveViewsHasher.Compute(await engine.ReadViewsAsync()));
    }
}
```

- [ ] **Step 2: Run the theory to verify red**

Run: `dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release --filter ArenaLiveViewsPollingEngineTests`

Expected: compilation fails because the SQLite and DuckDB arena engines do not exist.

- [ ] **Step 3: Implement polling adapters with equivalent prepared writes**

For both engines, open a fresh in-memory database; create `Players` and
`Inventory` with primary-key/update indexes (`Players.Id`, `Players.Zone`,
`Inventory.PlayerId`); bulk seed in one transaction; prepare statements for
player score/health/zone updates and inventory item replacement; and materialize
the three SQL constants on each `ReadViewsAsync` call. SQLite uses named
parameters and `SqliteDataReader`; DuckDB uses positional parameters and
`DuckDBDataReader`. Convert the result rows to the exact contracts from Task 1
and sort defensively before returning. Dispose all commands, transactions, and
connections.

- [ ] **Step 4: Run the focused theory plus the full benchmark tests**

Run: `dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release --filter ArenaLiveViewsPollingEngineTests`

Run: `dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release`

Expected: both polling engines match DataVo hashes and all existing benchmark tests pass.

- [ ] **Step 5: Commit**

```bash
git add demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/SqliteArenaLiveViewsEngine.cs \
  demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/DuckDbArenaLiveViewsEngine.cs \
  demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsPollingEngineTests.cs
git commit -m "feat: add SQLite and DuckDB arena polling engines"
```

### Task 4: Add desktop repetitions, artifacts, and CLI scenario

**Files:**
- Create: `demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/ArenaLiveViewsScenarioRunner.cs`
- Create: `demos/Research.Benchmark/src/Research.Benchmark.Host/ArenaLiveViewsCommand.cs`
- Modify: `demos/Research.Benchmark/src/Research.Benchmark.Host/Program.cs`
- Create: `demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsScenarioRunnerTests.cs`
- Create: `demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsCommandTests.cs`

**Consumes:** Task 1 fixture and all four Task 2–3 engines.

**Produces:** `arena-live-views` CLI command with strict profile/engine/repetition/output validation and per-repetition raw JSON artifacts.

- [ ] **Step 1: Write failing artifact and median tests**

```csharp
[Fact]
public async Task SevenRepetitions_WriteRawArtifactsAndMedianSummary()
{
    string output = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
    ArenaLiveViewsSummary summary = await ArenaLiveViewsScenarioRunner.RunAsync(
        ArenaLiveViewsProfile.Gameplay, new[] { () => new DataVoPollingArenaLiveViewsEngine() }, 7, output);

    Assert.Equal(7, summary.Engines.Single().Runs.Count);
    Assert.True(File.Exists(Path.Combine(output, "summary.json")));
    Assert.True(File.Exists(Path.Combine(output, "summary.md")));
    Assert.Equal(64, summary.Engines.Single().CanonicalHashes.ZoneInventory.Length);
}

[Fact]
public void Command_RejectsUnknownProfile()
{
    Assert.Throws<ArgumentException>(() => ArenaLiveViewsCommand.Parse(
        ["--profile", "fast", "--repetitions", "7", "--output", "/tmp/out"]));
}

[Fact]
public void Command_RejectsUnknownEngineRatherThanOmittingIt()
{
    Assert.Throws<ArgumentException>(() => ArenaLiveViewsCommand.Parse(
        ["--profile", "gameplay", "--engine", "rocksdb", "--output", "/tmp/out"]));
}
```

- [ ] **Step 2: Run the tests to verify red**

Run: `dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release --filter "ArenaLiveViewsScenarioRunnerTests|ArenaLiveViewsCommandTests"`

Expected: compilation fails because the scenario runner and command parser do not exist.

- [ ] **Step 3: Implement measured repetitions and artifact formatting**

`ArenaLiveViewsScenarioRunner.RunAsync` receives one profile, factories for
requested engines, an exact repetition count, and an output directory. For each
engine/repetition: create a new engine, initialize, apply warm-up batches
without timing, force GC only before the measured interval, time every measured
batch from `ApplyBatchAsync` through `ReadViewsAsync`, validate all three hashes
after every measured batch against
`ArenaLiveViewsFixture.CreateExpectedViews(profile, profile.WarmupBatches + measuredIndex + 1)`,
and dispose it.
Before any requested engine is timed, execute the same oracle sequence and
assert a DataVo-reactive one-batch probe produces its three hashes. This keeps
the approved DataVo-reactive equivalence guarantee while allowing a targeted
`--engine sqlite` diagnostic run to remain self-validating.
Persist `engineId/repetition-N.json` containing seed, profile, engine,
architecture, schema/query revision `arena-live-views-v1`, total time,
batch/view P50/P95/P99, writes, reactive changes or polling rows, all hashes,
OS, CPU, .NET version, and success status. Build percentile values from sorted
samples using index `ceil(percentile * count) - 1`, clamped to zero, for P50,
P95, and P99. A failed write, read, oracle hash, or artifact serialization
throws and leaves the command failed; no repetition is skipped.

`ArenaLiveViewsCommand` accepts exactly `--profile gameplay|ivm-stress`,
`--engine all|datavo-reactive|datavo-polling|sqlite|duckdb`,
`--repetitions <positive-int>`, `--output <absolute-or-relative-path>`, and
`--format markdown|json`. Default repetitions are 7; default output is
`artifacts/arena-live-views/<profile>-<UTC timestamp>`. It always writes
`summary.json` and `summary.md`, each listing every repetition path and median
total time; `--format` selects only the host's stdout representation. Add an
`arena-live-views` Program branch before generic `BenchmarkMetrics` handling;
it prints this scenario’s formatter directly and lists the scenario in the
unknown-scenario error.

- [ ] **Step 4: Verify the command on a small accepted run**

Run: `dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release --filter "ArenaLiveViewsScenarioRunnerTests|ArenaLiveViewsCommandTests"`

Run: `dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario arena-live-views --profile gameplay --engine all --repetitions 1 --output /private/tmp/datavo-arena-live-views-smoke --format markdown`

Expected: tests pass; the host prints all four engines, writes one artifact per
engine plus `summary.json`, and rejects no engine silently.

- [ ] **Step 5: Commit**

```bash
git add demos/Research.Benchmark/src/Research.Benchmark.Runners/ArenaLiveViews/ArenaLiveViewsScenarioRunner.cs \
  demos/Research.Benchmark/src/Research.Benchmark.Host/ArenaLiveViewsCommand.cs \
  demos/Research.Benchmark/src/Research.Benchmark.Host/Program.cs \
  demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsScenarioRunnerTests.cs \
  demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsCommandTests.cs
git commit -m "feat: add desktop arena live-view benchmark"
```

### Task 5: Add Unity macOS SQLite boundary and cross-engine workload

**Files:**
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineContracts.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineWorkload.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineRunner.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/Sqlite/ArenaSqliteNative.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/Sqlite/ArenaSqlitePollingSession.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaCrossEngineEditModeTests.cs`
- Modify: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaVisualBootstrap.cs`

**Consumes:** Approved profile constants and canonical encoding from Task 1; existing DataVo Unity candidate and arena DataVo session patterns.

**Produces:** Unity-safe `datavo-reactive`, `datavo-polling`, and `sqlite` cross-engine results with exactly three hashes and a recorded SQLite library version.

- [ ] **Step 1: Write failing Unity hash and SQLite proof tests**

```csharp
[Test]
public void SameProfileAndBatch_ProducesTheDesktopCompatibleHashFormat()
{
    ArenaCrossEngineProfile profile = ArenaCrossEngineProfile.Gameplay;
    ArenaCrossEngineHashes first = ArenaCrossEngineWorkload.Hash(
        ArenaCrossEngineWorkload.CreateExpectedViews(profile, batchCount: 3));
    ArenaCrossEngineHashes second = ArenaCrossEngineWorkload.Hash(
        ArenaCrossEngineWorkload.CreateExpectedViews(profile, batchCount: 3));

    Assert.That(first, Is.EqualTo(second));
    Assert.That(first.ZoneInventory.Length, Is.EqualTo(64));
}

[Test]
public void SqlitePollingSession_ReportsLibraryVersionAndMatchesDataVoPolling()
{
    string sqliteVersion = ArenaSqliteNative.GetLibraryVersion();
    ArenaCrossEngineResult sqlite = ArenaCrossEngineRunner.Run(
        ArenaCrossEngineProfile.Gameplay, ArenaCrossEngineKind.SqlitePolling, repetitions: 1);
    ArenaCrossEngineResult dataVo = ArenaCrossEngineRunner.Run(
        ArenaCrossEngineProfile.Gameplay, ArenaCrossEngineKind.DataVoPolling, repetitions: 1);

    Assert.That(sqliteVersion, Is.Not.Empty);
    Assert.That(sqlite.status, Is.EqualTo("passed"));
    Assert.That(sqlite.sqliteVersion, Is.EqualTo(sqliteVersion));
    Assert.That(sqlite.leaderboardChecksum, Is.EqualTo(dataVo.leaderboardChecksum));
    Assert.That(sqlite.aggregateChecksum, Is.EqualTo(dataVo.aggregateChecksum));
    Assert.That(sqlite.zoneInventoryChecksum, Is.EqualTo(dataVo.zoneInventoryChecksum));
}

[Test]
public void UnavailableSqliteBoundary_ProducesAnExplicitFailure()
{
    InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() =>
        ArenaSqliteNative.RequireAvailableForTest(() => throw new DllNotFoundException("sqlite3")));

    Assert.That(exception.Message, Is.EqualTo(
        "macOS sqlite3 native library is unavailable; Unity cross-engine SQLite benchmark cannot run."));
}
```

- [ ] **Step 2: Run the tests to verify red**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaCrossEngineEditModeTests`

Expected: compilation fails because the cross-engine workload and SQLite session do not exist.

- [ ] **Step 3: Implement the Unity-safe contracts, workload, and direct SQLite session**

Use `[Serializable]` field-based DTOs for `ArenaCrossEngineResult`,
`ArenaCrossEngineRepetition`, and `ArenaCrossEngineLatencySummary` so
`JsonUtility` serializes them. The top-level fields are `schemaVersion`,
`schemaRevision`, `status`, `engine`, `profile`, `seed`, `measuredBatches`,
`repetitions`, `leaderboardChecksum`, `aggregateChecksum`,
`zoneInventoryChecksum`, `unityVersion`, `platform`, `scriptingBackend`,
`sqliteVersion`, and bounded `error`. Each repetition includes its one-based
index, total milliseconds, writes applied, reactive changes, polling rows,
batch latency, and the three per-view latency summaries. A passed result has
the profile's exact measured-batch count, no error, all 64-character lowercase
hashes, and a nonempty SQLite version only for the `sqlite` engine.

`ArenaSqliteNative` declares only the required C API calls from library
`"sqlite3"`: `sqlite3_open_v2`, `sqlite3_close_v2`, `sqlite3_exec`,
`sqlite3_prepare_v2`, `sqlite3_bind_int`, `sqlite3_bind_text`, `sqlite3_step`,
`sqlite3_column_int`, `sqlite3_column_text`, `sqlite3_finalize`,
`sqlite3_errmsg`, and `sqlite3_libversion`. Define `SQLITE_OK = 0`,
`SQLITE_ROW = 100`, `SQLITE_DONE = 101`, and `SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY`.
Use `IntPtr` statement/database handles, `[MarshalAs(UnmanagedType.LPUTF8Str)]`
for SQL/text parameters, and pass `new IntPtr(-1)` (`SQLITE_TRANSIENT`) as the
destructor argument to every `sqlite3_bind_text` call so SQLite copies the
marshalled string before that buffer is released. `GetLibraryVersion` normalizes
`DllNotFoundException`, `EntryPointNotFoundException`, or an empty native value
through `RequireAvailableForTest(Func<string>)` into exactly `macOS sqlite3
native library is unavailable; Unity cross-engine SQLite benchmark cannot run.`
The session throws an `InvalidOperationException` containing the SQLite error
text for every non-OK/non-row/non-done status, finalizes every prepared
statement in `finally`, and closes its database in `Dispose`.

`ArenaCrossEngineWorkload` reproduces Task 1 profile IDs, fixture values, SQL
semantics, sorted UTF-8 line encoding, and SHA-256 lowercase hashes. It uses no
Unity vector types. `ArenaCrossEngineRunner.Run` creates a fresh DataVo or
SQLite engine every repetition, applies warm-up and measured batches, demands
all three views after every batch, records per-batch/view latency summaries, and
throws on an intra-engine hash change. DataVo reactive subscribes once;
DataVo polling and SQLite execute all three queries on each demand. Update
`ArenaVisualBootstrap.IsHeadlessArenaInvocation` to return true when either
`-datavoArenaMode` or `-datavoArenaEngine` is present so no visual scene starts
in the cross-engine Player.

- [ ] **Step 4: Run Unity verification**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaCrossEngineEditModeTests`

Run: `bash scripts/run-unity-arena-editor-tests.sh`

Expected: the macOS Editor can call system SQLite, cross-engine hashes are
equal for the gameplay test, and all existing arena EditMode tests pass.

- [ ] **Step 5: Commit**

```bash
git add demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineContracts.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineContracts.cs.meta \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineWorkload.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineWorkload.cs.meta \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineRunner.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineRunner.cs.meta \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/Sqlite.meta \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/Sqlite/ArenaSqliteNative.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/Sqlite/ArenaSqliteNative.cs.meta \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/Sqlite/ArenaSqlitePollingSession.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/Sqlite/ArenaSqlitePollingSession.cs.meta \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaCrossEngineEditModeTests.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaCrossEngineEditModeTests.cs.meta \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaVisualBootstrap.cs
git commit -m "feat: add Unity SQLite arena benchmark workload"
```

### Task 6: Add Unity cross-engine headless Player gate

**Files:**
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineBootstrap.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaCrossEngineBootstrapEditModeTests.cs`
- Create: `scripts/run-unity-macos-il2cpp-arena-cross-engine.sh`
- Modify: `scripts/test-unity-arena-project.sh`
- Modify: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaPlayerBuild.cs`

**Consumes:** Task 5 runner/result and existing `ArenaPlayerBuild`/IL2CPP script safety patterns.

**Produces:** A real Medium-stripping Player gate for every Unity engine/profile pair, with settings/lock preservation and cross-engine hash validation.

- [ ] **Step 1: Write failing invocation and static contract tests**

```csharp
[Test]
public void Invocation_RequiresKnownEngineProfileAndSafeRunId()
{
    ArenaCrossEngineInvocation invocation = ArenaCrossEngineBootstrap.ParseInvocation(new[]
    {
        "-datavoArenaEngine", "sqlite",
        "-datavoArenaProfile", "gameplay",
        "-datavoArenaRunId", "sqlite_gameplay_01",
        "-datavoArenaRepetitions", "1"
    });

    Assert.That(invocation.engine, Is.EqualTo(ArenaCrossEngineKind.SqlitePolling));
    Assert.That(invocation.profile.Id, Is.EqualTo("gameplay"));
    Assert.That(invocation.repetitions, Is.EqualTo(1));
}
```

Add shell assertions requiring `ArenaCrossEngineBootstrap`, `sqlite3_open_v2`,
`-datavoArenaEngine`, `-datavoArenaProfile`, and
`run-unity-macos-il2cpp-arena-cross-engine.sh` by extending the existing static
contract with these exact lines:

```bash
rg -Fq 'ArenaCrossEngineBootstrap' "$project/Assets/DataVoArena/Runtime/ArenaCrossEngineBootstrap.cs"
rg -Fq 'sqlite3_open_v2' "$project/Assets/DataVoArena/Runtime/Sqlite/ArenaSqliteNative.cs"
rg -Fq -- '-datavoArenaEngine' "$project/Assets/DataVoArena/Runtime/ArenaCrossEngineBootstrap.cs"
rg -Fq -- '-datavoArenaProfile' "$project/Assets/DataVoArena/Runtime/ArenaCrossEngineBootstrap.cs"
test -x "$root/scripts/run-unity-macos-il2cpp-arena-cross-engine.sh"
```

- [ ] **Step 2: Run the tests to verify red**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaCrossEngineBootstrapEditModeTests`

Run: `bash scripts/test-unity-arena-project.sh`

Expected: the Editor test does not compile and the static contract fails because
the bootstrap and cross-engine runner do not exist.

- [ ] **Step 3: Implement Player parsing, result persistence, and shell gate**

`ArenaCrossEngineBootstrap` activates only when `-datavoArenaEngine` is present.
It accepts exactly `datavo-reactive|datavo-polling|sqlite`,
`gameplay|ivm-stress`, a safe `[A-Za-z0-9_-]{1,128}` run id, and repetitions
from 1 through 7. The checked-in IL2CPP runner always passes one repetition so
each independent repetition has its own Player process; the parser permits a
larger local diagnostic request only within that explicit bound. It runs one
engine/profile summary, sets Unity runtime
metadata, writes exactly one JSON file at
`Application.persistentDataPath/DataVoArenaCrossEngine/<run-id>/result.json`,
logs exactly one `DATAVO_ARENA_CROSS_ENGINE_RESULT=<absolute-path>`, and exits
zero only for passed internally consistent results. Failure JSON has bounded
single-line text and exits one.

The shell runner follows the existing IL2CPP runner’s candidate preparation,
lock hash, tracked-settings hash, build-once, unique executable, timeout, and
diagnostic behavior. It calls `ArenaPlayerBuild.BuildMacOSIl2Cpp`, then makes
42 Player invocations: seven fresh one-repetition processes for every pair in
this order: `gameplay` × `datavo-reactive`, `datavo-polling`, `sqlite`, then
`ivm-stress` × the same engines. Every invocation has a unique run id,
`-datavoArenaRepetitions 1`, and a 120-second process limit. This is deliberate:
the seven repetitions must not share a Unity process, database, or session.

The Node validator requires `schemaVersion === 1`,
`schemaRevision === 'arena-live-views-v1'`, `status === 'passed'`,
`platform === 'OSXPlayer'`, `scriptingBackend === 'IL2CPP'`, exact requested
engine/profile, positive `measuredBatches` matching the profile, one
repetition, nonempty SQLite version only for `sqlite`, and lowercase
64-character hashes. It writes `$output_root/summary.json` containing all 42
raw report paths, per-engine/profile medians calculated from seven total times,
and the three hashes for every report. After each profile, it requires all
three engines’ leaderboard, aggregate, and zone-inventory hashes to be
identical for every repetition number. It prints every Editor/Player log path
and relevant log excerpts on any parse, Player, artifact, or equivalence
failure.

Mark the new shell runner executable before its first test:

```bash
chmod +x scripts/run-unity-macos-il2cpp-arena-cross-engine.sh
```

Keep `ArenaPlayerBuild`’s empty generated scene and setting restoration; update
its comment to name both headless bootstraps rather than adding a visual
component to the Player scene.

- [ ] **Step 4: Run actual Unity Player verification**

Run: `bash scripts/test-unity-arena-project.sh`

Run: `bash scripts/run-unity-arena-editor-tests.sh`

Run: `bash scripts/run-unity-macos-il2cpp-arena-cross-engine.sh`

Expected: static and Editor tests pass; 42 IL2CPP Player artifacts pass; both
profiles have equal hashes across the three engines; settings and lock hashes
remain unchanged.

- [ ] **Step 5: Commit**

```bash
git add demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineBootstrap.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaCrossEngineBootstrap.cs.meta \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaCrossEngineBootstrapEditModeTests.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaCrossEngineBootstrapEditModeTests.cs.meta \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaPlayerBuild.cs \
  scripts/run-unity-macos-il2cpp-arena-cross-engine.sh \
  scripts/test-unity-arena-project.sh
git commit -m "feat: prove Unity SQLite arena benchmark in IL2CPP"
```

### Task 7: Document both lanes and run the published matrices

**Files:**
- Modify: `docs/features/unity-and-godot.md`
- Modify: `docs/manual/performance/reproducing-benchmarks.md`
- Modify: `demos/Research.Benchmark/README.md`
- Create: `demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsDocumentationTests.cs`
- Modify: `docs/superpowers/specs/2026-07-22-unity-arena-cross-engine-benchmark-design.md` only if implementation discovered an explicit platform limitation that changes the approved claim.

**Consumes:** Task 4 desktop artifacts and Task 6 Unity artifacts.

**Produces:** Reproduction instructions that label each result lane, preserve raw artifact paths, and do not make cross-lane claims.

- [ ] **Step 1: Write failing documentation-contract assertions**

Add `ArenaLiveViewsDocumentationTests` that reads all three documentation files
and asserts they
contain the desktop command, the Unity cross-engine command, `gameplay`,
`ivm-stress`, `Desktop .NET lane`, `Unity macOS IL2CPP lane`,
`DuckDB is desktop-only`, and `do not compare Unity IL2CPP values directly with
desktop .NET values` for the new matrix.

- [ ] **Step 2: Run the test to verify red**

Run: `dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release --filter ArenaLiveViewsDocumentationTests`

Expected: failure because the cross-engine commands and lane interpretation text
are absent.

- [ ] **Step 3: Write exact reproduction and interpretation documentation**

Document these commands, with a user-chosen output directory outside tracked
source files:

```bash
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- \
  --scenario arena-live-views --profile gameplay --engine all --repetitions 7 \
  --output /private/tmp/datavo-arena-gameplay --format markdown

dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- \
  --scenario arena-live-views --profile ivm-stress --engine all --repetitions 7 \
  --output /private/tmp/datavo-arena-ivm-stress --format markdown

bash scripts/run-unity-macos-il2cpp-arena-cross-engine.sh
```

Use the exact headings `Desktop .NET lane` and `Unity macOS IL2CPP lane`. State
that the desktop table compares DataVo reactive, DataVo polling, SQLite, and
DuckDB on .NET; the Unity table compares only DataVo reactive, DataVo polling,
and macOS system SQLite in IL2CPP; `DuckDB is desktop-only`; vector search is
separately measured; and all raw JSON paths are part of the result. Include
this exact single-line warning:

```text
do not compare Unity IL2CPP values directly with desktop .NET values
```

- [ ] **Step 4: Run documentation and complete verification**

Run serially:

```bash
dotnet build DataVo.Core/DataVo.Core.csproj -c Release
dotnet build DataVo.Core/DataVo.Core.csproj -c Release -p:DataVoEnablePortableTarget=true
dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release
bash scripts/test-unity-arena-project.sh
bash scripts/run-unity-arena-editor-tests.sh
bash scripts/run-unity-macos-il2cpp-arena.sh
bash scripts/run-unity-macos-il2cpp-arena-cross-engine.sh
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario arena-live-views --profile gameplay --engine all --repetitions 7 --output /private/tmp/datavo-arena-gameplay-final --format markdown
dotnet run -c Release --project demos/Research.Benchmark/src/Research.Benchmark.Host -- --scenario arena-live-views --profile ivm-stress --engine all --repetitions 7 --output /private/tmp/datavo-arena-ivm-stress-final --format markdown
git diff --check HEAD
```

Expected: every test/gate passes, each matrix has all required engines for its
lane, every same-profile engine hash matches, and diff check emits no output.
Report the existing `TopKReactiveQuery` nullable warnings as pre-existing if
they remain.

- [ ] **Step 5: Commit**

```bash
git add docs/features/unity-and-godot.md \
  docs/manual/performance/reproducing-benchmarks.md \
  demos/Research.Benchmark/README.md \
  demos/Research.Benchmark/tests/Research.Benchmark.Tests/ArenaLiveViewsDocumentationTests.cs
git commit -m "docs: document cross-engine arena benchmark lanes"
```

## Plan self-review

- **Spec coverage:** Tasks 1–4 implement the deterministic desktop matrix,
  seven-repetition artifacts, and DuckDB’s desktop-only role. Tasks 5–6 prove
  the direct SQLite native boundary in actual macOS IL2CPP and validate three
  hash outputs across both profiles. Task 7 documents lane separation and
  reproduction.
- **Scope control:** The plan contains no DuckDB Unity path, no Unity vector
  comparison, no extra platform claim, and no persistence/Burst/Jobs work.
- **Type consistency:** Every desktop engine uses `IArenaLiveViewsEngine`; every
  view result is represented by `ArenaLiveViewsViews`; Unity uses separate
  JsonUtility DTOs but emits the same profile IDs and hash format.
- **TDD and verification:** Each implementation task starts with a concrete
  failing test, has a focused green command, commits independently, and ends
  with a full serial verification before publication.
