# Unity NPC Memory Arena and IVM Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a standalone, self-running Unity arena that visibly uses DataVo vector-memory search and fairly measures reactive incremental view maintenance against identical full-query polling.

**Architecture:** A new Unity 6000.5.4f1 project at `demos/unity/DataVo.UnityArenaDemo` owns its visual scene, deterministic simulation, and short Unity EditMode tests. A pure orchestration layer seeds the same tables and mutation schedule for each delivery mode; the only intentional difference is DataVo subscriptions plus `DispatchPendingNotifications()` versus re-querying the same views. Unity `MonoBehaviour` components consume snapshots from that layer and render primitive NPC markers and an IMGUI HUD; a headless player writes bounded JSON artifacts.

**Tech Stack:** Unity 6000.5.4f1; C#; DataVo portable Unity candidate; `com.datavo.unity.mathematics`; Unity Mathematics 1.4.0; NUnit EditMode tests; Unity IL2CPP macOS Player; Bash and Node.js contract runners.

## Global Constraints

- Keep `tests/unity/DataVo.UnitySmoke` as a compact compatibility test; do not add arena code to it.
- Add the new project at `demos/unity/DataVo.UnityArenaDemo`; use primitive GameObjects and IMGUI only, with no external art, input system, LLM, native database plugin, or network service.
- Pin Unity to `6000.5.4f1`, use the existing runtime BCL inventory, and prepare an owned `.generated` candidate symlink before each Unity run.
- V1 uses a `FLAT` index over `VECTOR(32)`, not HNSW. It must return an exact seeded nearest memory.
- Reactive and polling use the same seed, schema, source data, mutation schedule, SQL, vector-search schedule, and presentation model.
- The comparison is maintained deltas versus full-query polling, not DataVo versus SQLite. Do not assert that reactive is faster.
- Exclude seeding, index build, scene creation, and warmup from measured ticks. Include writes, delivery/recompute, vector search, and presentation updates.
- DataVo access stays on the Unity main thread. Burst, Jobs, Windows validation, HNSW scale/recall, networking, and LLM embeddings are not in this plan.
- Preserve user-owned untracked files; stage exact paths. Force-stage the final ignored plan document only.

---

## Target File Structure

| Path | Responsibility |
| --- | --- |
| `demos/unity/DataVo.UnityArenaDemo/Packages/manifest.json` | Pinned Unity dependencies and local DataVo packages. |
| `demos/unity/DataVo.UnityArenaDemo/ProjectSettings/*` | Unity-generated project settings. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaContracts.cs` | Options, modes, snapshots, metrics, and JSON report contract. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaSeed.cs` | Fixed-seed game data, `VECTOR(32)` memory vectors, and mutations. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaDataSession.cs` | DataVo schema, vector index/search, and shared query text. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaDeliveryRunner.cs` | Reactive/polling execution, work counters, and checksums. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaVisualController.cs` | Primitive scene, moving NPC markers, colours, and IMGUI HUD. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaHeadlessBootstrap.cs` | Finite command-line execution and one JSON result. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/*Tests.cs` | Unity EditMode tests. |
| `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaPlayerBuild.cs` | macOS IL2CPP player build. |
| `scripts/prepare-unity-arena-candidate.sh` | Owned candidate symlink preparation. |
| `scripts/check-unity-arena-project.sh` | Package/project/candidate static validation. |
| `scripts/run-unity-arena-editor-tests.sh` | Batch EditMode runner. |
| `scripts/run-unity-macos-il2cpp-arena.sh` | Two-mode real IL2CPP player runner. |
| `scripts/test-unity-arena-project.sh` | Fast source contract. |

## Shared Interfaces

All runtime source uses namespace `DataVo.UnityArena`.

```csharp
public enum ArenaDeliveryMode { Reactive, Polling }

public sealed record ArenaRunOptions
{
    public int Seed { get; init; } = 20260721;
    public int LogicalNpcCount { get; init; } = 1_000;
    public int VisibleNpcCount { get; init; } = 48;
    public int MemoriesPerNpc { get; init; } = 8;
    public int WarmupTicks { get; init; } = 30;
    public int MeasuredTicks { get; init; } = 300;
    public int MutationsPerTick { get; init; } = 24;
    public int MemorySearchEveryTicks { get; init; } = 12;
    public ArenaDeliveryMode Mode { get; init; }
}

public sealed class ArenaRunResult
{
    public int schemaVersion;
    public string status = string.Empty;
    public string mode = string.Empty;
    public int seed;
    public int measuredTicks;
    public string leaderboardChecksum = string.Empty;
    public string aggregateChecksum = string.Empty;
    public long reactiveChanges;
    public long pollingRows;
    public ArenaLatencySummary tick = new();
    public ArenaLatencySummary leaderboard = new();
    public ArenaLatencySummary vectorSearch = new();
    public string unityVersion = string.Empty;
    public string platform = string.Empty;
    public string scriptingBackend = string.Empty;
    public string error = string.Empty;
}

public sealed class ArenaDeliveryRunner : IDisposable
{
    public ArenaDeliveryRunner(ArenaRunOptions options);
    public void Warmup();
    public ArenaFrameSnapshot Step();
    public ArenaRunResult Complete();
    public void Dispose();
}

public static class ArenaBenchmark
{
    public static ArenaRunResult Run(ArenaRunOptions options);
    public static void RequireEquivalent(ArenaRunResult reactive, ArenaRunResult polling);
}
```

`ArenaFrameSnapshot` contains top-20 leaderboard rows, team aggregate rows,
selected NPC memory matches, and visible NPC state. It is the only rendering
input; visual components never call DataVo directly.

---

### Task 1: Bootstrap the independent Unity project and candidate contract

**Files:**
- Create: `demos/unity/DataVo.UnityArenaDemo/Packages/manifest.json`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/DataVo.UnityArena.Runtime.asmdef`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/DataVo.UnityArena.EditorTests.asmdef`
- Create: `demos/unity/DataVo.UnityArenaDemo/ProjectSettings/ProjectVersion.txt`
- Create: `demos/unity/DataVo.UnityArenaDemo/ProjectSettings/ProjectSettings.asset`
- Create: `scripts/prepare-unity-arena-candidate.sh`
- Create: `scripts/check-unity-arena-project.sh`
- Create: `scripts/test-unity-arena-project.sh`
- Create: `scripts/run-unity-arena-editor-tests.sh`

**Consumes:** `scripts/prepare-unity-candidate.sh`, `scripts/check-unity-runtime-bcl.sh`, and `integrations/unity/com.datavo.unity.mathematics`.

**Produces:** A batch-openable project with an owned candidate symlink. No generated candidate or `Library/` content is committed.

- [ ] **Step 1: Write the failing static project contract**

```bash
#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project="$root/demos/unity/DataVo.UnityArenaDemo"
for file in "$project/Packages/manifest.json" \
  "$project/ProjectSettings/ProjectVersion.txt" \
  "$project/Assets/DataVoArena/Runtime/DataVo.UnityArena.Runtime.asmdef" \
  "$project/Assets/DataVoArena/Editor/DataVo.UnityArena.EditorTests.asmdef" \
  "$root/scripts/prepare-unity-arena-candidate.sh" \
  "$root/scripts/check-unity-arena-project.sh"; do
  test -f "$file" || { echo "Missing Unity arena file: $file" >&2; exit 1; }
done
```

- [ ] **Step 2: Run the contract to verify it fails**

Run: `bash scripts/test-unity-arena-project.sh`

Expected: nonzero exit naming the missing arena project file.

- [ ] **Step 3: Create project, packages, and asmdefs**

Create the project using Unity `6000.5.4f1` with `-createProject`, then retain
only required `Assets`, `Packages`, and `ProjectSettings` files. Add the local
package entries below alongside Unity-generated module dependencies:

```json
"com.unity.mathematics": "1.4.0",
"com.unity.test-framework": "1.7.0",
"com.datavo.core.candidate": "file:../.generated",
"com.datavo.unity.mathematics": "file:../../../../integrations/unity/com.datavo.unity.mathematics"
```

The runtime asmdef must reference `DataVo.Unity.TypeBinding`,
`DataVo.Unity.EngineTypes`, and `DataVo.Unity.Mathematics`, set
`overrideReferences: true`, and list `DataVo.Core.dll` as its precompiled
reference. The test asmdef references the runtime assembly,
`UnityEditor.TestRunner`, and `UnityEngine.TestRunner`.

Implement candidate preparation with the existing proven safety sequence:

```bash
candidate_link="$root/demos/unity/DataVo.UnityArenaDemo/.generated"
candidate_root="$(mktemp -d /private/tmp/datavo-unity-arena-candidate.XXXXXX)"
candidate_output="$candidate_root/com.datavo.core.candidate"
bash "$root/scripts/check-unity-runtime-bcl.sh"
bash "$root/scripts/prepare-unity-candidate.sh" --output "$candidate_output"
ln -sfn "$candidate_output" "$candidate_link"
```

Reject a non-symlink at `candidate_link`. `check-unity-arena-project.sh` parses
manifest and lock JSON, validates Unity `6000.5.4f1`, Mathematics `1.4.0`, test
framework `1.7.0`, both local packages, and—in `--require-candidate` mode—the
candidate `package.json` and `candidate-manifest.json`.

Create `run-unity-arena-editor-tests.sh` from the existing smoke runner: hash
the arena `packages-lock.json`, verify the BCL, prepare the arena candidate, run
Unity `EditMode` tests, require a passed XML result with zero failures, require
the candidate project check, and fail if the lock hash changes. This runner is
created now so every later task can use its focused `--filter` option.

- [ ] **Step 4: Extend the source contract and verify green**

Add these checks then run the script:

```bash
rg -Fq '"com.datavo.core.candidate": "file:../.generated"' "$project/Packages/manifest.json"
rg -Fq '"com.unity.mathematics": "1.4.0"' "$project/Packages/manifest.json"
rg -Fq 'check-unity-runtime-bcl.sh' "$root/scripts/prepare-unity-arena-candidate.sh"
rg -Fq 'DataVo.Unity.TypeBinding' "$project/Assets/DataVoArena/Runtime/DataVo.UnityArena.Runtime.asmdef"
test -f "$root/scripts/run-unity-arena-editor-tests.sh"
echo "Unity arena project contract passed."
```

Run: `bash scripts/test-unity-arena-project.sh`

Expected: `Unity arena project contract passed.`

- [ ] **Step 5: Commit**

```bash
git add demos/unity/DataVo.UnityArenaDemo scripts/prepare-unity-arena-candidate.sh \
  scripts/check-unity-arena-project.sh scripts/test-unity-arena-project.sh \
  scripts/run-unity-arena-editor-tests.sh
git commit -m "feat: scaffold Unity arena demo"
```

---

### Task 2: Add deterministic seed data, options, and latency summaries

**Files:**
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaContracts.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaSeed.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaLatencyRecorder.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaSeedEditModeTests.cs`

**Consumes:** Runtime assembly from Task 1.

**Produces:** Validated options, stable 32-dimensional gameplay memory vectors,
repeatable mutations, snapshots, checksums, and P50/P95/P99 summaries.

- [ ] **Step 1: Write failing deterministic tests**

```csharp
[Test]
public void MemoryVector_IsAlwaysThirtyTwoFiniteComponents()
{
    float[] vector = ArenaSeed.MemoryVector(npcId: 7, memoryIndex: 3);
    Assert.That(vector, Has.Length.EqualTo(32));
    Assert.That(vector.All(float.IsFinite), Is.True);
}

[Test]
public void MutationSchedule_ReplaysForTheSameSeed()
{
    var first = ArenaSeed.CreateMutations(42, 100, 9, 12).ToArray();
    var second = ArenaSeed.CreateMutations(42, 100, 9, 12).ToArray();
    Assert.That(second, Is.EqualTo(first));
}

[Test]
public void LatencySummary_UsesNearestRankPercentiles()
{
    var recorder = new ArenaLatencyRecorder();
    foreach (double value in new[] { 1d, 2d, 3d, 4d, 5d }) recorder.AddMilliseconds(value);
    Assert.That(recorder.Snapshot().p95Milliseconds, Is.EqualTo(5d));
}
```

- [ ] **Step 2: Run the test to verify red**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaSeedEditModeTests`

Expected: compile/test failure because contracts, seed, recorder, and runner are absent.

- [ ] **Step 3: Implement the deterministic core**

Define immutable records `ArenaNpcSeed`, `ArenaMemorySeed`, `ArenaMutation`,
`ArenaLeaderboardEntry`, `ArenaMemoryMatch`, and `ArenaFrameSnapshot`.
`ArenaSeed.MemoryVector` derives each component only from npc id, memory index,
and component index, normalizes it, and reserves the first four memories for
orthogonal tags `danger`, `ally`, `loot`, and `objective`.

`ArenaRunOptions.Validate()` rejects nonpositive counts, a zero search interval,
visible count above logical count, and `MemoriesPerNpc * LogicalNpcCount`
overflow. `ArenaLatencyRecorder` stores milliseconds calculated from
`Stopwatch.GetTimestamp()` and returns zero—not `NaN`—for an empty snapshot.

- [ ] **Step 4: Run focused test to verify green**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaSeedEditModeTests`

Expected: all three tests pass with zero failures.

- [ ] **Step 5: Commit**

```bash
git add demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaContracts.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaSeed.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaLatencyRecorder.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaSeedEditModeTests.cs
git commit -m "feat: add deterministic Unity arena workload"
```

---

### Task 3: Implement DataVo schema and exact NPC vector-memory retrieval

**Files:**
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaDataSession.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaVectorSearchEditModeTests.cs`
- Modify: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaContracts.cs`

**Consumes:** `ArenaSeed`, `ArenaRunOptions`, Unity type binding, and `DataVoContext.SearchNearest(string, string, float[], int)`.

**Produces:** A disposable in-memory DataVo session with shared SQL text and `FindNearestMemories(int npcId, int topK)`.

- [ ] **Step 1: Write failing vector-session tests**

```csharp
[Test]
public void SeededDangerStimulus_ReturnsDangerAsNearestMemory()
{
    using var session = new ArenaDataSession(new ArenaRunOptions { LogicalNpcCount = 8, MemoriesPerNpc = 4 });
    ArenaMemoryMatch nearest = session.FindNearestMemories(1, 1).Single();
    Assert.That(nearest.tag, Is.EqualTo("danger"));
}

[Test]
public void PlayerPosition_UsesRegisteredUnityVector3Converter()
{
    using var session = new ArenaDataSession(new ArenaRunOptions { LogicalNpcCount = 8, MemoriesPerNpc = 4 });
    Vector3 position = session.ReadPlayerPosition(1);
    Assert.That(position, Is.EqualTo(ArenaSeed.CreatePlayers(8).First().position));
}
```

- [ ] **Step 2: Run the test to verify red**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaVectorSearchEditModeTests`

Expected: compile/test failure because `ArenaDataSession` is absent.

- [ ] **Step 3: Implement `ArenaDataSession`**

Create an in-memory `DataVoContext`, call `UseUnityTypes()`, create/use database
`UnityArena`, and create this schema exactly:

```sql
CREATE TABLE Players (
  Id INT PRIMARY KEY, Team VARCHAR(10), Zone VARCHAR(20),
  Position VECTOR(3), Health INT, Score INT
);
CREATE TABLE Inventory (
  InventoryId INT PRIMARY KEY, PlayerId INT, ItemId INT, Slot INT
);
CREATE TABLE NpcMemories (
  Id INT PRIMARY KEY, NpcId INT, Tag VARCHAR(20), Embedding VECTOR(32)
);
CREATE INDEX IX_NpcMemories_Embedding ON NpcMemories (Embedding) USING FLAT;
```

Seed all data before measured work. Insert players through a typed model so
`Vector3` conversion is exercised; store memories as `float[]` for `VECTOR(32)`.
Expose exactly these shared query constants:

```csharp
public const string LeaderboardSql = "SELECT Id, Score FROM Players ORDER BY Score DESC LIMIT 20";
public const string AliveByTeamSql = "SELECT Team, COUNT(*) AS Alive FROM Players WHERE Health > 0 GROUP BY Team";
public const string ZoneInventorySql = "SELECT p.Id, i.ItemId FROM Players p JOIN Inventory i ON p.Id = i.PlayerId WHERE p.Zone = 'arena-7'";
```

`FindNearestMemories` calls
`context.SearchNearest("NpcMemories", "IX_NpcMemories_Embedding", vector, topK)`,
materializes ids/tags, and throws `InvalidOperationException` on a missing
seeded result. `ReadPlayerPosition` must call
`context.Get<Vector3>(row, "Position")`, not decode a float array manually.

- [ ] **Step 4: Run focused and regression tests to verify green**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaVectorSearchEditModeTests`

Expected: both arena vector tests pass.

Run: `bash scripts/run-unity-editor-tests.sh`

Expected: existing Unity smoke stays green.

- [ ] **Step 5: Commit**

```bash
git add demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaDataSession.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaContracts.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaVectorSearchEditModeTests.cs
git commit -m "feat: add Unity arena NPC memory search"
```

---

### Task 4: Add equivalent reactive and polling leaderboard delivery modes

**Files:**
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaDeliveryRunner.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaDeliveryEditModeTests.cs`
- Modify: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaDataSession.cs`

**Consumes:** shared SQL constants, `ArenaSeed.CreateMutations`, and `QueryChange` callbacks.

**Produces:** Equivalent reactive and polling final leaderboard/aggregate checksums plus mode-specific work counters.

- [ ] **Step 1: Write failing equality and work-accounting tests**

```csharp
[Test]
public void ReactiveAndPolling_ProduceSameFinalView()
{
    var common = new ArenaRunOptions { LogicalNpcCount = 64, MemoriesPerNpc = 4, WarmupTicks = 2, MeasuredTicks = 20, MutationsPerTick = 4 };
    ArenaRunResult reactive = ArenaBenchmark.Run(common with { Mode = ArenaDeliveryMode.Reactive });
    ArenaRunResult polling = ArenaBenchmark.Run(common with { Mode = ArenaDeliveryMode.Polling });
    Assert.That(reactive.leaderboardChecksum, Is.EqualTo(polling.leaderboardChecksum));
    Assert.That(reactive.aggregateChecksum, Is.EqualTo(polling.aggregateChecksum));
}

[Test]
public void DeliveryModes_ReportOnlyTheirOwnWorkCounter()
{
    ArenaRunResult reactive = ArenaBenchmark.Run(new ArenaRunOptions { Mode = ArenaDeliveryMode.Reactive, LogicalNpcCount = 32, MemoriesPerNpc = 4, MeasuredTicks = 5 });
    ArenaRunResult polling = ArenaBenchmark.Run(new ArenaRunOptions { Mode = ArenaDeliveryMode.Polling, LogicalNpcCount = 32, MemoriesPerNpc = 4, MeasuredTicks = 5 });
    Assert.That(reactive.reactiveChanges, Is.GreaterThan(0));
    Assert.That(reactive.pollingRows, Is.EqualTo(0));
    Assert.That(polling.pollingRows, Is.GreaterThan(0));
    Assert.That(polling.reactiveChanges, Is.EqualTo(0));
}
```

- [ ] **Step 2: Run the test to verify red**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaDeliveryEditModeTests`

Expected: compile/test failure because `ArenaBenchmark` and runner are absent.

- [ ] **Step 3: Implement modes around the same state and SQL**

Reactive setup subscribes to `LeaderboardSql`, `AliveByTeamSql`, and
`ZoneInventorySql`; callbacks update presentation state from `Added`, `Updated`,
and `Removed` rows. Polling setup creates no subscriptions. Each `Step()`:

1. creates mutations from `(seed, tick)` and applies identical player/inventory writes;
2. times only delivery: one `DispatchPendingNotifications()` in reactive mode,
   or execution of each shared SQL query and full presentation rebuild in polling;
3. records callback changes or returned polling rows respectively;
4. performs identical scheduled `FindNearestMemories` work in both modes;
5. creates one `ArenaFrameSnapshot` from presentation state.

Build checksum input with culture-invariant newline-joined `Id:Score` rows and
`Team:Alive` rows, then SHA-256. `Complete()` reports results but never compares
speed. `ArenaBenchmark.RequireEquivalent` rejects different checksum values.

- [ ] **Step 4: Run focused and full arena tests to verify green**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaDeliveryEditModeTests`

Expected: both tests pass with zero failures.

Run: `bash scripts/run-unity-arena-editor-tests.sh`

Expected: all Arena EditMode tests pass.

- [ ] **Step 5: Commit**

```bash
git add demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaDeliveryRunner.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaDataSession.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaDeliveryEditModeTests.cs
git commit -m "feat: compare Unity arena reactive and polling views"
```

---

### Task 5: Render the self-running visual arena and diagnostics HUD

**Files:**
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaVisualController.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaVisualBootstrap.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaVisualEditModeTests.cs`

**Consumes:** `ArenaDeliveryRunner` and `ArenaFrameSnapshot`.

**Produces:** A no-input scene that shows coloured moving NPCs, selected memory, live leaderboard, mode, and metrics. Rendering performs no database access.

- [ ] **Step 1: Write failing visual-controller tests**

```csharp
[Test]
public void BehaviourColours_AreStableAndDistinct()
{
    Assert.That(ArenaVisualController.ColourFor("patrol"), Is.Not.EqualTo(ArenaVisualController.ColourFor("flee")));
    Assert.That(ArenaVisualController.ColourFor("assist"), Is.Not.EqualTo(ArenaVisualController.ColourFor("pursue")));
}

[Test]
public void RenderSnapshot_TakesOnlyAFrameSnapshot()
{
    Assert.That(typeof(ArenaVisualController).GetMethod("RenderSnapshot").GetParameters(), Has.Length.EqualTo(1));
}
```

- [ ] **Step 2: Run the test to verify red**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaVisualEditModeTests`

Expected: compile/test failure because the controller is absent.

- [ ] **Step 3: Implement the asset-free visual scene**

`ArenaVisualBootstrap` creates the controller only when no `-datavoArenaMode`
command-line option is present. The controller creates a camera, directional
light, ground plane, one player sphere, and `VisibleNpcCount` capsule markers.
It owns one runner, calls `Step()` at a fixed simulation interval, and updates
marker positions from `ArenaFrameSnapshot`.

Use this immutable behaviour mapping:

```csharp
"danger" => ("flee", Color.red),
"ally"   => ("assist", Color.green),
"loot"   => ("pursue", Color.yellow),
_         => ("patrol", Color.cyan)
```

Implement `OnGUI()` with the active delivery mode, tick/leaderboard/vector P99,
reactive change count or polling row count, top-20 leaderboard rows, selected
NPC id/behaviour, and at least three nearest-memory tags. No keyboard input is
added. If visual mode alternates modes, dispose the old runner and create a new
one with the same seed before the next mode starts.

- [ ] **Step 4: Verify green and inspect the scene**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaVisualEditModeTests`

Expected: visual tests pass.

Run: `"/Applications/Unity/Hub/Editor/6000.5.4f1/Unity.app/Contents/MacOS/Unity" -projectPath demos/unity/DataVo.UnityArenaDemo`

Expected: a self-running primitive arena appears with no control requirement.

- [ ] **Step 5: Commit**

```bash
git add demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaVisualController.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaVisualBootstrap.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaVisualEditModeTests.cs
git commit -m "feat: add visual Unity NPC memory arena"
```

---

### Task 6: Add headless JSON reporting and the Unity batch test runner

**Files:**
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaHeadlessBootstrap.cs`
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaHeadlessEditModeTests.cs`
- Modify: `scripts/run-unity-arena-editor-tests.sh`
- Modify: `scripts/test-unity-arena-project.sh`

**Consumes:** `ArenaBenchmark`, candidate preparation, and project checker.

**Produces:** A finite player invocation that writes exactly one JSON artifact and a batch EditMode lane that preserves the package lock.

- [ ] **Step 1: Write failing headless result tests**

```csharp
[Test]
public void ShortRun_ReportsPassedSchemaAndMeasuredTicks()
{
    ArenaRunResult result = ArenaBenchmark.Run(new ArenaRunOptions
    {
        Mode = ArenaDeliveryMode.Reactive,
        LogicalNpcCount = 32,
        MemoriesPerNpc = 4,
        MeasuredTicks = 8
    });
    Assert.That(result.schemaVersion, Is.EqualTo(1));
    Assert.That(result.status, Is.EqualTo("passed"));
    Assert.That(result.tick.count, Is.EqualTo(8));
    Assert.That(result.error, Is.Empty);
}
```

- [ ] **Step 2: Run the test to verify red**

Run: `bash scripts/run-unity-arena-editor-tests.sh --filter ArenaHeadlessEditModeTests`

Expected: failure because result serialization and the batch runner are absent.

- [ ] **Step 3: Implement bootstrap and batch runner**

`ArenaHeadlessBootstrap` detects `-datavoArenaMode reactive|polling`, optional
`-datavoArenaTicks`, and mandatory `-datavoArenaRunId`. Reject unknown modes,
missing run ids, and run ids outside `[A-Za-z0-9_-]{1,128}`. It runs one mode,
writes one `result.json` under
`Application.persistentDataPath/DataVoArena/<run-id>/`, logs exactly one
`DATAVO_ARENA_RESULT=<absolute-path>`, and exits 0 only for passed internally
consistent results. On failure, write a bounded error result when possible and
exit 1.

Set Unity platform/version/backend in the report from runtime values. Record
allocation/GC only if a Unity `ProfilerRecorder` is valid; otherwise serialize a
missing value, not a fabricated zero measurement.

Create `run-unity-arena-editor-tests.sh` from the smoke runner’s safe structure:
hash the arena lock, check BCL, prepare only arena candidate, run batch EditMode
tests, require passed XML/zero failures, run project check with
`--require-candidate`, and verify the lock hash did not change. The static
contract must require `DATAVO_ARENA_RESULT=`, `-datavoArenaMode`,
`-datavoArenaRunId`, candidate preparation, and BCL checking.

- [ ] **Step 4: Run batch verification to confirm green**

Run: `bash scripts/run-unity-arena-editor-tests.sh`

Expected: all arena EditMode tests pass and the lock hash is unchanged.

- [ ] **Step 5: Commit**

```bash
git add demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Runtime/ArenaHeadlessBootstrap.cs \
  demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaHeadlessEditModeTests.cs \
  scripts/run-unity-arena-editor-tests.sh scripts/test-unity-arena-project.sh
git commit -m "feat: add Unity arena benchmark reporting"
```

---

### Task 7: Prove both modes in real macOS IL2CPP and document narrow usage

**Files:**
- Create: `demos/unity/DataVo.UnityArenaDemo/Assets/DataVoArena/Editor/ArenaPlayerBuild.cs`
- Create: `scripts/run-unity-macos-il2cpp-arena.sh`
- Modify: `scripts/test-unity-arena-project.sh`
- Modify: `docs/features/unity-and-godot.md`
- Modify: `docs/manual/performance/reproducing-benchmarks.md`

**Consumes:** Headless bootstrap, project checker, and the existing macOS smoke runner’s build/result pattern.

**Produces:** Actual IL2CPP result artifacts for reactive and polling runs with equal final-state checksums and honest reproduction documentation.

- [ ] **Step 1: Write the failing IL2CPP source contract**

Add these checks to `scripts/test-unity-arena-project.sh`:

```bash
rg -Fq 'BuildMacOSIl2Cpp' "$project/Assets/DataVoArena/Editor/ArenaPlayerBuild.cs"
rg -Fq 'ScriptingImplementation.IL2CPP' "$project/Assets/DataVoArena/Editor/ArenaPlayerBuild.cs"
rg -Fq 'ManagedStrippingLevel.Medium' "$project/Assets/DataVoArena/Editor/ArenaPlayerBuild.cs"
rg -Fq 'DATAVO_ARENA_RESULT=' "$project/Assets/DataVoArena/Runtime/ArenaHeadlessBootstrap.cs"
test -f "$root/scripts/run-unity-macos-il2cpp-arena.sh"
```

- [ ] **Step 2: Run the contract to verify red**

Run: `bash scripts/test-unity-arena-project.sh`

Expected: failure because the builder and runner are absent.

- [ ] **Step 3: Implement actual player proof**

`ArenaPlayerBuild.BuildMacOSIl2Cpp()` requires Unity Hub’s `mac-il2cpp` module,
creates a generated empty scene containing the runtime bootstrap, sets
Standalone backend to `IL2CPP` and stripping to `Medium`, builds to the required
`-datavoArenaBuildPath`, and restores both settings in `finally`.

`run-unity-macos-il2cpp-arena.sh` hashes `ProjectSettings.asset`, verifies BCL,
candidate, and project contract, builds one app, finds exactly one executable,
and runs it twice with separate run ids: `reactive` then `polling`. For each JSON
result require:

```js
report.schemaVersion === 1
report.status === 'passed'
report.platform === 'OSXPlayer'
report.scriptingBackend === 'IL2CPP'
report.measuredTicks > 0
report.leaderboardChecksum.length === 64
report.aggregateChecksum.length === 64
```

Require equal leaderboard and aggregate checksums across both results. Time out
within 120 seconds per player run, print Editor/player logs on failure, and fail
when the tracked settings hash changes.

- [ ] **Step 4: Run source, Editor, and actual player verification**

Run: `bash scripts/test-unity-arena-project.sh`

Expected: `Unity arena project contract passed.`

Run: `bash scripts/run-unity-arena-editor-tests.sh`

Expected: all arena EditMode tests pass.

Run: `bash scripts/run-unity-macos-il2cpp-arena.sh`

Expected: both result artifacts pass, final checksums match, and the script
prints `Unity macOS IL2CPP arena benchmark passed.`

- [ ] **Step 5: Document use and limits**

In `docs/features/unity-and-godot.md`, add an NPC Memory Arena subsection:
Unity 6000.5.4f1/macOS IL2CPP evaluation demo only; self-running primitive NPCs,
exact local vector-memory search, and reactive or polling leaderboard delivery.
Do not broaden production-support language.

In `docs/manual/performance/reproducing-benchmarks.md`, add:

```bash
bash scripts/run-unity-arena-editor-tests.sh
bash scripts/run-unity-macos-il2cpp-arena.sh
```

State: “The result compares DataVo incremental maintained views against
full-query polling with the same DataVo data and SQL. It is not a
DataVo-versus-SQLite benchmark; interpret it with its recorded seed, tick count,
and Unity backend.”

- [ ] **Step 6: Run final verification and commit**

Run serially, because concurrent portable target restores race on project assets:

```bash
dotnet build DataVo.Core/DataVo.Core.csproj -c Release
dotnet build DataVo.Core/DataVo.Core.csproj -c Release -p:DataVoEnablePortableTarget=true
bash scripts/test-unity-arena-project.sh
bash scripts/run-unity-arena-editor-tests.sh
bash scripts/run-unity-macos-il2cpp-arena.sh
git diff --check HEAD
```

Expected: zero errors; arena contract, EditMode, and IL2CPP player passes; no
diff-check output. Existing `TopKReactiveQuery` nullable warnings must be
reported as pre-existing rather than attributed to this feature.

```bash
git add demos/unity/DataVo.UnityArenaDemo scripts/prepare-unity-arena-candidate.sh \
  scripts/check-unity-arena-project.sh scripts/test-unity-arena-project.sh \
  scripts/run-unity-arena-editor-tests.sh scripts/run-unity-macos-il2cpp-arena.sh \
  docs/features/unity-and-godot.md docs/manual/performance/reproducing-benchmarks.md
git commit -m "feat: add Unity NPC memory arena benchmark"
```

---

## Plan Self-Review

- **Spec coverage:** Tasks 1–2 establish the separate project and deterministic
  workload; Task 3 covers exact `VECTOR(32)` memory retrieval; Task 4 proves
  equal reactive/polling state; Task 5 supplies the visible self-running arena;
  Tasks 6–7 add reports, batch validation, actual macOS IL2CPP evidence, and
  narrowly scoped documentation.
- **Scope control:** No claim or code path for SQLite comparison, HNSW scale,
  Burst/Jobs, Windows, external art, networking, or LLM embeddings.
- **Type consistency:** `ArenaRunOptions`, `ArenaRunResult`,
  `ArenaDeliveryRunner`, and `ArenaBenchmark` appear under Shared Interfaces
  and retain those names in every task.
- **TDD:** Each task begins with a concrete failing contract/EditMode test,
  then records the expected failure, adds the minimum implementation, reruns
  focused verification, and commits an independently usable slice.
