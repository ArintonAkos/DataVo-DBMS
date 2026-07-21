# Unity NPC Memory Arena and IVM Benchmark Design

## Goal

Create a separate Unity 6000.5.4f1 project that is both a visual, self-running
NPC arena simulation and a reproducible benchmark. It must demonstrate two
DataVo features in an application-shaped workload:

1. NPC behaviour retrieves the nearest stored memories through DataVo vector
   search.
2. A live leaderboard is delivered either through DataVo incremental view
   maintenance (IVM) or by polling and fully recomputing the same SQL query.

The project is a runnable demonstration, not a finished game. It intentionally
uses primitive Unity objects and no external art, input system, network service,
LLM, or native database plugin.

## Project Boundary

Add a new standalone Unity project at
`demos/unity/DataVo.UnityArenaDemo`. Do not add arena logic to
`tests/unity/DataVo.UnitySmoke`; that project remains a compact compatibility
and IL2CPP regression test.

The arena project consumes the same prepared portable DataVo candidate and
Unity adapter packages as the smoke project. It references Unity Mathematics
for Unity-native vector types. The demo supports two launch forms:

- **Visual Editor/player mode** (default): continuously runs the simulation and
  displays the arena and diagnostics.
- **Headless benchmark mode**: accepts deterministic command-line options,
  performs a finite warmup and measurement run, writes one JSON result file,
  then exits with a nonzero code on an invariant failure.

No external database is a V1 baseline. The comparison isolates application
architecture: the same DataVo tables, mutations, SQL, and result consumer run
once with maintained subscriptions and once with polling. A future SQLite
comparison must be a separate, explicitly scoped experiment because it needs a
Unity-compatible provider/plugin and equivalent application mapping.

## Simulated Arena

The scene contains a player marker and a small visible subset of autonomous NPC
markers. Each NPC moves among waypoints and periodically receives a
deterministic gameplay stimulus. The simulation creates a larger logical
population than the visible marker count so the benchmark can scale without
turning the scene into visual noise.

An NPC has:

- an id, team, position, health, score, and current behaviour;
- a deterministic 32-dimensional *gameplay memory* vector;
- stored memories tagged with examples such as `danger`, `ally`, `loot`, and
  `objective`.

These are not text embeddings or an LLM claim. They are deterministic vectors
derived from gameplay traits so the demo uses the real vector-index and
nearest-neighbour path honestly and reproducibly. A production game can later
store locally generated text embeddings in the same `VECTOR(32)`-style schema
or a larger fixed dimension.

The memory table uses a `VECTOR(32)` column and a `FLAT` index in V1. FLAT is
exact, deterministic, and appropriate for a small local demo. HNSW is a
separate scale/recall experiment after functional Unity vector-search coverage
is established.

Every configured behaviour interval, the selected NPC issues a top-k nearest
memory search. Its nearest tag selects a simple deterministic behaviour and
the corresponding marker colour. The HUD shows the selected NPC, its behaviour,
and its nearest memories so vector search is visible rather than hidden.

## Live Leaderboard Workload

The shared data model includes `Players`, `Inventory`, and `NpcMemories`.
Every simulation tick uses a fixed seed to mutate positions, health, score,
zones, and a bounded number of inventory records. The live SQL views are based
on the existing GameArena workload:

- top-20 leaderboard ordered by score;
- alive-player count grouped by team;
- selected-zone player/inventory join.

The visible HUD renders the leaderboard and aggregate. It uses the same
presentation model in both modes.

### Reactive mode

At setup, the demo registers supported standing queries with `Subscribe`.
Each tick applies writes, calls `DispatchPendingNotifications` on the Unity
main thread, and updates the presentation model only from received changes.

### Polling mode

At setup, no live subscriptions are registered. After the identical writes on
each tick, the demo executes the identical live-view SQL and rebuilds the same
presentation model from the returned rows.

Reactive and polling runs are independent contexts seeded with the same data
and mutation schedule. This prevents one mode from contaminating the other.
At the end of a finite benchmark run, their final leaderboard and aggregate
results must be semantically equal. A result mismatch is a correctness failure,
not a performance datapoint.

## Measurement Contract

The measured region excludes database seeding, vector-index creation, scene
object creation, and warmup ticks. Both modes include the writes, the
leaderboard delivery/recompute work, the shared NPC vector-search schedule, and
the same presentation-model update.

Each benchmark result records:

- configuration: seed, logical/visible NPC counts, memories per NPC, warmup,
  measured ticks, mutations per tick, vector dimension, and mode;
- tick, mutation, leaderboard-maintenance, and vector-search latency
  (P50/P95/P99/max);
- 60 Hz and 120 Hz frame-budget miss rates;
- managed allocation/GC counters when the Unity runtime exposes them;
- reactive changes emitted or polling rows materialized;
- final leaderboard checksum and aggregate checksum for equivalence;
- Unity version, platform, scripting backend, and result schema version.

The output must state that it compares **incremental maintained views with
full-query polling**, not “DataVo versus SQLite”. The benchmark must never
assert that reactive mode is faster: performance depends on machine, dataset,
and query shape. It only asserts deterministic correctness and valid metric
output.

## Visual Presentation

The scene is self-running; keyboard input is intentionally unnecessary.
Primitive markers make the simulation inspectable without assets:

- player marker and moving NPC markers;
- behaviour colours for patrol, pursue, flee, and assist;
- compact HUD for current mode, tick/maintenance latency, GC/allocation data,
  live leaderboard, work count, and selected NPC memory-search details;
- optional automatic alternation between modes in visual mode, with a clear
  mode label and reset between runs.

The default visual configuration favours legibility. Headless options can use a
larger logical population and longer run for reproducible performance results.

## Error Handling

Startup fails with an actionable error if the prepared DataVo package is absent,
the schema/index cannot be created, a vector result cannot be materialized, or
a required subscription is unsupported. The player writes exactly one bounded
JSON result, including a failure status and message when possible, before it
exits. Benchmark mode has a timeout in the runner.

The demo validates vector dimensions before search and treats missing nearest
results as an error for the seeded dataset. It does not silently replace a
vector failure with a random NPC behaviour.

## Verification

1. .NET/Unity-independent deterministic scenario tests cover the mutation
   schedule, known nearest-memory result, result checksum, and reactive/polling
   semantic equivalence.
2. Unity EditMode tests verify package import, schema/index setup, typed
   Unity mapping, and both delivery modes on a short seeded run.
3. A Unity Editor batch runner prepares the portable candidate, runs a short
   headless benchmark in each mode, and validates its JSON schema and equality
   checksums.
4. The existing macOS Unity 6000.5.4f1 IL2CPP player runner gains a separate
   arena invocation. It builds the actual player, runs the finite headless
   reactive and polling cases, checks their result files, and preserves the
   tracked smoke-project settings.

Windows IL2CPP validation is intentionally subsequent platform work. Burst and
the C# Job System are not part of this design; DataVo access remains on the
Unity main thread because the benchmark measures database delivery work rather
than a Burst-compatible data-oriented pipeline.

## Delivery Slices

1. Create the independent Unity project, candidate preparation, minimal scene,
   deterministic models, and visual self-running NPC movement.
2. Add DataVo schema, exact vector-memory search, visible nearest-memory
   behaviour, and functional tests.
3. Add shared live-view presentation plus reactive and polling execution modes;
   prove final-state equivalence.
4. Add latency/work/GC reporting, JSON artifacts, Unity batch execution, and
   macOS IL2CPP player proof. Document methodology and the narrow claim.
