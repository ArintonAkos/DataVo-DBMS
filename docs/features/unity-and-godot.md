# Unity and Godot Evaluation Status

DataVo is not currently a supported Unity or Godot runtime integration. This page records the proposed use cases, the managed integration boundary, and the proof required before support is claimed.

The public `DataVo.Core` package remains `net10.0`-only. `DataVoEnablePortableTarget=true` creates an experimental local `netstandard2.1` candidate; it is not a distributed or supported Unity package. Passing its .NET-host verifier alone does not prove Unity Editor, Mono, IL2CPP, Jobs, or Burst compatibility.

## Support matrix

| Environment or boundary | Status | Meaning |
| --- | --- | --- |
| Unity 6000.5.4f1 Editor | Verified (narrow smoke) | The generated candidate imports into the tracked project and its EditMode smoke suite passes. |
| macOS IL2CPP (6000.5.4f1) | Verified (narrow smoke) | A Medium-stripping Player builds, launches, executes automatic Unity value binding, writes a passing JSON report, and exits zero. |
| Windows x64 IL2CPP | Unverified | Windows must build, launch, run the smoke suite, and exit successfully on its own target. |
| Direct Burst calls | Unsupported by design | DataVo contains managed classes, strings, collections, locks, exceptions, and storage services. |
| Job-to-managed batch bridge | Planned proof | Jobs produce fixed-layout POD commands; managed code drains them after `JobHandle` completion. |
| In-memory mode | Candidate scope | This is the only storage mode included in the Stage 3 Unity candidate proof. |
| Disk/LSM persistence | Unsupported until separately validated | The experimental portable target rejects both modes; do not use DataVo for shipped game saves. |
| Godot C# | Unverified | Godot is not part of the first Unity proof and requires its own final-artifact validation. |

## Candidate use cases

- deterministic gameplay and simulation test state
- inventory, scoreboard, or economy views maintained after writes
- editor-side semantic asset lookup
- local NPC-memory and vector-search prototypes
- debugging and playtest tooling

These are evaluation targets, not current support claims.

## Planned managed boundary

DataVo runs in managed code outside Burst jobs. A Burst-compatible job may write blittable commands or numeric rows into a Unity native container. After the job completes, managed integration code drains the batch, maps it to schema columns, and calls DataVo. No DataVo type belongs in the job struct, and no DataVo method is called by Burst-compiled code.

## Verified narrow in-memory scenario

The verified candidate uses `StorageMode.InMemory` only. It registers both UnityEngine and Unity.Mathematics types through `UseUnityTypes()`, performs typed model insert/map/read for all initial supported types, rejects an exact vector-length mismatch, and emits a Player JSON result. Similar APIs and the portable-asset verifier may pass under a .NET host, but that does not establish wider Unity compatibility.

## NPC Memory Arena evaluation demo

`demos/unity/DataVo.UnityArenaDemo` is a Unity 6000.5.4f1/macOS IL2CPP evaluation demo, not a supported game-runtime package. It is a self-running primitive NPC arena: each selected NPC searches its exact local vector memories, while its deterministic scoreboard, team aggregate, and zone inventory are delivered either through DataVo incremental maintenance or through full-query polling.

The reproducible Player gate builds one Medium-stripping macOS IL2CPP app, launches it once per delivery mode, records JSON artifacts, and requires both modes to finish with equal leaderboard and aggregate checksums. This establishes that this particular in-memory vector-search and reactive-query workload runs on the pinned macOS target. It does not establish support for arbitrary projects, platforms, storage modes, or save data.

## Proof gate

The exact Unity 6000.5.4f1 macOS Editor and macOS IL2CPP Medium-stripping gates have passed the narrow type-binding smoke and the separate NPC Memory Arena evaluation workload. Support remains unverified for Windows, other Unity versions and targets, Mono, Burst, Jobs, broader vector-search or reactive-query workloads, and persistent storage. Disk and LSM modes require a separate durability proof.

## Related pages

- [v0.1 Alpha Scope](../manual/preface/alpha-scope.md)
- [Setup and Packaging](./setup-and-packaging.md)
- [Runtime Observability](./runtime-observability.md)
- [Reactive Queries](./reactive-queries.md)
- [Vector Queries Guide](./vector-queries-guide.md)
