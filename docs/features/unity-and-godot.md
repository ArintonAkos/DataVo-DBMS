# Unity and Godot Evaluation Status

DataVo is not currently a supported Unity or Godot runtime integration. This page records the proposed use cases, the managed integration boundary, and the proof required before support is claimed.

## Support matrix

| Environment or boundary | Status | Meaning |
| --- | --- | --- |
| Unity Editor | Unverified until Stage 3 | The final candidate package has not been imported and executed in a tracked Unity project. |
| Windows x64 IL2CPP | Unverified until Stage 3 | The player must build, launch, run the smoke suite, and exit successfully. |
| Direct Burst calls | Unsupported by design | DataVo contains managed classes, strings, collections, locks, exceptions, and storage services. |
| Job-to-managed batch bridge | Planned proof | Jobs produce fixed-layout POD commands; managed code drains them after `JobHandle` completion. |
| In-memory mode | Candidate scope | The first proof covers in-memory SQL, snapshots, reactive queries, and vector search. |
| Disk/LSM persistence | Unsupported until separately validated | Do not use these modes for shipped game saves until durability is proven per platform. |
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

## Candidate in-memory scenario

The first proof will exercise `StorageMode.InMemory`, snapshots, reactive subscriptions, Flat vector search, and HNSW recall through the final packaged artifact. Similar APIs already run under modern .NET, but that does not establish Unity compatibility.

## Proof gate

Support remains unverified until a pinned Unity 6.5 project imports the exact candidate package, passes Editor execution, and builds and executes a Windows x64 IL2CPP player with Medium managed stripping. Disk and LSM modes require a later durability proof.

## Related pages

- [v0.1 Alpha Scope](../manual/preface/alpha-scope.md)
- [Setup and Packaging](./setup-and-packaging.md)
- [Runtime Observability](./runtime-observability.md)
- [Reactive Queries](./reactive-queries.md)
- [Vector Queries Guide](./vector-queries-guide.md)
