# Unity macOS IL2CPP Player Smoke Design

## Status

Approved in conversation on 2026-07-21; this document records the agreed
design before implementation. It follows the executed Unity `6000.5.4f1`
Editor smoke test and provides a narrow macOS Player/AOT proof. It does not
make a Windows, Burst, Jobs, performance, or production-save-data claim.

## Problem

The existing Unity evidence proves that the generated portable DataVo Core
candidate and Unity type-binding package import, compile, and execute in the
Editor. That is necessary but not sufficient for IL2CPP. In particular, the
model mapper's reflection use and the value-converter registration must be
observed in a built player with managed stripping enabled.

## Decision

Add a small, non-interactive macOS IL2CPP player harness to the existing
tracked `tests/unity/DataVo.UnitySmoke` project. The harness runs the same
important automatic Unity type-binding operations in a built player, writes a
machine-readable result, and terminates with a process success/failure code.

The Player is a test artifact, not a reusable runtime package or demo. It uses
only the current in-memory DataVo scenario; persistent save-data support is out
of scope.

## Alternatives considered

### Build-only check

Building a macOS IL2CPP application is quick, but it cannot reveal a
reflection, stripping, initialization, or runtime conversion failure. Rejected.

### Unity PlayMode test runner

PlayMode tests are useful, but they leave Player startup, clean termination,
and external machine-readable result handling indirect. Rejected for this
proof; existing Editor tests remain in place.

### Dedicated Player bootstrap (chosen)

A runtime bootstrap executes the smoke scenario after the Player starts. It
owns its JSON report and exit code, so a shell runner can assert the complete
build-launch-execute-report lifecycle without human interaction. Chosen.

## Architecture

### Runtime bootstrap

A source-controlled runtime assembly contains an internal Player smoke runner.
It is activated with Unity's runtime-initialization mechanism, creates a
temporary in-memory `DataVoContext`, calls `UseUnityTypes()`, and executes the
following checks:

1. create a table with `VECTOR(2)`, `VECTOR(3)`, and `VECTOR(4)` columns;
2. write a public model containing the supported UnityEngine and Unity
   Mathematics vector/quaternion types through `Insert<T>`;
3. select the row and validate `Map<T>` and representative ad-hoc `Get<T>`
   reads, including quaternion component order;
4. attempt a deliberately incompatible `Get<Vector3>` from `VECTOR(2)` and
   verify that its error names both dimensions.

The runner catches every exception, writes exactly one UTF-8 JSON result below
`Application.persistentDataPath`, and then requests Player termination with
exit code zero on success or non-zero on failure. The report contains a schema
version, pass/fail status, Unity version, scripting backend/build target where
available, and a bounded failure message. It contains no database data,
absolute workspace paths, or stack trace requirement.

The bootstrap is not a `MonoBehaviour` attached to a hand-authored scene. The
build script creates an ignored blank scene for the build, while Unity's
runtime-initialization attribute starts the bootstrap. This avoids committing
fragile manually-written Unity YAML merely to host a test object.

### Editor build entry point

An Editor-only build class exposes a deterministic `-executeMethod` entry
point. It:

- creates/reuses an ignored blank smoke scene;
- sets the target to macOS standalone and scripting backend to IL2CPP;
- sets managed stripping to Medium; and
- writes the Player into an ignored temporary output directory.

It fails early with an actionable message if the locally installed Unity Editor
does not include the macOS IL2CPP build-support module. It must restore modified
Player settings after building so an ad-hoc validation does not silently change
tracked project configuration.

### Host runner

`scripts/run-unity-macos-il2cpp-player-smoke.sh` first prepares the exact local
portable candidate and verifies the existing smoke-project contract. It invokes
the pinned Unity Editor in batch mode using the build entry point, launches the
generated macOS executable directly, waits for the JSON report with a finite
timeout, validates the report schema and success status, and reports the Player
log on failure.

The script rejects a report left over from a prior run by using a fresh ignored
output/report location for each invocation. A successful build alone is never a
pass. A zero process exit without a valid successful report is also never a
pass.

## Error handling

- Missing macOS IL2CPP module: explicit unsupported-environment failure, not a
  false pass or fallback to Mono.
- Unity build failure, Player launch failure, timeout, missing/malformed report,
  non-zero Player exit, or failed scenario: fail the host script.
- Scenario exception: serialize a concise diagnostic and exit non-zero after
  closing the report.
- The Player must not disable stripping or add `link.xml` solely to mask an AOT
  failure. Any real stripping/reflection failure blocks the proof and triggers
  a smaller follow-up design.

## Testing and evidence

1. A static shell-contract test covers required build arguments, the report
   validation contract, timeout behavior, and the absence of a Mono fallback.
2. Existing Core and Unity Editor tests remain required regression checks.
3. The implementation gate runs the real pinned Unity `6000.5.4f1` macOS
   IL2CPP build and then executes its generated Player.
4. Evidence records the Unity revision, target backend, stripping level, result
   report, and exact command outcome in the implementation plan.

## Non-goals

- Windows x64 IL2CPP proof; this remains a later, separate execution gate.
- Burst/Jobs/Collections integration.
- Performance comparison, gameplay benchmark, or allocation claim from this
  one Player smoke scenario.
- Persistent database backends, Unity save-data guidance, or public package
  publication.
- Additional Unity value types, tensors, or changes to `DataVo.Core` APIs.

## Success criteria

- A fresh generated candidate is consumed by the existing smoke project.
- The pinned Unity Editor creates a macOS IL2CPP Player with Medium stripping.
- The generated Player launches, executes the automatic Unity conversion/model
  scenario, writes a valid successful JSON report, and exits successfully.
- Failures are deterministic and diagnostic; no mode falls back to Editor or
  Mono execution.
- Documentation describes only this narrow macOS IL2CPP Player result.
