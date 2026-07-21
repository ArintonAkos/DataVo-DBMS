# Unity Portable Dependency Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task, with the
> tests in each task run before its implementation steps.

**Goal:** Generate a portable DataVo Unity candidate whose managed dependency
closure has exactly one provider for each framework assembly, then prove the
existing Medium-stripping macOS IL2CPP Player smoke against that closure.

**Architecture:** The experimental `netstandard2.1` Core target compiles
against Unity's JSON-8-compatible API surface. A tracked inventory pins the
four framework DLLs Unity 6000.5.4f1 provides. Candidate preparation obtains
NuGet assembly identities through the existing managed-metadata inspector and
records one resolution per DLL: copied, Unity-provided, or a deterministic
conflict. The candidate itself contains copied DLLs only; its manifests retain
the Unity-provider provenance.

**Tech stack:** .NET 10 SDK for build tools, `netstandard2.1`, NuGet,
Node.js, Bash, Unity 6000.5.4f1 (`d550df8bd089`), Unity BCL extensions,
macOS standalone IL2CPP with Medium managed stripping.

## Global constraints

- Keep `DataVo.Core` Unity-free. Do not add a Unity reference, Unity-specific
  source branch, `link.xml`, stripping exception, or change to the normal
  `net10.0` implementation.
- The only Core package version changed by this slice is the portable
  `System.Text.Json` reference: `10.0.0` to `8.0.0`. `net10.0` remains
  framework-owned.
- Treat same filename as insufficient. A Unity-provided assembly is usable
  only when its full assembly identity exactly matches the resolved NuGet
  asset. A same-simple-name mismatch is a candidate-preparation error, never a
  copy-and-hope fallback.
- The tracked Unity BCL inventory is a pin for 6000.5.4f1 revision
  `d550df8bd089`, not a claim about every Unity version. The runtime checker
  must run before candidate preparation, EditMode execution, and Player
  execution.
- Continue to use `ScriptingImplementation.IL2CPP` and
  `ManagedStrippingLevel.Medium`. Do not lower stripping to make the smoke
  pass.
- Keep the existing Player root correction based on
  `EditorApplication.applicationContentsPath`; commit it as part of the final
  Player-gate task. Do not lose it.
- Preserve unrelated worktree changes and untracked files. Stage exact paths;
  force-stage only the plan/spec documents under `docs/superpowers`.
- Do not claim general Unity, Windows, Burst, Jobs, persistence, benchmark, or
  production support. Only record macOS IL2CPP evidence after the actual Player
  process writes a passing report and exits zero.

## Resolution contract

| Input for a NuGet runtime DLL | Result in candidate | Required manifest fields |
| --- | --- | --- |
| No tracked Unity provider with the same simple name | Copy to `Runtime/Dependencies` | `resolution: "copied"`, `fixturePath`, NuGet provenance, identity |
| Same simple name and exact full identity | Do not copy; Unity is authoritative | `resolution: "unity-provided"`, `unityRuntimePath`, Unity inventory reference, NuGet provenance, identity |
| Same simple name but different full identity | Fail preparation before package import | Error names NuGet identity/provenance and Unity identity/runtime path |

The initial tracked providers use the Unity-relative path
`Resources/Scripting/BCLExtensions/runtime/netstandard2.1/<assembly>.dll`:

| DLL | Exact identity | SHA-256 |
| --- | --- | --- |
| `System.Text.Json.dll` | `System.Text.Json, Version=8.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51` | `f80c939be592939ca3bbbc9ddf61bb137ce201ec38d70005915cb70acf2d4cc3` |
| `System.Text.Encodings.Web.dll` | `System.Text.Encodings.Web, Version=8.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51` | `5ad1f348223f281c34b9b857445a1c3412a430026ef049aeac641c53a92408be` |
| `Microsoft.Bcl.AsyncInterfaces.dll` | `Microsoft.Bcl.AsyncInterfaces, Version=8.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51` | `136d5965cf4768e8420b547a8bddea882921f426c371833d558a858a2f0c235a` |
| `System.Runtime.CompilerServices.Unsafe.dll` | `System.Runtime.CompilerServices.Unsafe, Version=6.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a` | `fd67abc4c4af10affbcd42fd68a2a7bc361b26714afe6965d4d0d139459d8297` |

`System.Threading.Channels`, `System.IO.Pipelines`, and
`System.ComponentModel.Annotations` are not initial providers because the
pinned BCL-extension directory does not contain them. Their resolver outcome
is determined solely from the inventory, so they are copied unless a future
inventory intentionally adds an exact provider.

## File structure

| File | Responsibility |
| --- | --- |
| `DataVo.Core/DataVo.Core.csproj` | Portable JSON-8 package boundary; `net10.0` unchanged. |
| `tests/unity/DataVo.UnitySmoke/UnityRuntimeBclManifest.json` | Versioned, hashed Unity BCL provider contract. |
| `scripts/check-unity-runtime-bcl.sh` | Local Unity installation verification against the contract. |
| `scripts/test-check-unity-runtime-bcl.sh` | Positive and tampered-inventory checker tests. |
| `tools/DataVo.UnityCandidateInspector/Program.cs` | Existing fixture inventory plus a compact identity-only JSON mode. |
| `scripts/resolve-unity-candidate-assets.mjs` | Exact identity resolution and dependency provenance generation. |
| `scripts/prepare-unity-candidate.sh` | Builds the inspector before resolution, verifies Unity BCL, and links provider provenance into the candidate. |
| `scripts/test-unity-candidate-inspector.sh` | Tests the new identity-only inspector mode. |
| `scripts/test-prepare-unity-candidate.sh` | Asserts copied/Unity-provided output behavior and no duplicate framework DLLs. |
| `scripts/prepare-unity-smoke-candidate.sh`, `scripts/run-unity-editor-tests.sh`, `scripts/run-unity-macos-il2cpp-player-smoke.sh` | Gate Unity actions on the same BCL inventory check. |
| `tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Editor/PlayerSmokeBuild.cs` | Retains corrected Editor-root discovery and restores Player settings. |

## Task 1: Pin the portable JSON boundary and prove it still builds

**Files:**
- Modify: `DataVo.Core/DataVo.Core.csproj`
- Modify: `scripts/test-prepare-unity-candidate.sh`

**Step 1: Add a failing portable-package assertion.**

Extend `scripts/test-prepare-unity-candidate.sh` before changing the project
file. After it produces the fixture, assert that the resolved dependency record
for `System.Text.Json` has package version `8.0.0`. Keep the test general: it
must search the dependency records by `packageId`, not assume the DLL is copied
because later tasks will make it Unity-provided.

Run:

```bash
bash scripts/test-prepare-unity-candidate.sh
```

Expected before the Core change: failure stating that the portable candidate
resolved `System.Text.Json` `10.0.0` instead of `8.0.0`.

**Step 2: Change only the portable package reference.**

In the existing `netstandard2.1` `ItemGroup` in
`DataVo.Core/DataVo.Core.csproj`, change:

```xml
<PackageReference Include="System.Text.Json" Version="10.0.0" />
```

to:

```xml
<PackageReference Include="System.Text.Json" Version="8.0.0" />
```

Do not move it outside the target framework condition. Do not alter the
`System.Threading.Channels` or `System.ComponentModel.Annotations` references
in this task.

**Step 3: Compile both boundaries and run relevant persistence coverage.**

Run:

```bash
dotnet build DataVo.Core/DataVo.Core.csproj -c Release
dotnet build DataVo.Core/DataVo.Core.csproj -c Release -p:DataVoEnablePortableTarget=true
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "FullyQualifiedName~BTreePersistenceTests|FullyQualifiedName~HNSWIndexPersistence|FullyQualifiedName~FlatVectorIndex"
bash scripts/test-prepare-unity-candidate.sh
```

Expected: both targets compile; JSON-backed persistence coverage passes; the
candidate test sees JSON `8.0.0`. If source-generated JSON code requires an API
newer than JSON 8, stop at the compile error and fix the compatibility issue in
Core normally—do not add target-specific wrappers merely to suppress it.

**Step 4: Commit the boundary change.**

```bash
git add DataVo.Core/DataVo.Core.csproj scripts/test-prepare-unity-candidate.sh
git commit -m "fix: align portable JSON dependency with Unity"
```

## Task 2: Add the immutable Unity BCL runtime contract

**Files:**
- Create: `tests/unity/DataVo.UnitySmoke/UnityRuntimeBclManifest.json`
- Create: `scripts/check-unity-runtime-bcl.sh`
- Create: `scripts/test-check-unity-runtime-bcl.sh`
- Modify: `scripts/check-unity-smoke-project.sh`

**Step 1: Write checker tests before its implementation.**

Create `scripts/test-check-unity-runtime-bcl.sh`. It must:

1. run the future checker with the real default manifest and installed Unity
   root, expecting success;
2. copy the manifest into a temporary path, replace one SHA-256 character, run
   the checker with `--manifest <tampered-file>`, and require failure that
   names both the assembly and hash mismatch; and
3. replace one identity string, require failure that names both the assembly
   and identity mismatch.

The test must use only temporary manifest copies and must not modify the
installed Editor. It should skip neither negative case merely because the
positive local Unity installation is missing; a missing installation is a
clear failure that says `6000.5.4f1` is required.

Run:

```bash
bash scripts/test-check-unity-runtime-bcl.sh
```

Expected before implementation: missing checker/manifest failure.

**Step 2: Add the tracked manifest.**

Create `UnityRuntimeBclManifest.json` with schema version `1`, editor version
`6000.5.4f1`, revision `d550df8bd089`, and the four entries in the resolution
contract table. Each entry has:

```json
{
  "fileName": "System.Text.Json.dll",
  "runtimePath": "Resources/Scripting/BCLExtensions/runtime/netstandard2.1/System.Text.Json.dll",
  "identity": "System.Text.Json, Version=8.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
  "sha256": "f80c939be592939ca3bbbc9ddf61bb137ce201ec38d70005915cb70acf2d4cc3"
}
```

Use `assemblies` sorted by `fileName`. Do not put an absolute workstation path
in the tracked JSON.

**Step 3: Implement the checker with metadata, not filename trust.**

Implement `scripts/check-unity-runtime-bcl.sh` with these arguments:

```text
--unity-root <Unity.app/Contents>    optional; default is the pinned Hub path
--manifest <path>                    optional; default is the tracked manifest
```

Validate argument count, manifest schema, pin fields, unique `fileName` and
unique `runtimePath`, relative paths without `..`, lowercase 64-character hash
format, and nonempty identity. For every record:

- require the declared file under `--unity-root`;
- calculate `shasum -a 256` and compare exactly; and
- invoke the managed inspector identity mode introduced in Task 3, or a small
  local `dotnet` metadata probe if Task 3 is temporarily not yet available, to
  compare full identity exactly.

The script must emit a stable machine-readable success line:

```text
Verified Unity runtime BCL inventory: <manifest path>
```

and errors must name the entry, expected value, observed value, and actual
runtime file path. Add a source-contract check in
`scripts/check-unity-smoke-project.sh` that requires the manifest and checker
to exist, but do not make this static project check attempt to inspect a local
Unity installation.

**Step 4: Run the checker tests.**

```bash
bash scripts/test-check-unity-runtime-bcl.sh
bash scripts/check-unity-smoke-project.sh
```

Expected: positive inventory validation succeeds; both tampered copies fail
for their declared reason; static smoke-project validation remains portable.

**Step 5: Commit the runtime contract.**

```bash
git add tests/unity/DataVo.UnitySmoke/UnityRuntimeBclManifest.json \
  scripts/check-unity-runtime-bcl.sh \
  scripts/test-check-unity-runtime-bcl.sh \
  scripts/check-unity-smoke-project.sh
git commit -m "test: pin Unity runtime BCL inventory"
```

## Task 3: Make assembly identity available to the resolver

**Files:**
- Modify: `tools/DataVo.UnityCandidateInspector/Program.cs`
- Modify: `scripts/test-unity-candidate-inspector.sh`

**Step 1: Extend the inspector contract test first.**

Add a second invocation to `scripts/test-unity-candidate-inspector.sh` that
passes two absolute DLL paths to the new identity mode and writes an inventory
outside the fixture root. Assert:

- schema version is `1`;
- records are deterministically sorted by full source path;
- each record has `sourcePath`, `fileName`, lowercase `sha256`, and full
  `identity`;
- no `publicSystemTypes` field is present; and
- a missing DLL exits `64` and names that path.

Run:

```bash
bash scripts/test-unity-candidate-inspector.sh
```

Expected before implementation: unknown identity-mode argument failure.

**Step 2: Add a narrow identity-only mode.**

Preserve the existing required `--root --output <assemblies...>` candidate
inventory mode and all its duplicate public-System-type protections. Add a
second mutually exclusive invocation:

```text
dotnet DataVo.UnityCandidateInspector.dll \
  --identity-output <path> <absolute-assembly-path> [more paths...]
```

It may inspect paths outside a candidate root. It must use the same
`AssemblyName.GetAssemblyName` and SHA-256 implementation as the full mode,
but it must not scan public types or apply duplicate-identity rejection: a
resolver needs to inspect every candidate input before deciding whether a
same-simple-name case is a conflict. Output:

```json
{
  "schemaVersion": 1,
  "assemblies": [
    {
      "sourcePath": "/absolute/path/System.Text.Json.dll",
      "fileName": "System.Text.Json.dll",
      "sha256": "...",
      "identity": "System.Text.Json, Version=8.0.0.0, Culture=neutral, PublicKeyToken=..."
    }
  ]
}
```

Parse modes explicitly so a caller cannot supply `--root` together with
`--identity-output`. Return `64` for invalid command-line usage and retain
the current `1` return for metadata/read failures.

**Step 3: Run the existing and extended contract.**

```bash
dotnet build tools/DataVo.UnityCandidateInspector/DataVo.UnityCandidateInspector.csproj --no-restore
bash scripts/test-unity-candidate-inspector.sh
```

Expected: the legacy fixture inventory behavior remains unchanged and the new
identity-only JSON is usable outside a fixture.

**Step 4: Commit the inspector extension.**

```bash
git add tools/DataVo.UnityCandidateInspector/Program.cs \
  scripts/test-unity-candidate-inspector.sh
git commit -m "feat: inspect Unity candidate dependency identities"
```

## Task 4: Resolve candidate dependencies against the Unity provider contract

**Files:**
- Modify: `scripts/resolve-unity-candidate-assets.mjs`
- Modify: `scripts/prepare-unity-candidate.sh`
- Modify: `scripts/test-prepare-unity-candidate.sh`
- Create: `scripts/test-resolve-unity-candidate-assets.mjs`

**Step 1: Add synthetic resolution tests before changing the resolver.**

Create `scripts/test-resolve-unity-candidate-assets.mjs`. It must call the
resolver module through an exported `resolveCandidateAssets(options)` function,
not spawn a nested shell. Build three temporary fixture inventories whose
asset files can be empty because the test injects inspector JSON directly:

1. an assembly absent from the Unity inventory resolves `copied` and gets a
   fixture path;
2. `System.Text.Json.dll` with the exact tracked 8.0.0.0 identity resolves
   `unity-provided` and has no fixture path or copied destination; and
3. `System.Text.Json.dll` with identity 10.0.0.0 throws an error that contains
   the NuGet package/version, candidate identity, Unity identity, and Unity
   runtime path.

Also assert that two NuGet assets with the same filename still fail before any
copy attempt, and that an untracked DLL is copied only once. The test should
write no real Unity path and must clean its temporary tree.

Run:

```bash
node scripts/test-resolve-unity-candidate-assets.mjs
```

Expected before implementation: import/export or contract failure because the
resolver currently copies every dependency and has no provider inventory.

**Step 2: Refactor resolver input and exact comparison.**

Keep the CLI entry point but add required arguments:

```text
--inspector <DataVo.UnityCandidateInspector.dll>
--unity-runtime-inventory <UnityRuntimeBclManifest.json>
```

After extracting and sorting NuGet dependency assets from `project.assets.json`:

1. reject duplicate destination filenames exactly as today;
2. invoke `dotnet <inspector> --identity-output <temporary-output>` once with
   all resolved source paths;
3. correlate identity records to source paths exactly once and reject a missing
   or extra record;
4. map the simple assembly name from `fileName` to the inventory record;
5. apply the three-outcome table; and
6. copy only `copied` records after all resolution decisions have succeeded.

Use exact full identity strings, not version prefix parsing. The resolver may
create `Runtime/Dependencies` even when no dependencies are copied, but it
must leave it empty for Unity-provided entries. Export the pure resolver
function for the synthetic test; call it from the CLI only when the module is
the program entry point.

Write `dependency-assets.json` schema version `2`. Every record includes
`packageId`, `packageVersion`, normalized `assetPath`, `identity`, and
`resolution`. Copied records additionally include `fixturePath`; Unity-provided
records additionally include `unityRuntimePath` and
`unityRuntimeInventory`. Never emit `fixturePath: null`.

**Step 3: Prepare the inspector and validate Unity before resolution.**

In `scripts/prepare-unity-candidate.sh`:

1. restore/build the inspector before calling the resolver;
2. run `scripts/check-unity-runtime-bcl.sh` with the tracked manifest and
   default pinned Unity root before candidate resolution;
3. pass the built inspector and manifest to the resolver; and
4. continue to build the full candidate assembly inventory only from
   `Runtime/DataVo.Core.dll` plus copied DLLs.

Update the inline candidate-manifest writer to schema version `2` and add a
`unityRuntimeInventory` object containing the tracked relative manifest path,
pin version, revision, and manifest SHA-256. When mapping fixture assemblies
to NuGet provenance, require `resolution === "copied"`; Unity-provided records
remain provenance in `dependency-assets.json`, not fictional fixture assembly
records.

The portable .NET consumer build remains before resolver execution and retains
its complete NuGet closure. It is deliberately not changed to rely on Unity
BCL DLLs.

**Step 4: Expand end-to-end candidate assertions.**

Update `scripts/test-prepare-unity-candidate.sh` to require:

- dependency schema version `2` and candidate schema version `2`;
- all eight common provenance fields on every record;
- copied records have a relative existing `fixturePath` and appear exactly once
  in `assembly-inventory.json`/candidate assemblies;
- Unity-provided records have `unityRuntimePath` and no fixture path and do
  not appear in the candidate's copied assembly inventory;
- the four provider filenames do not exist in `Runtime/Dependencies`;
- the JSON record is `System.Text.Json` `8.0.0` with
  `resolution: "unity-provided"`; and
- the candidate manifest references the checked Unity inventory.

Run:

```bash
node scripts/test-resolve-unity-candidate-assets.mjs
bash scripts/test-prepare-unity-candidate.sh
```

Expected: synthetic copied/provider/conflict behavior passes, and the real
candidate contains no copy of Unity's four framework provider DLLs. Any later
NuGet resolution that mismatches a pinned Unity simple name fails before Unity
package import with an actionable error.

**Step 5: Commit closure resolution.**

```bash
git add scripts/resolve-unity-candidate-assets.mjs \
  scripts/prepare-unity-candidate.sh \
  scripts/test-prepare-unity-candidate.sh \
  scripts/test-resolve-unity-candidate-assets.mjs
git commit -m "fix: resolve Unity candidate dependency providers"
```

## Task 5: Gate Unity execution and obtain real IL2CPP evidence

**Files:**
- Modify: `scripts/prepare-unity-smoke-candidate.sh`
- Modify: `scripts/run-unity-editor-tests.sh`
- Modify: `scripts/run-unity-macos-il2cpp-player-smoke.sh`
- Modify: `scripts/test-unity-macos-il2cpp-player-smoke.sh`
- Modify: `tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Editor/PlayerSmokeBuild.cs`
- Modify: `scripts/check-unity-support-status.sh` and relevant Unity docs only
  after a real Player pass

**Step 1: Extend static runner contracts first.**

Before changing runners, add source-contract assertions that all Unity action
paths invoke `check-unity-runtime-bcl.sh` before candidate preparation or Unity
batch mode. Retain the existing assertion for
`EditorApplication.applicationContentsPath`. Add an assertion that the Player
runner records the pre-run SHA-256 of
`ProjectSettings/ProjectSettings.asset` and fails if it is not restored when
the runner returns.

Run:

```bash
bash scripts/test-unity-macos-il2cpp-player-smoke.sh
```

Expected before implementation: it fails for the missing BCL gate and
ProjectSettings restoration contract.

**Step 2: Integrate the common BCL gate and preserve source control state.**

Make `prepare-unity-smoke-candidate.sh` call the checker before it invokes the
candidate preparation script. Make the Editor and Player runners call the
checker before any package/candidate preparation, so direct and indirect entry
points are both protected.

In the Player runner, hash `ProjectSettings.asset` before Unity starts and
compare the hash after the build/launch attempt. If the file differs, fail with
the before/after hashes and a clear source-control-state error. Do not add or
commit a Unity-generated `SceneTemplateSettings.json`; it is not part of the
smoke feature. If the first valid Unity import normalizes the tracked project
settings, inspect that diff explicitly and either make one intentional baseline
commit or update the build restoration code until repeated runner execution is
clean—never silently accept generated persistence of the temporary IL2CPP or
stripping settings.

Keep `PlayerSmokeBuild.GetEditorRoot()` based on
`EditorApplication.applicationContentsPath`, then use its `Contents` parent
chain to find the Unity app root. This fixes the already-observed false
`mac-il2cpp` module-missing result.

**Step 3: Run fast gates and actual Unity evidence.**

Run in this order:

```bash
bash scripts/test-check-unity-runtime-bcl.sh
bash scripts/check-unity-smoke-project.sh
bash scripts/test-unity-candidate-inspector.sh
node scripts/test-resolve-unity-candidate-assets.mjs
bash scripts/test-prepare-unity-candidate.sh
bash scripts/test-unity-mathematics-package.sh --all
bash scripts/test-unity-macos-il2cpp-player-smoke.sh
bash scripts/run-unity-editor-tests.sh
bash scripts/run-unity-macos-il2cpp-player-smoke.sh
```

Expected final Player evidence: Unity builds macOS IL2CPP at Medium stripping,
the app launches, writes schema-version-one `result.json` with `status` passed
and `scriptingBackend` `IL2CPP`, then exits zero. Inspect the candidate to
confirm no duplicate framework providers before interpreting a runtime failure.

If a new Linker/AOT failure occurs after this contract passes, retain its log
and stop: it is a separate real compatibility issue, not permission to weaken
the contract.

**Step 4: Update status documentation only with evidence.**

After and only after Step 3's actual Player success, update
`scripts/check-unity-support-status.sh` and the narrow Unity verification docs
to say that the exact Unity 6000.5.4f1 macOS IL2CPP Medium-stripping smoke
passed. Keep all other support categories explicitly unverified.

**Step 5: Commit gates, corrected root discovery, and evidence.**

First commit behavior and tests, including the existing uncommitted root fix:

```bash
git add scripts/prepare-unity-smoke-candidate.sh \
  scripts/run-unity-editor-tests.sh \
  scripts/run-unity-macos-il2cpp-player-smoke.sh \
  scripts/test-unity-macos-il2cpp-player-smoke.sh \
  tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Editor/PlayerSmokeBuild.cs
git commit -m "test: gate Unity smoke on runtime dependencies"
```

Then, only if actual Player evidence passed, commit the status wording:

```bash
git add scripts/check-unity-support-status.sh <exact-evidence-doc-paths>
git commit -m "docs: record Unity IL2CPP player smoke evidence"
```

## Final verification and handoff

Run:

```bash
git diff --check HEAD
dotnet build DataVo.Core/DataVo.Core.csproj -c Release
dotnet build DataVo.Core/DataVo.Core.csproj -c Release -p:DataVoEnablePortableTarget=true
bash scripts/test-check-unity-runtime-bcl.sh
bash scripts/test-unity-candidate-inspector.sh
node scripts/test-resolve-unity-candidate-assets.mjs
bash scripts/test-prepare-unity-candidate.sh
bash scripts/test-unity-mathematics-package.sh --all
bash scripts/test-unity-macos-il2cpp-player-smoke.sh
bash scripts/run-unity-editor-tests.sh
bash scripts/run-unity-macos-il2cpp-player-smoke.sh
git status --short
```

Report the exact passing commands, Player result path, `result.json` fields,
and the narrow scope of the proof. If pre-existing full-suite `Catalog.xml`
test interference remains, report it separately rather than describing the
whole solution as green.
