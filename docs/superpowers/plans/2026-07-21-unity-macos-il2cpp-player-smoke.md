# Unity macOS IL2CPP Player Smoke Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the existing automatic Unity type binding executes in a built,
Medium-stripping macOS IL2CPP Player and emits a validated JSON result.

**Architecture:** A Player-only runtime assembly starts through Unity runtime
initialization, performs an in-memory DataVo conversion scenario, writes one
report under `Application.persistentDataPath`, and exits with its result. An
Editor build entry point forces macOS IL2CPP plus Medium stripping; a shell
runner prepares the exact candidate, builds, launches, and validates it.

**Tech Stack:** Unity `6000.5.4f1` (`d550df8bd089`), macOS standalone
IL2CPP, UnityEngine, Unity.Mathematics, DataVo portable candidate, Bash,
Node.js.

## Global Constraints

- Keep `DataVo.Core` Unity-free; do not change its public API.
- Consume only the existing generated candidate from
  `scripts/prepare-unity-smoke-candidate.sh`.
- The only valid Player backend is `ScriptingImplementation.IL2CPP`, with
  `ManagedStrippingLevel.Medium`; never fall back to Mono.
- The required Unity Hub module is `mac-il2cpp` (Mac Build Support (IL2CPP))
  for `6000.5.4f1`. Its absence is an explicit environmental failure.
- This uses `StorageMode.InMemory` only and makes no Windows, Burst, Jobs,
  persistence, performance, or public-support claim.
- Do not add `link.xml` or weaken stripping to hide an AOT/reflection failure.
- Preserve unrelated untracked files; stage exact files only, force-staging
  documents under `docs/superpowers/`.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `.gitignore` | Ignores the generated blank Player scene and metadata. |
| `Assets/DataVoSmoke/Runtime/DataVo.UnitySmoke.Player.asmdef` | Runtime-only smoke assembly and package/Core references. |
| `Assets/DataVoSmoke/Runtime/PlayerSmokeBootstrap.cs` | Scenario, JSON report, and Player shutdown. |
| `Assets/DataVoSmoke/Editor/PlayerSmokeBuild.cs` | Batch-mode deterministic IL2CPP build entry point. |
| `scripts/test-unity-macos-il2cpp-player-smoke.sh` | Fast source/runner contract test. |
| `scripts/run-unity-macos-il2cpp-player-smoke.sh` | Candidate preparation, build, launch, timeout, and JSON validation. |
| `scripts/check-unity-support-status.sh` and Unity docs | Evidence-supported status wording after a real pass. |

### Task 1: Add the Player runtime scenario and a failing contract test

**Files:**
- Create: `tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Runtime/DataVo.UnitySmoke.Player.asmdef`
- Create: `tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Runtime/PlayerSmokeBootstrap.cs`
- Create: `scripts/test-unity-macos-il2cpp-player-smoke.sh`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: `UseUnityTypes()`, `Insert<T>()`, `Map<T>()`, and
  `Get<T>()` from the existing candidate and `DataVo.Unity.TypeBinding`.
- Produces: `DataVo.UnitySmoke.Player.PlayerSmokeBootstrap` and version-one
  report fields `schemaVersion`, `status`, `unityVersion`, `platform`,
  `scriptingBackend`, `scenarioCount`, and `error`.

- [ ] **Step 1: Write the failing source-contract test**

Create `scripts/test-unity-macos-il2cpp-player-smoke.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runtime="$root/tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Runtime/PlayerSmokeBootstrap.cs"
asmdef="$root/tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Runtime/DataVo.UnitySmoke.Player.asmdef"
require_text() {
  rg -Fq -- "$2" "$1" || { echo "Missing Player smoke contract: $2" >&2; exit 1; }
}
require_text "$runtime" "[RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.AfterSceneLoad)]"
require_text "$runtime" "context.UseUnityTypes();"
require_text "$runtime" "context.Insert(\"States\", source);"
require_text "$runtime" "context.Map<PlayerSmokeState>(row)"
require_text "$runtime" "context.Get<Vector3>(row, \"Position\")"
require_text "$runtime" "Application.Quit(exitCode);"
require_text "$runtime" "Application.persistentDataPath"
require_text "$asmdef" "DataVo.Unity.TypeBinding"
require_text "$asmdef" "DataVo.Core.dll"
echo "Unity macOS IL2CPP Player smoke source contract passed."
```

- [ ] **Step 2: Run the contract test and observe failure**

Run: `bash scripts/test-unity-macos-il2cpp-player-smoke.sh`

Expected: failure because both runtime files are absent.

- [ ] **Step 3: Add the runtime assembly and ignored scene location**

Append this exact line to `.gitignore`:

```gitignore
/tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/.generated/
```

Create the asmdef:

```json
{
  "name": "DataVo.UnitySmoke.Player",
  "rootNamespace": "DataVo.UnitySmoke.Player",
  "references": ["DataVo.Unity.TypeBinding", "Unity.Mathematics"],
  "includePlatforms": [],
  "excludePlatforms": [],
  "allowUnsafeCode": false,
  "overrideReferences": true,
  "precompiledReferences": ["DataVo.Core.dll"],
  "autoReferenced": true,
  "defineConstraints": [],
  "versionDefines": [],
  "noEngineReferences": false
}
```

- [ ] **Step 4: Implement the self-contained Player bootstrap**

Create `PlayerSmokeBootstrap.cs` in namespace
`DataVo.UnitySmoke.Player`. It must declare a model with all eight registered
types: `Vector2`, `Vector3`, `Vector4`, `Quaternion`, `float2`,
`float3`, `float4`, and `quaternion`. Use the concrete values and
component-by-component quaternion checks from `AdapterImportTests`.

```csharp
[RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.AfterSceneLoad)]
private static void Start()
{
    PlayerSmokeResult result;
    try { result = RunScenario(); }
    catch (Exception exception) { result = PlayerSmokeResult.Failed(Bound(exception.Message)); }
    WriteResult(result);
    int exitCode = result.status == "passed" ? 0 : 1;
    Application.Quit(exitCode);
}

private static PlayerSmokeResult RunScenario()
{
    using var context = CreateInMemoryContext();
    context.UseUnityTypes();
    context.Execute("CREATE TABLE States (Id INT PRIMARY KEY, Position VECTOR(3), Velocity VECTOR(3), Rotation VECTOR(4), SimulationRotation VECTOR(4), ScreenPosition VECTOR(2), Drift VECTOR(2), Four VECTOR(4), MathematicsFour VECTOR(4))");
    var source = CreateSource();
    context.Insert("States", source);
    var row = context.Execute("SELECT Id, Position, Velocity, Rotation, SimulationRotation, ScreenPosition, Drift, Four, MathematicsFour FROM States").Single().Data.Single();
    PlayerSmokeState mapped = context.Map<PlayerSmokeState>(row);
    Vector3 position = context.Get<Vector3>(row, "Position");
    AssertEquivalent(source, mapped, position);
    AssertVectorLengthFailure(context);
    return PlayerSmokeResult.Passed();
}
```

`AssertVectorLengthFailure` must call `Get<Vector3>` on a materialized
`float[2]` and require both `VECTOR(3)` and `VECTOR(2)` in the error.
`WriteResult` accepts only a command-line
`-datavoSmokeRunId <[A-Za-z0-9_-]+>`, then writes:

```csharp
string path = Path.Combine(Application.persistentDataPath, "DataVoSmoke", runId, "result.json");
Directory.CreateDirectory(Path.GetDirectoryName(path)!);
File.WriteAllText(path, JsonUtility.ToJson(result));
Debug.Log("DATAVO_SMOKE_RESULT=" + path);
```

Set `scriptingBackend` to `IL2CPP` under `#if ENABLE_IL2CPP`, otherwise
`non-il2cpp`. The error field is capped at 1,024 characters and never includes
a stack trace.

- [ ] **Step 5: Run the contract test and observe success**

Run: `bash scripts/test-unity-macos-il2cpp-player-smoke.sh`

Expected: `Unity macOS IL2CPP Player smoke source contract passed.`

- [ ] **Step 6: Commit the runtime foundation**

```bash
git add .gitignore scripts/test-unity-macos-il2cpp-player-smoke.sh \
  tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Runtime/DataVo.UnitySmoke.Player.asmdef \
  tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Runtime/PlayerSmokeBootstrap.cs
git commit -m "test: add Unity IL2CPP player smoke runtime"
```

### Task 2: Add deterministic build and execute validation

**Files:**
- Create: `tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Editor/PlayerSmokeBuild.cs`
- Create: `scripts/run-unity-macos-il2cpp-player-smoke.sh`
- Modify: `scripts/test-unity-macos-il2cpp-player-smoke.sh`

**Interfaces:**
- Consumes: Task 1 runtime bootstrap and existing candidate/smoke preparation
  scripts.
- Produces: batch method
  `DataVo.UnitySmoke.Editor.PlayerSmokeBuild.BuildMacOSIl2Cpp` and the
  reproducible host command.

- [ ] **Step 1: Extend the contract test first**

Add checks that the new source contains all of:

```bash
require_text "$build" "ScriptingImplementation.IL2CPP"
require_text "$build" "ManagedStrippingLevel.Medium"
require_text "$build" "BuildTarget.StandaloneOSX"
require_text "$build" "mac-il2cpp"
require_text "$runner" "prepare-unity-smoke-candidate.sh"
require_text "$runner" "-executeMethod DataVo.UnitySmoke.Editor.PlayerSmokeBuild.BuildMacOSIl2Cpp"
require_text "$runner" "-datavoSmokeRunId"
require_text "$runner" "DATAVO_SMOKE_RESULT="
require_text "$runner" "scriptingBackend !== 'IL2CPP'"
```

- [ ] **Step 2: Run the extended contract test and observe failure**

Run: `bash scripts/test-unity-macos-il2cpp-player-smoke.sh`

Expected: failure because the build class and runner are absent.

- [ ] **Step 3: Implement the Editor build entry point**

Create `PlayerSmokeBuild.cs` under the existing `Editor` directory:

```csharp
public static void BuildMacOSIl2Cpp()
{
    RequireMacIl2CppModule();
    string output = RequireArgument("-datavoSmokeBuildPath");
    string scene = EnsureGeneratedEmptyScene();
    var target = NamedBuildTarget.Standalone;
    var oldBackend = PlayerSettings.GetScriptingBackend(target);
    var oldStripping = PlayerSettings.GetManagedStrippingLevel(target);
    try
    {
        PlayerSettings.SetScriptingBackend(target, ScriptingImplementation.IL2CPP);
        PlayerSettings.SetManagedStrippingLevel(target, ManagedStrippingLevel.Medium);
        BuildReport report = BuildPipeline.BuildPlayer(new BuildPlayerOptions
        {
            scenes = new[] { scene },
            locationPathName = output,
            target = BuildTarget.StandaloneOSX,
            options = BuildOptions.None
        });
        if (report.summary.result != BuildResult.Succeeded)
            throw new BuildFailedException(report.summary.result.ToString());
    }
    finally
    {
        PlayerSettings.SetScriptingBackend(target, oldBackend);
        PlayerSettings.SetManagedStrippingLevel(target, oldStripping);
    }
}
```

`RequireMacIl2CppModule` must search the installed Editor's
`PlaybackEngines/MacStandaloneSupport/Variations` for one directory ending
`_il2cpp`. On absence, throw an exception that names the Unity Hub
`mac-il2cpp` module and `6000.5.4f1`; it must not accept the existing Mono
variation. `EnsureGeneratedEmptyScene` creates and saves
`Assets/DataVoSmoke/.generated/PlayerSmoke.unity` through
`EditorSceneManager.NewScene` and `EditorSceneManager.SaveScene`.

- [ ] **Step 4: Implement the host runner**

Create the runner with this initial sequence:

```bash
#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
editor="/Applications/Unity/Hub/Editor/6000.5.4f1/Unity.app/Contents/MacOS/Unity"
project="$root/tests/unity/DataVo.UnitySmoke"
output_root="$(mktemp -d /private/tmp/datavo-unity-il2cpp.XXXXXX)"
app="$output_root/DataVoUnitySmoke.app"
editor_log="$output_root/editor.log"
player_log="$output_root/player.log"
run_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
bash "$root/scripts/prepare-unity-smoke-candidate.sh"
bash "$root/scripts/check-unity-smoke-project.sh" --require-candidate
"$editor" -batchmode -quit -projectPath "$project" \
  -executeMethod DataVo.UnitySmoke.Editor.PlayerSmokeBuild.BuildMacOSIl2Cpp \
  -datavoSmokeBuildPath "$app" -logFile "$editor_log"
"$app/Contents/MacOS/DataVoUnitySmoke" -batchmode -nographics \
  -datavoSmokeRunId "$run_id" -logFile "$player_log"
```

Capture the Player exit code outside `set -e`. Add a 120-second watchdog
which terminates only the child process started by this script. After exit,
extract exactly one `DATAVO_SMOKE_RESULT=` line from the Player log. A Node
validation block must reject a missing/duplicate path, malformed JSON,
`schemaVersion !== 1`, `status !== 'passed'`,
`scriptingBackend !== 'IL2CPP'`, or `scenarioCount < 3`. A zero Player exit
without the valid report is a failure. On every failure print both log paths;
on success print app path, report path, Unity version, and backend.

- [ ] **Step 5: Run the complete source contract**

Run: `bash scripts/test-unity-macos-il2cpp-player-smoke.sh`

Expected: `Unity macOS IL2CPP Player smoke source contract passed.`

- [ ] **Step 6: Commit the build/run boundary**

```bash
git add scripts/test-unity-macos-il2cpp-player-smoke.sh \
  scripts/run-unity-macos-il2cpp-player-smoke.sh \
  tests/unity/DataVo.UnitySmoke/Assets/DataVoSmoke/Editor/PlayerSmokeBuild.cs
git commit -m "test: run Unity macOS IL2CPP player smoke"
```

### Task 3: Execute the true gate and record only observed evidence

**Files:**
- Modify: `scripts/check-unity-support-status.sh`
- Modify: `docs/manual/preface/alpha-scope.md`
- Modify: `docs/features/unity-and-godot.md`
- Modify: `integrations/unity/com.datavo.unity.mathematics/README.md`
- Modify: `integrations/unity/com.datavo.unity.mathematics/Documentation~/README.md`
- Modify: this plan's `Evidence` section

**Interfaces:**
- Consumes: a real successful
  `scripts/run-unity-macos-il2cpp-player-smoke.sh` execution.
- Produces: truthful scoped documentation and a status-document contract.

- [ ] **Step 1: Run non-Player regressions**

Run:

```bash
bash scripts/test-unity-mathematics-package.sh --all
bash scripts/test-prepare-unity-candidate.sh
bash scripts/check-unity-smoke-project.sh
bash scripts/test-unity-macos-il2cpp-player-smoke.sh
```

Expected: all pass. A fresh candidate restore may need approved network access
only if its intentional isolated NuGet restore hits sandbox DNS failure.

- [ ] **Step 2: Verify or install the exact module**

Use Unity Hub to add **Mac Build Support (IL2CPP)** (`mac-il2cpp`) to Editor
`6000.5.4f1`, then run:

```bash
find /Applications/Unity/Hub/Editor/6000.5.4f1/PlaybackEngines/MacStandaloneSupport/Variations \
  -maxdepth 1 -type d -name '*_il2cpp' -print
```

Expected: at least one directory. If none appears, stop: Player evidence remains
unverified and no documentation claim changes.

- [ ] **Step 3: Run the real build-and-execute gate**

Run: `bash scripts/run-unity-macos-il2cpp-player-smoke.sh`

Expected: a macOS IL2CPP Player built with Medium stripping launches, writes a
version-one successful report, and exits zero. A build, AOT, stripping,
reflection, launch, report, timeout, or exit failure blocks the proof.

- [ ] **Step 4: Update only documentation supported by Step 3**

After a real pass, change the two support tables and their checker to require:

```markdown
| Unity Editor | Executed macOS ARM64 Editor smoke: 6000.5.4f1 only |
| macOS IL2CPP | Executed Player smoke: 6000.5.4f1, Medium stripping, in-memory only |
| Windows x64 IL2CPP | Unverified |
| Direct Burst calls | Unsupported by design |
| Job-to-managed batch bridge | Planned proof |
| In-memory mode | Candidate smoke scope only |
| Disk/LSM persistence | Unsupported until separately validated |
```

Update both package READMEs with the same narrow result and explicitly retain
the exclusions: Windows x64, Burst, Jobs/Collections, persistent save data,
broad performance claims, and public production support. Append Unity revision,
IL2CPP variation name, report/result summary, and `git diff --check HEAD`
outcome to this plan; never put local absolute paths in public docs.

- [ ] **Step 5: Run final verification**

Run:

```bash
bash scripts/test-unity-mathematics-package.sh --all
bash scripts/test-prepare-unity-candidate.sh
bash scripts/check-unity-smoke-project.sh --require-candidate
bash scripts/test-unity-macos-il2cpp-player-smoke.sh
bash scripts/run-unity-editor-tests.sh
bash scripts/run-unity-macos-il2cpp-player-smoke.sh
bash scripts/check-unity-support-status.sh
git diff --check HEAD
```

Expected: every targeted check passes. Do not claim the unrelated full DataVo
Disk-mode suite passed; it has existing `Catalog.xml` test interference.

- [ ] **Step 6: Commit the evidence**

```bash
git add scripts/check-unity-support-status.sh \
  docs/manual/preface/alpha-scope.md docs/features/unity-and-godot.md \
  integrations/unity/com.datavo.unity.mathematics/README.md \
  integrations/unity/com.datavo.unity.mathematics/Documentation~/README.md
git add -f docs/superpowers/plans/2026-07-21-unity-macos-il2cpp-player-smoke.md
git commit -m "docs: record Unity macOS IL2CPP player smoke"
```

## Plan Self-Review

- Task 1 covers the complete automatic binding scenario and a fresh Player
  report; Task 2 enforces build settings, launch, timeout, and report validation;
  Task 3 gates any documentation claim on execution.
- The exact run-ID contract, `BuildMacOSIl2Cpp` method name, Player app name,
  backend, and stripping level agree across all tasks.
- No Core change, Windows claim, durable storage path, Burst/Jobs work, or
  additional Unity type was introduced.
- The sole external prerequisite is named precisely as Unity Hub module
  `mac-il2cpp`; the current installation does not have an `*_il2cpp`
  variation, so execution will stop clearly until it is installed.

## Evidence

Implementation has not started. Local inspection on 2026-07-21 found only Mono
macOS variations under the pinned Editor's `MacStandaloneSupport` directory;
the required `mac-il2cpp` module is not installed.
