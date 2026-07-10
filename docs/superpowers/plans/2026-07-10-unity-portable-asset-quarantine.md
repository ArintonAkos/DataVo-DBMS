# Unity Portable-Asset Quarantine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete Stage 1 of the approved Unity compatibility reset by publishing only the verified `net10.0` Core asset, retaining `netstandard2.1` behind an explicit experimental build property, removing the misleading portable benchmark lanes, and replacing Unity support claims with a tested-status matrix.

**Architecture:** `DataVo.Core` remains one source tree, but its default target set becomes `net10.0`. Setting `DataVoEnablePortableTarget=true` adds `netstandard2.1` for repair work without adding it to normal release output. The release gate validates the contents of the final `.nupkg`, not merely MSBuild properties. The manually dispatched benchmark workflow measures only the modern asset during quarantine. A canonical support matrix in the public alpha manual is mirrored in the repository-facing Unity page and enforced by a small documentation guard.

**Tech Stack:** MSBuild/.NET 10, Bash, NuGet package validation, GitHub Actions YAML, VitePress/Node 20, Markdown.

## Global Constraints

- Implement only Stage 1 from `docs/superpowers/specs/2026-07-10-unity-compatibility-reset-design.md`. Do not begin the Stage 2 API, compatibility-adapter, HNSW, or performance-restoration work.
- Preserve all current compatibility source files. This stage quarantines the portable package asset; it does not delete the experimental implementation.
- The property name is exactly `DataVoEnablePortableTarget`, defaults to `false`, and adds `netstandard2.1` only when explicitly set to `true`.
- A normal `dotnet pack` must contain exactly one `DataVo.Core.dll` under `lib/net10.0/` and no `lib/netstandard2.1/` entries.
- `EnablePackageValidation` must be on for every pack operation. The normal package must pass it. An experimental multi-target package is not a Stage 1 deliverable and is expected to remain blocked by the known `DateOnly`/`TimeOnly` API mismatch until Stage 2.
- The experimental target must still build with:

  ```bash
  dotnet build DataVo.Core/DataVo.Core.csproj \
    -c Release \
    -f netstandard2.1 \
    -p:DataVoEnablePortableTarget=true
  ```

- Do not describe a `netstandard2.1` DLL loaded by a .NET 10 host as Unity performance.
- Do not claim direct Burst compatibility. DataVo remains managed code; the planned integration boundary is a POD batch produced by a job and drained by managed code after completion.
- Do not touch or stage the existing unrelated untracked benchmark images, architecture images, Reddit draft, or plot generator.
- Use `apply_patch` for source edits. Stage exact paths rather than `git add .`.
- Baseline for this plan is design commit `2186e31`.

---

### Task 1: Make the portable target opt-in and prove the final package shape

**Files:**

- Create: `scripts/assert-core-package-targets.sh`
- Modify: `DataVo.Core/DataVo.Core.csproj:3-27`
- Modify: `Directory.Build.props:5-7,29-33`
- Modify: `scripts/verify-local-packages.sh:3-75`

- [ ] **Step 1: Add a package-content assertion before changing the project targets**

Create `scripts/assert-core-package-targets.sh` with this exact behavior:

```bash
#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <DataVo.Core.nupkg>" >&2
  exit 2
fi

readonly package_path="$1"

if [[ ! -f "$package_path" ]]; then
  echo "Core package does not exist: $package_path" >&2
  exit 2
fi

entries="$(unzip -Z1 "$package_path")"
readonly entries
portable_entries="$(
  printf '%s\n' "$entries" |
    sed -n '/netstandard2\.1/p'
)"
readonly portable_entries
core_dlls="$(
  printf '%s\n' "$entries" |
    sed -n '/^lib\/[^/]*\/DataVo\.Core\.dll$/p'
)"
readonly core_dlls

if [[ -n "$portable_entries" ]]; then
  echo "Public DataVo.Core package contains quarantined netstandard2.1 entries:" >&2
  printf '%s\n' "$portable_entries" >&2
  exit 1
fi

if [[ "$core_dlls" != "lib/net10.0/DataVo.Core.dll" ]]; then
  echo "Public DataVo.Core package must contain exactly lib/net10.0/DataVo.Core.dll." >&2
  echo "Found Core DLL entries:" >&2
  printf '%s\n' "${core_dlls:-<none>}" >&2
  exit 1
fi

echo "Core package target assertion passed: net10.0 only."
```

Make it executable:

```bash
chmod +x scripts/assert-core-package-targets.sh
```

- [ ] **Step 2: Run the new assertion against the current default package and verify that it fails**

Use an isolated output directory so ignored packages under `artifacts/` cannot produce a false result:

```bash
RED_DIR="$(mktemp -d)"
dotnet pack DataVo.Core/DataVo.Core.csproj \
  -c Release \
  -o "$RED_DIR" \
  -p:PackageVersion=0.0.0-stage1-red
bash scripts/assert-core-package-targets.sh \
  "$RED_DIR/DataVo.Core.0.0.0-stage1-red.nupkg"
```

Expected: the pack itself succeeds, then the assertion exits `1` and lists entries under `lib/netstandard2.1/`. If it passes, inspect the exact package supplied to the script before proceeding.

- [ ] **Step 3: Make `net10.0` the default and preserve an explicit portable build**

At the start of the main property group in `DataVo.Core/DataVo.Core.csproj`, replace the unconditional target list with:

```xml
<DataVoEnablePortableTarget Condition="'$(DataVoEnablePortableTarget)' == ''">false</DataVoEnablePortableTarget>
<TargetFrameworks>net10.0</TargetFrameworks>
<TargetFrameworks Condition="'$(DataVoEnablePortableTarget)' == 'true'">net10.0;netstandard2.1</TargetFrameworks>
```

Leave the existing target-conditioned AOT, tensors, and portable dependency groups intact. Do not add a publish-specific override and do not remove the portable package references.

- [ ] **Step 4: Enable package validation centrally and disclose the Core release boundary**

In the first property group in `Directory.Build.props`, add:

```xml
<EnablePackageValidation>true</EnablePackageValidation>
```

This property is intentionally global: it is inert for ordinary builds and ensures every packable project is validated whenever it is packed.

Replace the `DataVo.Core` package release notes with:

```xml
<PackageReleaseNotes>SQL engine, storage, indexing, and ALTER TABLE support. The public Core package currently ships net10.0 only; the experimental netstandard2.1 target is not published while Unity compatibility is being validated.</PackageReleaseNotes>
```

- [ ] **Step 5: Make local package verification isolated and invoke the content assertion**

In `scripts/verify-local-packages.sh`:

1. Create `TEMP_DIR` before `PACKAGES_DIR`.
2. Set `PACKAGES_DIR="$TEMP_DIR/packages"` instead of using `artifacts/packages`.
3. Create the package directory before packing.
4. Pass `-o "$PACKAGES_DIR"` to both pack commands.
5. Resolve exactly one Core package and one Data package.
6. Run the Core target assertion before creating the consumer app.

The top half of the script should have this shape; keep the existing consumer `Program.cs` and package-consumer behavior unchanged:

```bash
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TEMP_DIR="$(mktemp -d)"
PACKAGES_DIR="$TEMP_DIR/packages"
APP_DIR="$TEMP_DIR/DataVo.PackageSmoke"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

mkdir -p "$PACKAGES_DIR"

echo "[1/6] Packing DataVo.Core and DataVo.Data..."
dotnet pack "$ROOT_DIR/DataVo.Core/DataVo.Core.csproj" \
  -c Release \
  -o "$PACKAGES_DIR" >/dev/null
dotnet pack "$ROOT_DIR/DataVo.Data/DataVo.Data.csproj" \
  -c Release \
  -o "$PACKAGES_DIR" >/dev/null

echo "[2/6] Verifying package artifacts exist..."
CORE_PACKAGE="$(
  find "$PACKAGES_DIR" -maxdepth 1 -type f \
    -name 'DataVo.Core.*.nupkg' -print
)"
DATA_PACKAGE="$(
  find "$PACKAGES_DIR" -maxdepth 1 -type f \
    -name 'DataVo.Data.*.nupkg' -print
)"

if [[ -z "$CORE_PACKAGE" || "$CORE_PACKAGE" == *$'\n'* ]]; then
  echo "Expected exactly one DataVo.Core nupkg, found:" >&2
  printf '%s\n' "${CORE_PACKAGE:-<none>}" >&2
  exit 1
fi

if [[ -z "$DATA_PACKAGE" || "$DATA_PACKAGE" == *$'\n'* ]]; then
  echo "Expected exactly one DataVo.Data nupkg, found:" >&2
  printf '%s\n' "${DATA_PACKAGE:-<none>}" >&2
  exit 1
fi

echo "[3/6] Verifying the public Core target boundary..."
bash "$ROOT_DIR/scripts/assert-core-package-targets.sh" "$CORE_PACKAGE"

echo "[4/6] Creating temporary consumer app..."
```

Renumber the two remaining progress messages to `[5/6]` and `[6/6]`. Continue installing by package ID from `"$PACKAGES_DIR"` so the smoke app consumes the isolated packages.

- [ ] **Step 6: Verify the default package, evaluated properties, and experimental build**

Run:

```bash
bash scripts/verify-local-packages.sh

dotnet msbuild DataVo.Core/DataVo.Core.csproj \
  -getProperty:DataVoEnablePortableTarget \
  -getProperty:TargetFrameworks \
  -getProperty:EnablePackageValidation

dotnet msbuild DataVo.Core/DataVo.Core.csproj \
  -p:DataVoEnablePortableTarget=true \
  -getProperty:DataVoEnablePortableTarget \
  -getProperty:TargetFrameworks \
  -getProperty:EnablePackageValidation

dotnet build DataVo.Core/DataVo.Core.csproj \
  -c Release \
  -f netstandard2.1 \
  -p:DataVoEnablePortableTarget=true
```

Expected:

- package smoke prints `Core package target assertion passed: net10.0 only` and its final success line;
- default properties report `false`, `net10.0`, and `true`;
- opt-in properties report `true`, `net10.0;netstandard2.1`, and `true`;
- the experimental target builds successfully. The four existing nullable warnings in `TopKReactiveQuery.cs` may remain; do not broaden this stage to fix them.

Do not require an opt-in multi-target pack to pass yet. Package validation is expected to expose the public `DateOnly`/`TimeOnly` mismatch until Stage 2.

- [ ] **Step 7: Commit the package quarantine**

```bash
git add \
  DataVo.Core/DataVo.Core.csproj \
  Directory.Build.props \
  scripts/assert-core-package-targets.sh \
  scripts/verify-local-packages.sh
git commit -m "build: quarantine portable Core package asset"
```

---

### Task 2: Gate the tag release on the packed artifact

**Files:**

- Modify: `.github/workflows/publish-nuget.yml:24-50`

- [ ] **Step 1: Confirm the publishing workflow has no final-artifact assertion**

Run:

```bash
rg -n "assert-core-package-targets" .github/workflows/publish-nuget.yml
```

Expected: no matches and exit code `1`. This is the missing release gate.

- [ ] **Step 2: Add the assertion after all packages are packed and before NuGet login**

Insert this step immediately after `Pack`:

```yaml
      - name: Verify release package contents
        shell: bash
        run: >-
          bash scripts/assert-core-package-targets.sh
          artifacts/packages/DataVo.Core.*.nupkg
```

Do not pass `DataVoEnablePortableTarget` anywhere in this workflow. A clean runner produces exactly one matching Core `.nupkg`; zero or multiple arguments make the assertion fail before credentials are requested.

- [ ] **Step 3: Verify the workflow guardrails locally**

Run:

```bash
rg -n "assert-core-package-targets" .github/workflows/publish-nuget.yml

if rg -n "DataVoEnablePortableTarget" .github/workflows/publish-nuget.yml; then
  echo "Publish workflow must not opt into the portable target." >&2
  exit 1
fi

bash scripts/verify-local-packages.sh
```

Expected: one assertion invocation, no portable-property match, and a passing local package smoke test.

- [ ] **Step 4: Commit the release gate**

```bash
git add .github/workflows/publish-nuget.yml
git commit -m "ci: enforce portable package quarantine"
```

---

### Task 3: Remove portable measurements from the manual benchmark workflow

**Files:**

- Modify: `.github/workflows/benchmark.yml:17-29,86-103,119`

- [ ] **Step 1: Demonstrate that the workflow still schedules the quarantined asset**

Run:

```bash
rg -n "netstandard2\.1|matrix\.datavo_core" .github/workflows/benchmark.yml
```

Expected: matches for two matrix rows, the target override, and the alternate host path.

- [ ] **Step 2: Reduce the matrix to modern Linux and Windows measurements**

Keep these two matrix entries and remove each `datavo_core` key:

```yaml
        include:
          - label: linux-x64-net10
            runner: ubuntu-latest
          - label: windows-x64-net10
            runner: windows-latest
```

Change the benchmark-host build step to:

```yaml
      - name: Build benchmark host
        run: dotnet build demos/Research.Benchmark/src/Research.Benchmark.Host/Research.Benchmark.Host.csproj -c Release --no-restore
```

In the run script, replace the target-dependent host selection with:

```bash
HOST_DLL="demos/Research.Benchmark/src/Research.Benchmark.Host/bin/Release/net10.0/Research.Benchmark.Host.dll"
```

Change the log metadata line to:

```bash
echo "DataVo.Core target: net10.0"
```

Leave the benchmark host and runner project support for an explicitly labeled local `netstandard2.1` experiment in place. This task disables the GitHub lane; it does not delete research scaffolding.

- [ ] **Step 3: Verify that the workflow has no quarantined lane and the modern host builds**

Run:

```bash
if rg -n "netstandard2\.1|matrix\.datavo_core" .github/workflows/benchmark.yml; then
  echo "Quarantined benchmark lane is still reachable." >&2
  exit 1
fi

dotnet build \
  demos/Research.Benchmark/src/Research.Benchmark.Host/Research.Benchmark.Host.csproj \
  -c Release
```

Expected: no workflow matches and a successful `net10.0` host build.

- [ ] **Step 4: Commit the benchmark quarantine**

```bash
git add .github/workflows/benchmark.yml
git commit -m "ci: quarantine portable benchmark lanes"
```

---

### Task 4: Publish and enforce the honest Unity evaluation status

**Files:**

- Create: `scripts/check-unity-support-status.sh`
- Modify: `README.md:233-237,271,279-284`
- Modify: `docs/manual/preface/alpha-scope.md:53-67`
- Modify: `docs/manual/preface/roadmap.md:42-67`
- Modify: `docs/features/unity-and-godot.md:1-90`
- Modify: `docs/features/setup-and-packaging.md:12-33,82-88`
- Modify: `docs/features/roadmap-and-integrations.md:55-59`
- Modify: `docs/features/index.md:32-34,80,97,133-141`
- Modify: `.github/workflows/publish-nuget.yml`
- Modify: `.github/workflows/deploy-docs.yml:3-9,33-39`
- Regenerate: `docs/public/ai/index.json`
- Regenerate: `docs/public/ai/pages/manual/preface/alpha-scope.md`
- Regenerate: `docs/public/ai/pages/manual/preface/roadmap.md`
- Regenerate: `docs/public/llms-full.txt`

- [ ] **Step 1: Add a documentation-status guard**

Create `scripts/check-unity-support-status.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

require_text() {
  local file="$1"
  local expected="$2"

  if ! rg -Fq -- "$expected" "$file"; then
    echo "Missing required Unity status text in $file:" >&2
    echo "  $expected" >&2
    return 1
  fi
}

reject_text() {
  local file="$1"
  local forbidden="$2"

  if rg -Fq -- "$forbidden" "$file"; then
    echo "Found obsolete Unity support claim in $file:" >&2
    echo "  $forbidden" >&2
    return 1
  fi
}

readonly support_rows=(
  "| Unity Editor | Unverified until Stage 3 |"
  "| Windows x64 IL2CPP | Unverified until Stage 3 |"
  "| Direct Burst calls | Unsupported by design |"
  "| Job-to-managed batch bridge | Planned proof |"
  "| In-memory mode | Candidate scope |"
  "| Disk/LSM persistence | Unsupported until separately validated |"
)

for support_file in \
  "$ROOT/docs/manual/preface/alpha-scope.md" \
  "$ROOT/docs/features/unity-and-godot.md"; do
  for row in "${support_rows[@]}"; do
    require_text "$support_file" "$row"
  done
done

require_text \
  "$ROOT/README.md" \
  "Unity is an evaluation target, not a supported runtime."
require_text \
  "$ROOT/docs/features/setup-and-packaging.md" \
  "The public DataVo.Core package currently contains only a net10.0 asset."
require_text \
  "$ROOT/docs/features/roadmap-and-integrations.md" \
  "Unity is an evaluation target, not an available runtime integration."
require_text \
  "$ROOT/docs/features/index.md" \
  "### I want to evaluate future Unity or Godot support"

reject_text \
  "$ROOT/README.md" \
  "Use DataVo as a local gameplay/profile/state database."
reject_text \
  "$ROOT/README.md" \
  "Keep persistence and query behavior deterministic across development environments."
reject_text \
  "$ROOT/README.md" \
  "Reuse the same SQL surface in tools and runtime workflows."
reject_text \
  "$ROOT/docs/features/setup-and-packaging.md" \
  "Unity and Godot teams: use disk mode for persistent save/profile data"
reject_text \
  "$ROOT/docs/features/roadmap-and-integrations.md" \
  "DataVo is suitable for game-development local data workflows"
reject_text \
  "$ROOT/docs/features/index.md" \
  "### I want to ship to Unity or Godot"
reject_text \
  "$ROOT/docs/features/unity-and-godot.md" \
  "StorageMode = StorageMode.Disk"

echo "Unity support-status documentation check passed."
```

Make it executable:

```bash
chmod +x scripts/check-unity-support-status.sh
```

- [ ] **Step 2: Run the guard and verify that the current claims fail**

```bash
bash scripts/check-unity-support-status.sh
```

Expected: non-zero exit with the first missing required matrix row. This establishes that adding a disclaimer alone is insufficient.

- [ ] **Step 3: Replace the README support claim with evaluation language**

Replace the current `Unity and Godot developers` bullets with:

```markdown
### Unity and Godot evaluation

- Unity is an evaluation target, not a supported runtime.
- The current public packages are .NET 10 assets; no verified Unity or Godot runtime artifact is distributed.
- The planned first Unity proof covers in-memory execution in the Editor and a Windows x64 IL2CPP player.
- Direct Burst calls are unsupported by design. The planned boundary is a blittable job batch drained by managed code after the job completes.
- Disk and LSM persistence are unsupported for Unity save data until separately validated.
```

Rename the documentation link to `Unity and Godot evaluation status`, and add this status bullet:

```markdown
- Unity and Godot runtime compatibility remains unverified; do not treat the current packages as game-runtime support.
```

- [ ] **Step 4: Add the canonical public support matrix**

In `docs/manual/preface/alpha-scope.md`, add this section before `v0.1 Scope Summary`:

```markdown
## Unity Evaluation Status

Unity is an evaluation target, not a supported runtime in v0.1. The public `DataVo.Core` package contains only a `net10.0` asset. An experimental portable target remains internal until the same final artifact passes a tracked Unity Editor and player proof.

| Environment or boundary | Status | Meaning |
| --- | --- | --- |
| Unity Editor | Unverified until Stage 3 | No tracked Unity project has imported and executed the candidate package yet. |
| Windows x64 IL2CPP | Unverified until Stage 3 | A built and executed IL2CPP player is required; compilation alone is insufficient. |
| Direct Burst calls | Unsupported by design | DataVo uses managed engine types and services and is not a Burst job payload. |
| Job-to-managed batch bridge | Planned proof | A Burst-compatible job may produce POD commands that managed code drains after completion. |
| In-memory mode | Candidate scope | This is the only storage mode included in the first Unity proof. |
| Disk/LSM persistence | Unsupported until separately validated | Do not use DataVo for Unity save data until durability is tested on each advertised platform. |
```

Update the `NuGet launch packaging` row so its notes say that the public Core package currently contains only `net10.0`; portable/Unity packaging is not supported.

In `docs/manual/preface/roadmap.md`, add a short proof-first Unity paragraph before the tooling-roadmap paragraph:

```markdown
Unity integration follows a separate proof-first path. The first candidate is managed, in-memory use under a pinned Unity Editor and an executed Windows x64 IL2CPP player. Direct Burst calls and unverified game-save persistence are outside that scope.
```

Add this row to `Roadmap Summary`:

```markdown
| Unity managed integration proof | Planned | Editor and Windows x64 IL2CPP execution must pass before a portable Core asset is published. |
```

Update the `NuGet packages` row to state that the current public Core package is `net10.0` only.

- [ ] **Step 5: Rewrite the repository-facing game-engine page as an evaluation page**

Replace `docs/features/unity-and-godot.md` with content organized under these sections:

```markdown
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
```

Do not retain the disk-mode sample or direct package-install instructions.

- [ ] **Step 6: Correct the remaining feature-documentation claims**

In `docs/features/setup-and-packaging.md`, add this subsection after the NuGet install flow:

```markdown
### Current target-framework boundary

The public DataVo.Core package currently contains only a net10.0 asset. The experimental `netstandard2.1` target is quarantined and is not a supported or distributed Unity artifact.
```

Change the game-engine guidance bullets to say:

```markdown
- Unity and Godot teams: evaluation only; do not use the current packages as a shipped runtime or save-data dependency.
- The planned first Unity proof is limited to `StorageMode.InMemory`; Disk and LSM require separate platform durability validation.
```

In `docs/features/roadmap-and-integrations.md`, move the game-engine material out of the `Available today` claim by replacing it with:

```markdown
## Unity and Godot evaluation

Unity is an evaluation target, not an available runtime integration. The first planned proof uses an in-memory managed boundary in a pinned Unity Editor and an executed Windows x64 IL2CPP player. Godot requires a separate validation pass.

See [Unity and Godot](./unity-and-godot.md) for the current support matrix and proof boundary.
```

In `docs/features/index.md`:

- rename the feature card to `Unity and Godot evaluation`;
- describe it as a support matrix and proof plan, not local persistence guidance;
- change the audience text to teams evaluating future game-engine integrations;
- rename `I want to ship to Unity or Godot` to `I want to evaluate future Unity or Godot support`;
- keep the reading links, but do not call them shipping instructions.

- [ ] **Step 7: Run the documentation guard and regenerate the curated public export**

```bash
bash scripts/check-doc-api-samples.sh
bash scripts/check-unity-support-status.sh

cd docs
npm ci
npm run docs:build
cd ..

git diff --check
git diff --name-only
```

Expected:

- the existing documentation API-sample check passes;
- the guard prints its success line;
- VitePress builds successfully;
- the AI generator updates the alpha-scope and roadmap exports plus `llms-full.txt`; `ai/index.json` receives the normal generation timestamp;
- feature pages remain repository-facing and excluded from the curated VitePress source set;
- no unrelated file changes appear.

- [ ] **Step 8: Gate both release packaging and docs deployment on the status check**

In `.github/workflows/publish-nuget.yml`, add this step before `Pack`:

```yaml
      - name: Verify runtime support claims
        run: bash scripts/check-unity-support-status.sh
```

In `.github/workflows/deploy-docs.yml`, add the same step after checkout and before Node setup:

```yaml
      - name: Verify runtime support claims
        run: bash scripts/check-unity-support-status.sh
```

Also add `README.md` and `scripts/check-unity-support-status.sh` to that workflow's `on.push.paths` list so changes to either checked surface execute the docs gate.

Then run:

```bash
bash scripts/check-unity-support-status.sh
rg -n "check-unity-support-status" \
  .github/workflows/publish-nuget.yml \
  .github/workflows/deploy-docs.yml
```

Expected: the guard passes and both workflows contain one invocation.

- [ ] **Step 9: Commit the documentation boundary**

Review `git diff --stat` and stage only:

```bash
git add \
  README.md \
  docs/manual/preface/alpha-scope.md \
  docs/manual/preface/roadmap.md \
  docs/features/unity-and-godot.md \
  docs/features/setup-and-packaging.md \
  docs/features/roadmap-and-integrations.md \
  docs/features/index.md \
  docs/public/ai/index.json \
  docs/public/ai/pages/manual/preface/alpha-scope.md \
  docs/public/ai/pages/manual/preface/roadmap.md \
  docs/public/llms-full.txt \
  scripts/check-unity-support-status.sh \
  .github/workflows/publish-nuget.yml \
  .github/workflows/deploy-docs.yml
git commit -m "docs: mark Unity support as unverified"
```

If the generator changes an additional tracked generated file, inspect it and add it only when it is a direct deterministic consequence of the two manual source edits.

---

### Task 5: Run the Stage 1 exit review

**Files:**

- Verify only; modify files only if a failed check identifies a Stage 1 defect.

- [ ] **Step 1: Verify both Core build modes**

```bash
dotnet build DataVo.Core/DataVo.Core.csproj -c Release

dotnet build DataVo.Core/DataVo.Core.csproj \
  -c Release \
  -f netstandard2.1 \
  -p:DataVoEnablePortableTarget=true
```

Expected: both commands succeed. The default command builds only `net10.0`.

- [ ] **Step 2: Verify final package behavior and documentation gates**

```bash
bash scripts/verify-local-packages.sh
bash scripts/check-doc-api-samples.sh
bash scripts/check-unity-support-status.sh
```

Expected: package validation passes, the consumer app runs from isolated local packages, the Core package contains only `lib/net10.0/DataVo.Core.dll`, and both documentation checks pass.

- [ ] **Step 3: Verify the relevant tests and benchmark host**

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release

dotnet test \
  demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj \
  -c Release

dotnet build \
  demos/Research.Benchmark/src/Research.Benchmark.Host/Research.Benchmark.Host.csproj \
  -c Release
```

Expected: all tests pass and the default benchmark host builds against the modern Core asset.

- [ ] **Step 4: Verify docs without regenerating tracked timestamps again**

```bash
cd docs
node scripts/clean-docs-dist.cjs
npx vitepress build
cd ..
```

Expected: VitePress reports a successful build. This direct build intentionally skips `docs:ai`; the generated export was already refreshed and committed in Task 4.

- [ ] **Step 5: Check the exact exit criteria and working tree**

```bash
if rg -n "netstandard2\.1|matrix\.datavo_core" .github/workflows/benchmark.yml; then
  echo "Portable benchmark lane is still enabled." >&2
  exit 1
fi

if rg -n "DataVoEnablePortableTarget" .github/workflows/publish-nuget.yml; then
  echo "Release workflow opts into the experimental target." >&2
  exit 1
fi

git diff --check
git status --short --branch
git log --oneline -6
```

Expected:

- no unstaged or staged Stage 1 changes remain;
- the only untracked files are the pre-existing user-owned images, Reddit draft, and plot generator;
- the four task commits appear after design commit `2186e31`;
- `master` remains otherwise unchanged.

Do not mark Stage 1 complete if the normal package contains a portable asset, the experimental build cannot be reproduced, a documentation guard fails, or the release workflow can bypass the final-artifact assertion.

## Stage 1 Completion Record

When all checks pass, record these facts in the implementation handoff:

- exact commit IDs for the four Stage 1 commits;
- `dotnet msbuild` default and opt-in target-property output;
- the package entry assertion result;
- the experimental `netstandard2.1` build result and any existing warnings;
- DataVo and research benchmark test totals;
- documentation/VitePress build result;
- confirmation that Stage 2 has not started;
- the known Stage 2 blocker: experimental multi-target package API validation still requires removal of public `System.DateOnly`/`System.TimeOnly` replacements and adoption of the approved `DataVoDate` contract.
