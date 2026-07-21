# Unity Slice B2a: Portable Candidate Fixture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Generate a deterministic, inspectable local UPM fixture from the exact experimental netstandard2.1 DataVo.Core package, ready for the later Unity Editor smoke project.

**Architecture:** The existing UPM adapter assemblies explicitly consume DataVo.Core as a precompiled Unity plugin, not as an imaginary source asmdef. A candidate-preparation command packs the opt-in Core asset in isolated caches, restores a netstandard consumer to resolve every managed dependency, copies the resolved DLL closure into a staged local UPM package, and records hashes, assembly identities, package provenance, and source commit. A small framework-only inspector rejects duplicate assembly identities and duplicate public System types before the fixture becomes visible at its requested output path.

**Tech Stack:** Bash, Node.js JSON parsing, C#/.NET 10 System.Reflection.Metadata, DataVo.Core portable package candidate, Unity Package Manager fixture layout. Unity Editor is intentionally not required for this slice.

## Global Constraints

- This is Slice B2a only. Do not add com.datavo.unity.jobs, Collections/Burst dependencies, a smoke project, Unity metadata, Editor tests, player builds, NuGet publishing, or a Unity support claim.
- Unity 6000.5.3f1 is not installed in this workspace. Do not infer a Collections/Burst/Test Framework version or package lock from internet documentation.
- DataVo.Core remains Unity-free and must be packed only with DataVoEnablePortableTarget=true. The normal public package remains net10.0-only.
- The fixture is generated test material, never public distribution. Its package name is com.datavo.core.candidate and its output directory must be supplied explicitly and must not already exist.
- Candidate preparation must use fresh, isolated NuGet caches. It must not accept a workspace DLL, a project reference, or a package restored from a global cache.
- Fixture content includes only the exact lib/netstandard2.1/DataVo.Core.dll and the managed runtime/compile dependency closure selected by a clean netstandard2.1 consumer restore.
- Preserve the candidate package SHA-256, each copied assembly SHA-256, full assembly identity, resolved package ID/version, source package asset path, current source commit, and fixture schema version in candidate-manifest.json.
- The fixture generator must stage beside the requested output and publish only by a final rename. A pre-existing output path is an error; the generator must not delete or overwrite it.
- Reject duplicate assembly identities and duplicate public top-level System.* types across distinct copied DLLs before publishing the fixture.
- Adapter asmdefs reference the candidate as a Unity precompiled plugin. They must use overrideReferences:true and precompiledReferences:["DataVo.Core.dll"]; DataVo.Core must not appear in their asmdef references arrays.
- DataVo.Unity.Mathematics retains Unity.Mathematics as its only asmdef reference and keeps noEngineReferences:false, because Unity `6000.5.4f1` Mathematics `1.4.0` forwards its value types through `UnityEngine.MathematicsModule`. DataVo.Unity.EngineTypes has no asmdef references and keeps noEngineReferences:false.
- Static or .NET-host checks are not Unity import, Burst, Mono, IL2CPP, AOT, player, or performance evidence.
- Preserve unrelated user-owned untracked files. Use apply_patch for edits and stage exact paths only.

## File Structure

| File | Responsibility |
| --- | --- |
| integrations/unity/com.datavo.unity.mathematics/Runtime/Mathematics/DataVo.Unity.Mathematics.asmdef | Explicit plugin reference to DataVo.Core.dll plus Unity.Mathematics source-assembly reference. |
| integrations/unity/com.datavo.unity.mathematics/Runtime/EngineTypes/DataVo.Unity.EngineTypes.asmdef | Explicit plugin reference to DataVo.Core.dll for UnityEngine adapters. |
| scripts/test-unity-mathematics-package.sh | Encodes the corrected UPM adapter assembly contract. |
| tools/DataVo.UnityCandidateInspector/DataVo.UnityCandidateInspector.csproj | Framework-only executable project for inspecting copied managed DLLs. |
| tools/DataVo.UnityCandidateInspector/Program.cs | Emits assembly inventory and rejects identity/framework-type conflicts. |
| scripts/test-unity-candidate-inspector.sh | Red/green integration contract for the inspector. |
| scripts/resolve-unity-candidate-assets.mjs | Reads a restored netstandard assets file and copies the selected managed dependency closure into the fixture staging directory. |
| scripts/prepare-unity-candidate.sh | Isolated pack, restore, extraction, inventory, manifest, and atomic fixture publication. |
| scripts/test-prepare-unity-candidate.sh | End-to-end fixture-shape, provenance, and non-overwrite regression contract. |
| scripts/xunit-serial.runsettings | Source-controlled xUnit configuration for deterministic full-suite verification. |
| docs/superpowers/plans/2026-07-21-unity-slice-b2a-candidate-fixture.md | Execution record and explicit remaining Unity Editor gates. |

## Task 1: Correct adapter asmdefs for a precompiled candidate DLL

**Files:**
- Modify: scripts/test-unity-mathematics-package.sh
- Modify: integrations/unity/com.datavo.unity.mathematics/Runtime/Mathematics/DataVo.Unity.Mathematics.asmdef
- Modify: integrations/unity/com.datavo.unity.mathematics/Runtime/EngineTypes/DataVo.Unity.EngineTypes.asmdef

**Produces:** Adapter assemblies that use Unity precompiled-plugin references rather than an asmdef reference for a generated DataVo.Core.dll.

- [x] **Step 1: Change the existing metadata contract to the intended plugin shape**

In the Node assertion block, require exactly:

~~~
Mathematics references: ["Unity.Mathematics"]
Mathematics overrideReferences: true
Mathematics precompiledReferences: ["DataVo.Core.dll"]
EngineTypes references: []
EngineTypes overrideReferences: true
EngineTypes precompiledReferences: ["DataVo.Core.dll"]
~~~

Keep the existing names, root namespaces, autoReferenced values, and noEngineReferences assertions.

- [x] **Step 2: Run the targeted metadata contract and observe red**

Run:

~~~bash
bash scripts/test-unity-mathematics-package.sh --metadata
~~~

Expected: failure because the current asmdefs still name DataVo.Core in references and have empty precompiledReferences arrays.

- [x] **Step 3: Change only the two asmdefs to the plugin model**

Set the Mathematics asmdef fields to:

~~~json
"references": ["Unity.Mathematics"],
"overrideReferences": true,
"precompiledReferences": ["DataVo.Core.dll"]
~~~

Set the EngineTypes asmdef fields to:

~~~json
"references": [],
"overrideReferences": true,
"precompiledReferences": ["DataVo.Core.dll"]
~~~

Do not add a DataVo.Core.asmdef beside the generated DLL. A source asmdef with that identity would collide with the precompiled DataVo.Core plugin.

- [x] **Step 4: Verify the corrected adapter contract and portable C# surface**

Run:

~~~bash
bash scripts/test-unity-mathematics-package.sh --all
dotnet build /private/tmp/datavo-unity-adapter-contract/DataVo.Unity.AdapterContract.csproj --no-restore --disable-build-servers -p:DataVoEnablePortableTarget=true
~~~

Expected: the static package contract passes. The disposable adapter C# harness has no adapter diagnostics; it may repeat the known portable Core nullable warnings.

- [x] **Step 5: Commit the corrected plugin boundary**

~~~bash
git add scripts/test-unity-mathematics-package.sh integrations/unity/com.datavo.unity.mathematics/Runtime/Mathematics/DataVo.Unity.Mathematics.asmdef integrations/unity/com.datavo.unity.mathematics/Runtime/EngineTypes/DataVo.Unity.EngineTypes.asmdef
git commit -m "fix: reference DataVo Core as Unity plugin"
~~~

## Task 2: Add a framework-only candidate assembly inspector

**Files:**
- Create: tools/DataVo.UnityCandidateInspector/DataVo.UnityCandidateInspector.csproj
- Create: tools/DataVo.UnityCandidateInspector/Program.cs
- Create: scripts/test-unity-candidate-inspector.sh

**Produces:**

~~~text
dotnet DataVo.UnityCandidateInspector.dll --root fixture-root --output inventory.json Runtime/DataVo.Core.dll Runtime/Dependencies/*.dll
~~~

The JSON output contains a schemaVersion and one ordered record per input DLL:

~~~json
{
  "file": "Runtime/DataVo.Core.dll",
  "sha256": "lowercase hex",
  "identity": "DataVo.Core, Version=...",
  "publicSystemTypes": ["System.Example"]
}
~~~

- [x] **Step 1: Write the failing inspector integration contract**

Create scripts/test-unity-candidate-inspector.sh. It must:

1. require the already-built Debug netstandard2.1 Core artifact;
2. invoke the inspector once for that DLL and require one inventory record with a SHA-256 and DataVo.Core identity;
3. invoke the inspector with the same DLL twice and require a non-zero exit plus the exact phrase Duplicate assembly identity.

Run:

~~~bash
bash scripts/test-unity-candidate-inspector.sh
~~~

Expected: failure because the inspector project does not exist.

- [x] **Step 2: Implement the minimal inspector**

Target net10.0 with no PackageReference. Parse --root, --output, then one or more assembly paths; reject unknown options, missing root/output/input, a root that does not contain an input, and duplicate input path arguments with an argument error.

For each file:

~~~csharp
AssemblyName name = AssemblyName.GetAssemblyName(path);
using FileStream stream = File.OpenRead(path);
using PEReader peReader = new(stream);
MetadataReader metadata = peReader.GetMetadataReader();
~~~

Compute lowercase SHA-256 from file bytes. Enumerate public top-level types whose namespace begins with System. Use TypeAttributes.Public only, not nested types. Before writing JSON, reject a repeated name.FullName across different files and reject any repeated public System type across different files. Write sorted JSON with paths relative to the fixture root supplied by --root.

- [x] **Step 3: Run the inspector contract and verify green**

Run:

~~~bash
dotnet restore tools/DataVo.UnityCandidateInspector/DataVo.UnityCandidateInspector.csproj
bash scripts/test-unity-candidate-inspector.sh
~~~

Expected: normal inventory succeeds and the duplicated DataVo.Core input fails with Duplicate assembly identity.

- [x] **Step 4: Commit the inventory boundary**

~~~bash
git add tools/DataVo.UnityCandidateInspector/DataVo.UnityCandidateInspector.csproj tools/DataVo.UnityCandidateInspector/Program.cs scripts/test-unity-candidate-inspector.sh
git commit -m "feat: inspect Unity candidate assemblies"
~~~

## Task 3: Resolve the exact portable managed dependency closure

**Files:**
- Create: scripts/resolve-unity-candidate-assets.mjs
- Create: scripts/test-prepare-unity-candidate.sh

**Consumes:** A clean netstandard2.1 project's project.assets.json and the matching private NuGet package root.

**Produces:** Copied DLL files beneath Runtime/Dependencies and dependency-assets.json with records:

~~~json
{
  "packageId": "System.Text.Json",
  "packageVersion": "10.0.0",
  "assetPath": "lib/netstandard2.1/System.Text.Json.dll",
  "fixturePath": "Runtime/Dependencies/System.Text.Json.dll"
}
~~~

- [x] **Step 1: Write the failing fixture-shape portion of the preparation test**

Create scripts/test-prepare-unity-candidate.sh before the resolver/generator. It must create a unique temporary output, call scripts/prepare-unity-candidate.sh --output with that path, and require:

- package.json name com.datavo.core.candidate and unity 6000.0;
- Runtime/DataVo.Core.dll, Runtime/Dependencies, candidate-manifest.json, dependency-assets.json, and assembly-inventory.json;
- every dependency-assets fixturePath exists and every manifest assembly SHA-256 matches its copied file;
- exactly one DataVo.Core assembly appears, sourced from lib/netstandard2.1/DataVo.Core.dll;
- output creation fails if the requested path already exists, without modifying its sentinel file.

Run:

~~~bash
bash scripts/test-prepare-unity-candidate.sh
~~~

Expected: failure because the preparation command and resolver do not exist.

- [x] **Step 2: Implement deterministic assets resolution**

The Node command accepts exactly --assets, --packages-root, --runtime-directory, and --output. It selects the target whose name begins .NETStandard,Version=v2.1, unions compile and runtime DLL assets, excludes DataVo.Core, and resolves each asset through libraries[libraryKey].path beneath the supplied package root.

Copy each selected DLL into Runtime/Dependencies using its basename. Reject an absent source DLL or a basename collision before copying. Emit stable, package-ID/version/path-sorted dependency-assets.json using only forward-slash fixture paths.

- [x] **Step 3: Verify the resolver through the preparation contract**

Run:

~~~bash
bash scripts/test-prepare-unity-candidate.sh
~~~

Expected: it still fails only because scripts/prepare-unity-candidate.sh is absent; direct resolver behavior is exercised by the generator after Task 4.

## Task 4: Generate and atomically publish the UPM candidate fixture

**Files:**
- Create: scripts/prepare-unity-candidate.sh
- Modify: scripts/test-prepare-unity-candidate.sh

**CLI:**

~~~text
scripts/prepare-unity-candidate.sh --output /absolute/or/relative/new-directory
~~~

**Generated layout:**

~~~text
<output>/
  package.json
  README.md
  candidate-manifest.json
  dependency-assets.json
  assembly-inventory.json
  Runtime/
    DataVo.Core.dll
    Dependencies/
      <resolved managed DLLs>
~~~

- [x] **Step 1: Implement isolated pack and consumer restore**

Require one --output value and reject any existing output. Create an adjacent staging directory and isolated pack, restore, HTTP, MSBuild-extension, and DOTNET_CLI_HOME paths under mktemp. Pack DataVo.Core Release with:

~~~bash
-p:DataVoEnablePortableTarget=true
-p:EnablePackageValidation=true
-p:PackageVersion=0.0.0-unitycandidate.<git-short-sha>
~~~

Require exactly lib/net10.0/DataVo.Core.dll and lib/netstandard2.1/DataVo.Core.dll in the nupkg. Extract the latter to Runtime/DataVo.Core.dll.

Restore tools/DataVo.PortableConsumer targeting the generated package with only the isolated local source plus NuGet.org. Require its assets file to select lib/netstandard2.1/DataVo.Core.dll and confirm the consumer cache nupkg byte-matches the just-packed candidate.

- [x] **Step 2: Resolve, inspect, and write provenance files**

Call resolve-unity-candidate-assets.mjs with the consumer assets file and isolated cache. Run DataVo.UnityCandidateInspector with --root set to the staging directory and --output assembly-inventory.json over DataVo.Core.dll plus every copied dependency DLL.

Write package.json:

~~~json
{
  "name": "com.datavo.core.candidate",
  "displayName": "DataVo Core Candidate (Generated)",
  "version": "0.0.0-preview.1",
  "unity": "6000.0",
  "description": "Generated test-only fixture from the exact experimental DataVo.Core portable package."
}
~~~

Write README.md that names this output generated/test-only, instructs callers not to commit it, and states that it proves no Unity support.

Write candidate-manifest.json with schemaVersion 1, sourceCommit, candidate package ID/version/SHA-256, netstandard asset path, relative fixture assembly paths, dependency-assets filename, and assembly-inventory filename. Use Node JSON serialization with sorted keys/records so repeated runs from the same commit have deterministic content apart from the candidate package version only.

- [x] **Step 3: Publish only after all validation succeeds**

After all required files exist and the inspector succeeds, rename the complete staging directory to --output. On a failure, leave the requested path absent and clean only owned temporary/staging paths.

- [x] **Step 4: Run the end-to-end candidate preparation contract**

Run:

~~~bash
bash scripts/test-prepare-unity-candidate.sh
bash scripts/verify-portable-candidate.sh
bash scripts/check-unity-support-status.sh
~~~

Expected: fixture shape, hashes, dependency closure, duplicate protections, existing portable smoke, and support disclaimers all pass. Network access may be required solely for the explicitly isolated fresh NuGet restores.

- [x] **Step 5: Commit the generated-fixture preparation tooling**

~~~bash
git add scripts/resolve-unity-candidate-assets.mjs scripts/prepare-unity-candidate.sh scripts/test-prepare-unity-candidate.sh
git commit -m "feat: prepare Unity portable candidate fixture"
~~~

## Task 5: Final verification and evidence boundary

**Files:**
- Create: scripts/xunit-serial.runsettings
- Modify: docs/superpowers/plans/2026-07-21-unity-slice-b2a-candidate-fixture.md

- [x] **Step 1: Run source and portable verification**

Run:

~~~bash
bash scripts/test-unity-mathematics-package.sh --all
bash scripts/test-unity-candidate-inspector.sh
bash scripts/test-prepare-unity-candidate.sh
dotnet build DataVo.Core/DataVo.Core.csproj --no-restore -p:DataVoEnablePortableTarget=true
bash scripts/verify-portable-candidate.sh
bash scripts/check-unity-support-status.sh
git diff --check HEAD
~~~

Expected: all commands exit zero. Existing TopKReactiveQuery nullable warnings may appear in portable Core builds but no errors/new warnings are accepted.

- [x] **Step 2: Add a portable serial test configuration and run the full test suite**

Create scripts/xunit-serial.runsettings:

~~~xml
<?xml version="1.0" encoding="utf-8"?>
<RunSettings>
  <RunConfiguration><MaxCpuCount>1</MaxCpuCount></RunConfiguration>
  <xUnit>
    <ParallelizeAssembly>false</ParallelizeAssembly>
    <ParallelizeTestCollections>false</ParallelizeTestCollections>
    <MaxParallelThreads>1</MaxParallelThreads>
  </xUnit>
</RunSettings>
~~~

Run:

~~~bash
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-build --no-restore --settings scripts/xunit-serial.runsettings --logger "trx;LogFileName=unity-slice-b2a-serial.trx" --results-directory /private/tmp/datavo-unity-b2a-tests
~~~

Expected TRX counters: 1,418 total, 1,417 executed/passed, 0 failed, 1 intentional Stage2B skip.

- [x] **Step 3: Record what remains blocked**

Mark this plan complete only for deterministic candidate preparation. Record that pinned Unity Editor import, derived packages-lock.json, Jobs/Collections, Burst, the smoke project, Windows x64 IL2CPP execution, and NuGet derivation from Unity-compiled adapter DLLs remain unexecuted Slice B2b/B2c work.

- [x] **Step 4: Commit the verified plan**

~~~bash
git add scripts/xunit-serial.runsettings
git add -f docs/superpowers/plans/2026-07-21-unity-slice-b2a-candidate-fixture.md
git commit -m "docs: record Unity candidate fixture verification"
~~~

## Implementation Record

Completed 2026-07-21.

- Corrected the UPM adapter assembly definitions so `DataVo.Core.dll` is consumed as a precompiled Unity plugin. `DataVo.Core` is no longer an asmdef reference, avoiding a source-assembly identity collision with the generated candidate DLL.
- Added a framework-only managed-assembly inspector. It records identities, SHA-256 hashes, and public top-level `System.*` types, rejecting duplicate identities and duplicate framework types before fixture publication.
- Added an atomic candidate generator. It packs the opt-in portable package in isolated NuGet caches, restores a clean netstandard2.1 consumer, extracts the exact Core asset plus its resolved managed dependency closure, inventories it, and writes provenance manifests before the final rename.
- The resolver verification is exercised through the successful end-to-end preparation contract after the generator exists; it copies only the assets selected by the fresh consumer restore.
- Added the checked-in serial xUnit configuration. The full run produced 1,418 total tests: 1,417 passed, 0 failed, and 1 intentional Stage2B performance-probe skip.

Verification completed successfully:

~~~bash
bash scripts/test-unity-mathematics-package.sh --all
bash scripts/test-unity-candidate-inspector.sh
bash scripts/test-prepare-unity-candidate.sh
dotnet build DataVo.Core/DataVo.Core.csproj --no-restore --disable-build-servers -p:DataVoEnablePortableTarget=true
bash scripts/verify-portable-candidate.sh
bash scripts/check-unity-support-status.sh
dotnet test DataVo.Tests/DataVo.Tests.csproj --no-build --no-restore --settings scripts/xunit-serial.runsettings --logger "trx;LogFileName=unity-slice-b2a-serial.trx" --results-directory /private/tmp/datavo-unity-b2a-tests
~~~

The portable Core build still reports the pre-existing four `TopKReactiveQuery` nullable warnings (`CS8602`), with no errors. No Unity Editor is installed locally, and no Unity import, package lock derivation, Jobs/Collections implementation, Burst, Mono, IL2CPP, player build, Windows x64 test, or NuGet package derived from Unity-compiled adapter DLLs has been performed. Those remain explicit Slice B2b/B2c work, gated on a clean project using the pinned Unity 6000.5.3f1 Editor.
