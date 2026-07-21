# Unity Portable Dependency Closure Design

## Status

Approved in conversation on 2026-07-21, pending review of this written
specification. This is a narrow follow-up to the macOS IL2CPP Player smoke
slice. It repairs the generated Unity candidate's framework dependency closure;
it does not introduce Unity references into `DataVo.Core`, change the normal
`net10.0` package, or claim general Unity support.

## Evidence and problem

The actual macOS IL2CPP Player build reached Unity's managed stripping step with
the installed `6000.5.4f1` (`d550df8bd089`) macOS IL2CPP module. It failed in
Unity Linker while writing `System.Text.Json.dll`:

```text
IL1011: Failed to write System.Text.Json.dll
Member 'System.Runtime.CompilerServices.IsExternalInit' is declared in another
module and needs to be imported.
```

The exact generated portable candidate carried `System.Text.Json` assembly
version `10.0.0.0`. The pinned Unity runtime already ships
`System.Text.Json` `8.0.0.0` in its `netstandard2.1` BCL extensions. Unity
therefore saw two framework implementations with the same simple name but
incompatible identities during linking. This occurs before the Player smoke
scenario starts, so it is not a Unity value-binding, mapper-reflection, or
database-operation failure.

The installed Unity BCL also supplies these exact framework identities:

| Assembly | Unity identity version |
| --- | --- |
| `System.Text.Json` | `8.0.0.0` |
| `System.Text.Encodings.Web` | `8.0.0.0` |
| `Microsoft.Bcl.AsyncInterfaces` | `8.0.0.0` |
| `System.Runtime.CompilerServices.Unsafe` | `6.0.0.0` |

It does not supply `System.Threading.Channels`, `System.IO.Pipelines`, or
`System.ComponentModel.Annotations` in that BCL-extension directory. Those
remain candidate dependencies unless a later exact runtime inventory proves
otherwise.

## Decision

The experimental `netstandard2.1` target references `System.Text.Json` `8.0.0`
instead of `10.0.0`; the normal `net10.0` target remains BCL-owned and
unchanged. The generated Unity candidate treats the pinned Unity BCL as a
declared platform provider, not as an accidental second search directory.

For every resolved NuGet runtime assembly, candidate preparation applies one of
three explicit outcomes:

1. **Copied dependency:** Unity does not declare the assembly's simple name as
   platform-provided; the exact NuGet asset is copied to
   `Runtime/Dependencies` as today.
2. **Unity-provided dependency:** Unity declares the simple name and the NuGet
   assembly's full identity exactly equals the recorded Unity identity; the
   NuGet copy is omitted and the manifest records that Unity owns it.
3. **Conflict:** Unity declares the simple name but full identity differs; the
   candidate preparation fails before Unity import with both identities,
   package provenance, and the Unity runtime path.

No fallback chooses an assembly based on a simple filename, no `link.xml` is
added, and stripping remains Medium. A future Unity upgrade must refresh the
runtime inventory deliberately; it must not silently reuse this pin's contract.

## Alternatives considered

### Disable stripping or preserve every type

This could bypass the Linker path but leaves duplicate framework assemblies and
does not prove an AOT-safe closure. Rejected.

### Keep `System.Text.Json` 10 and copy it beside Unity's 8

This caused the observed linker failure. Rejected.

### Split a smaller Unity-only database runtime

This is an architectural product decision with a much larger API and behavior
surface. The evidence currently identifies a deterministic package closure
mistake, so a runtime split is premature. Rejected for this slice.

### Exact runtime-provider contract (chosen)

Compile the experimental target against the JSON API Unity actually provides,
then generate a fixture with one authoritative provider for each framework
assembly. Exact identity comparison makes the candidate repeatable and catches
future incompatibilities before the Editor/IL2CPP stages.

## Architecture

### Core target dependency boundary

`DataVo.Core.csproj` changes only its `netstandard2.1` package reference:

```xml
<PackageReference Include="System.Text.Json" Version="8.0.0" />
```

`System.Threading.Channels` and `System.ComponentModel.Annotations` retain
their existing references initially. Their final closure is decided by actual
identity inspection, not an assumed Unity version. Any use of a JSON API absent
from version 8 is a compile failure that blocks this slice; no conditional
wrapper or behavior downgrade is added merely to preserve source compatibility.

`net10.0` does not reference this PackageReference group, so its BCL version,
performance path, AOT settings, and public API do not change.

### Pinned Unity runtime inventory

A tracked JSON manifest belongs beside the Unity smoke project. It records:

- schema version;
- pinned Editor version and revision;
- each Unity-provided managed assembly's relative runtime path;
- full assembly identity (name, version, culture, public key token); and
- SHA-256 of the Unity file.

The initial manifest contains the four assemblies in the evidence table above.
It is not a claim that every Unity version supplies them. A verification script
compares the manifest to the local `6000.5.4f1` Editor before an Editor or Player
run, rejecting a missing, modified, or different-identity assembly.

### Candidate resolver

The generated candidate resolver reads NuGet's netstandard dependency closure
and the tracked Unity runtime inventory. The existing managed-metadata inspector
gains a small machine-readable identity mode so resolver code does not parse
assembly metadata with filename heuristics.

The resolver records every outcome in `dependency-assets.json`:

```json
{
  "packageId": "System.Text.Json",
  "packageVersion": "8.0.0",
  "assetPath": "lib/netstandard2.0/System.Text.Json.dll",
  "resolution": "unity-provided",
  "unityRuntimePath": "Resources/Scripting/BCLExtensions/runtime/netstandard2.1/System.Text.Json.dll",
  "identity": "System.Text.Json, Version=8.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51"
}
```

Copied records retain their fixture path. Conflicting records stop generation;
they never enter the UPM fixture. The candidate assembly inventory includes only
the copied assemblies, while its manifest links to the Unity inventory used for
the omitted ones. This preserves provenance without pretending Unity-owned DLLs
were bundled by DataVo.

### Player smoke integration

The Editor and Player runner both verify the runtime inventory before candidate
preparation. The existing `UseUnityTypes()` Player scenario is unchanged: once
the closure passes, it remains the proof for automatic model insert/map/read,
all initial Unity vector types, exact vector dimensions, JSON result creation,
and successful process exit.

The runner's fixed Editor-root discovery is kept: it uses
`EditorApplication.applicationContentsPath`, not the ambiguous
`EditorApplication.applicationPath`. Generated Player scene and temporary
output remain ignored. Player settings are restored after build.

## Errors

- A JSON 8 source/API incompatibility fails the portable Core compile and is
  reported as a dependency-boundary failure.
- A missing or altered pinned Unity assembly fails the inventory check before
  Unity package import.
- A same-simple-name but different-full-identity NuGet dependency fails
  candidate preparation before Editor/IL2CPP execution.
- Unity Linker/AOT failure after closure validation remains a real Player-gate
  failure; this slice does not lower stripping or add preservation metadata.

## Testing and evidence

1. Core's portable build and focused JSON persistence tests compile and pass
   with the JSON 8 reference; the normal `net10.0` test/build remains unchanged.
2. Resolver tests cover copied, exact Unity-provided, and conflicting identity
   outcomes using synthetic assembly inventory fixtures. They assert no
   Unity-provided assembly is copied into `Runtime/Dependencies`.
3. The local Unity runtime inventory checker passes for `6000.5.4f1` revision
   `d550df8bd089` and fails on a changed hash or identity fixture.
4. Candidate preparation passes with an auditable dependency manifest; portable
   .NET-host consumer tests still resolve their own NuGet dependencies and do
   not rely on Unity BCL assemblies.
5. The existing Unity EditMode suite passes after the candidate closure change.
6. The actual macOS IL2CPP Medium-stripping Player builds, launches, writes a
   successful schema-version-one report, and exits zero. Only then may the
   documentation record narrow macOS Player evidence.

## Non-goals

- Changing the `net10.0` target or its System.Text.Json implementation.
- Adding Unity references, `link.xml`, stripping exemptions, or source-level
  platform conditionals throughout DataVo algorithms.
- Treating filename equality as assembly compatibility.
- Windows, Burst, Jobs, persistent save-data, benchmark, or production support
  claims.
- A separate Unity database engine.
