# Native AOT Deployment

> Source route: /manual/client-interfaces/native-aot
> Source file: manual/client-interfaces/native-aot.md

`DataVo.Core`, `DataVo.Data`, `DataVo.VectorData`, and `DataVo.Generators` are built and tested for Native AOT on `net8.0`, `net9.0` and `net10.0`. Trim and AOT analyzers are enabled across the core, ADO.NET and VectorData assemblies with `IL2026` and `IL3050` treated as build errors and verified on every change; the `DataVo.AotSmoke` native executable exercises all three in CI.

```xml
<PropertyGroup>
  <TargetFramework>net10.0</TargetFramework>
  <PublishAot>true</PublishAot>
  <InvariantGlobalization>true</InvariantGlobalization>
</PropertyGroup>
```

```bash
dotnet publish -c Release -r osx-arm64
```

## Package AOT Compatibility

| Package / Feature | Native AOT Status | Notes |
| --- | --- | --- |
| `DataVo.Core` | **Verified** | Runtime SQL parser/executor, LSM/InMemory storage, `StreamQuery`, `StreamRef`, `Subscribe`, and `SubscribeZeroAlloc` use zero reflection. |
| `DataVo.Data` | **Verified** | `DataVoConnection`, `DataVoCommand`, `DataVoDataReader`, and `DataVoTransaction` are trim- and AOT-clean. |
| `DataVo.VectorData` | **Verified** | `DataVoVectorStore`, collections, and LINQ filter translation are trim- and AOT-clean; construct collections directly in AOT apps, because `VectorStore.GetCollection` carries the abstraction's own `[RequiresDynamicCode]` annotation. See [VectorData](/manual/client-interfaces/vector-data). |
| `DataVo.Generators` | **Verified** | `[DataVoTable]`, `[DataVoQuery]`, compiled LINQ interceptors, and `.SubscribeInto()` emit static C# at build time. |
| `DataVo.EntityFrameworkCore` | **Excluded** | Depends on EF Core's runtime infrastructure and is not covered by the Native AOT guarantee. |

## Writing AOT-Safe DataVo Code

- **Use `DataVo.Generators` for typed queries:** Prefer `[DataVoTable]` compiled LINQ and `[DataVoQuery]` over reflection-based mappers.
- **Avoid reflection-based micro-ORMs unless AOT-enabled:** `DataVo.Data` itself is AOT-clean, but third-party libraries layered on top (such as unconfigured Dapper reflection paths) govern their own AOT compatibility.
- **Use zero-allocation streaming on hot paths:** `StreamRef()` and `SubscribeZeroAlloc()` operate on `ReadOnlySpan<byte>` and `RowRef` structs with zero reflection and zero managed heap allocations.
- **Keep `DataVoContext.Insert<T>` types preserved:** `Insert<T>` carries `[DynamicallyAccessedMembers]` annotations so the trimmer preserves public properties and parameterless constructors, or use source-generated `db.Insert(player)` for zero-reflection writes.

## Verification

The repository validates AOT cleanliness with `scripts/check-aot-baseline.sh`, which publishes an AOT smoke application exercising `DataVo.Core`, `DataVo.Data`, and `DataVo.Generators` under `PublishAot=true`.

## See Also

- [Roslyn Source Generators](/manual/client-interfaces/source-generators): Compile-time helpers and `[DataVoQuery]`
- [Compiled LINQ & Spatial Queries](/manual/client-interfaces/compiled-linq): Zero-reflection LINQ interceptors
- [Zero-Allocation Rows](/manual/internals/zero-allocation-rows): `RowRef` and `StreamRef` internals
