# Native AOT

DataVo is designed with Native AOT constraints in mind, but the v0.1 claim is specific. The core direction is C#-native, source-generated, and less dependent on reflection-heavy runtime discovery. That does not mean every integration surface is fully AOT-ready.

For dynamic SQL, DataVo still has to parse a runtime string.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("SELECT Id, Name FROM Users WHERE Id = 1");
```

For queries called often enough that parse and mapping overhead matters, prefer source-generated queries. The generator turns a supported SQL pattern into generated C# code that can avoid repeated parse and mapper setup.

```csharp
using DataVo.Core;
using DataVo.Core.CompiledQueries;

public sealed record PlayerRow(int Id, string Name, int Level);

public static partial class PlayerQueries
{
    [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
    public static partial PlayerRow? GetPlayer(DataVoContext db, int id);
}
```

AOT-sensitive applications should validate the final published application with the .NET toolchain rather than relying on a documentation claim.

```bash
dotnet publish -c Release -r osx-arm64 /p:PublishAot=true
```

EF Core has its own AOT and trimming constraints. Treat DataVo's EF bridge separately from the core engine and test the complete application.

```csharp
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseInMemoryDatabase("datavo-ef-host")
    .UseDataVo(dataVo => dataVo.UseInMemoryStorage().WithDataSource("EfDemo"))
    .Options;
```

## Native AOT Support Summary

| Feature | Status | Notes |
| --- | --- | --- |
| C#-native embedded core | Supported | Runs in-process without a native provider boundary. |
| Source-generated query paths | Supported | `[DataVoQuery]` emits static code for supported shapes. |
| Typed row readers | Supported | Avoid dictionary and boxing overhead on eligible projections. |
| Source-generated AOT-sensitive paths | Supported | Generated queries and typed readers are the supported AOT-oriented paths in v0.1. |
| Reflection-free catalog/runtime everywhere | Planned | Some runtime areas still need additional trimming hardening. |
| Full EF Core AOT claim | Not Supported | EF integration has separate provider and reflection considerations. |
| Full dynamic SQL AOT guarantee | Not Supported | Runtime SQL parsing and dynamic result shapes remain more dynamic than generated calls. |
