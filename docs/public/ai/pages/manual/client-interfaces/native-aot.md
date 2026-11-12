# Native AOT

> Source route: /manual/client-interfaces/native-aot
> Source file: manual/client-interfaces/native-aot.md

DataVo v0.2 Release Candidate (`v0.2.0-rc.3`) locks Native AOT compatibility across `DataVo.Core` and `DataVo.Data`. Both assemblies enable .NET trim and AOT analyzers and treat `IL2026` and `IL3050` diagnostics as build errors.

For dynamic SQL, DataVo parses the runtime SQL string without reflection-based type discovery.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("SELECT Id, Name FROM Users WHERE Id = 1");
```

For queries called often enough that parse and mapping overhead matters, prefer source-generated queries (`DataVo.Generators`). The generator turns a supported SQL pattern into generated C# code that avoids repeated parse and mapper setup while remaining completely trim- and AOT-safe.

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

AOT-targeted applications can publish directly with `PublishAot=true`:

```bash
dotnet publish -c Release -r osx-arm64 /p:PublishAot=true
```

EF Core has its own upstream AOT and trimming constraints. Treat DataVo's `DataVo.EntityFrameworkCore` bridge separately from `DataVo.Core` and `DataVo.Data` when publishing Native AOT binaries.

```csharp
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseInMemoryDatabase("datavo-ef-host")
    .UseDataVo(dataVo => dataVo.UseInMemoryStorage().WithDataSource("EfDemo"))
    .Options;
```

## Native AOT Support Summary

In v0.2.0-rc.3, `DataVo.Core` and `DataVo.Data` are locked and passing under Native AOT (`PublishAot=true`), including the C#-native embedded core, ADO.NET provider (`DataVoConnection`, `DataVoCommand`, `DataVoDataReader`), source-generated query paths (`[DataVoQuery]`), and zero-allocation `RowRef` readers. `DataVo.EntityFrameworkCore` depends on EF Core's runtime infrastructure and is **not** covered by the Native AOT guarantee.
