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

The AOT-oriented supported paths in v0.1 are the C#-native embedded core (no native provider boundary), source-generated query paths (`[DataVoQuery]`), and typed row readers. A fully reflection-free catalog and runtime everywhere is planned — some runtime areas still need trimming hardening. A full EF Core AOT claim and a full dynamic-SQL AOT guarantee are **not** supported, because EF integration and runtime SQL parsing remain more dynamic than generated calls.
