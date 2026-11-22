# Entity Framework Core

> Source route: /manual/client-interfaces/entity-framework
> Source file: manual/client-interfaces/entity-framework.md

::: warning Experimental Preview — Excluded from Native AOT
`DataVo.EntityFrameworkCore` is an **experimental integration bridge**, not a standalone migration-capable EF Core provider. It relies on a host EF provider (such as `UseInMemoryDatabase`) for EF infrastructure services and is **excluded from DataVo's Native AOT guarantee**. For production or AOT workloads, use [`DataVoContext`](/manual/client-interfaces/datavo-context-api), [Compiled LINQ](/manual/client-interfaces/compiled-linq), or [ADO.NET / Dapper](/manual/client-interfaces/ado-net).
:::

`DataVo.EntityFrameworkCore` lets EF Core users evaluate DataVo-backed persistence and DataVo-native queries while keeping familiar `DbContext` and `DbSet<T>` models.

```csharp
using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

var options = new DbContextOptionsBuilder<GameDbContext>()
    .UseInMemoryDatabase("datavo-ef-host")
    .UseDataVo(dataVo => dataVo
        .UseInMemoryStorage()
        .WithDataSource("game"))
    .Options;

using var ctx = new GameDbContext(options);
ctx.EnsureDataVoCreated();

ctx.Players.Add(new Player { Id = 1, Name = "Ada", Score = 1200, Region = "EU" });
ctx.SaveChanges();

List<Player> top = ctx.QueryFromDataVo<Player>(q => q
    .Where(static p => p.Score >= 1000)
    .OrderByDescending(static p => p.Score)
    .Take(10));
```

## Configuring `DataVoDbContext`

Derive from `DataVoDbContext` to access `LoadFromDataVo`, `QueryFromDataVo`, `QueryFromDataVoAsync`, and `ExplainQueryFromDataVo`:

```csharp
public sealed class GameDbContext : DataVoDbContext
{
    public GameDbContext(DbContextOptions<GameDbContext> options) : base(options) { }

    public DbSet<Player> Players => Set<Player>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Player>(entity =>
        {
            entity.ToTable("players");
            entity.HasKey(static p => p.Id);
            entity.Property(static p => p.Name).HasMaxLength(80);
        });
    }
}
```

- **`UseDataVo(...)`** attaches DataVo storage options (or a DataVo connection string) to `DbContextOptionsBuilder`.
- **`EnsureDataVoCreated()`** initializes the mapped tables in DataVo (available on both `DbContext` and `DatabaseFacade`).
- **`LoadFromDataVo()`** synchronizes EF's change tracker from DataVo when rows were written by another context or directly through `DataVoContext`.
- **`ExplainQueryFromDataVo<T>(...)`** inspects whether a LINQ expression fits the supported DataVo-native translation subset before running it.

## Vector Query Preview Shims

Inside `QueryFromDataVo`, `DataVoVectorDbFunctions` exposes preview shims (`CosineDistance` and `L2Distance`) that translate to DataVo's `<=>` and `<->` SQL vector operators:

```csharp
float[] queryVector = [1f, 0f, 0f];

List<Document> nearest = ctx.QueryFromDataVo<Document>(q => q
    .Where(d => DataVoVectorDbFunctions.CosineDistance(EF.Functions, d.Embedding, queryVector) < 0.3)
    .OrderBy(d => DataVoVectorDbFunctions.CosineDistance(EF.Functions, d.Embedding, queryVector))
    .Take(5));
```

Note that `COSINE_DISTANCE()` and `L2_DISTANCE()` exist only as EF Core preview shims, not as core DataVo SQL scalar functions.

## Current Limitations

| Area | Status |
| --- | --- |
| `DbSet` CRUD (`Add`, `SaveChanges`, `LoadFromDataVo`) | Supported for mapped scalar CLR properties |
| `QueryFromDataVo` / `QueryFromDataVoAsync` | Supported for bounded `Where`, `OrderBy`/`ThenBy`, `Skip`, `Take`, `Select` |
| Vector distance shims (`CosineDistance`, `L2Distance`) | Preview translation inside `QueryFromDataVo` |
| EF Migrations (`dotnet ef migrations`), shadow properties, complex joins, `GroupBy` | **Not supported** (unsupported patterns throw `DataVoEfException`) |
| Native AOT (`PublishAot=true`) | **Not supported** |

## See Also

- [Compiled LINQ & Spatial Queries](/manual/client-interfaces/compiled-linq) — AOT-safe, zero-reflection LINQ alternative
- [Error Handling](/manual/reference/error-handling) — `DataVoEfException` and engine error codes
- [Native AOT Deployment](/manual/client-interfaces/native-aot) — Why `DataVo.EntityFrameworkCore` is excluded from AOT
