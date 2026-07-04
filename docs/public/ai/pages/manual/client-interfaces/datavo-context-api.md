# Connecting to DataVo

> Source route: /manual/client-interfaces/datavo-context-api
> Source file: manual/client-interfaces/datavo-context-api.md

DataVo is embedded in your .NET process. There is no server to start, no socket to open, and no connection pool to configure for the core API. You create a `DataVoContext`, choose a storage mode, and execute SQL directly against the engine.

The fastest way to prove the engine is working is to use in-memory storage. This keeps the example self-contained and avoids creating files while you learn the API.

```csharp
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("CREATE DATABASE App");
db.Execute("USE App");
```

Once a database is selected, `Execute` accepts DataVo SQL. The method returns a `List<QueryResult>` because a SQL string can contain more than one statement. Each `QueryResult` contains fields, row dictionaries, messages, and an `IsError` flag.

```csharp
db.Execute("""
CREATE TABLE Users (
  Id INT PRIMARY KEY,
  Name VARCHAR(80),
  IsActive BIT
);
""");

db.Execute("INSERT INTO Users (Id, Name, IsActive) VALUES (1, 'Ada', true)");

List<QueryResult> results = db.Execute("""
SELECT Id, Name, IsActive
FROM Users
WHERE IsActive = true;
""");

QueryResult result = results[0];

foreach (var row in result.Data)
{
    Console.WriteLine($"{row["Id"]}: {row["Name"]}");
}
```

For persisted local data, switch the same code to disk or LSM storage. Disk mode is the simpler file-backed mode. LSM mode is the high-throughput storage engine used in the headline benchmark results.

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});

db.Execute("CREATE DATABASE App");
db.Execute("USE App");
```

`LsmStrictFsync = true` is the conservative durability setting. It waits for the LSM write-ahead log to reach stable storage before acknowledging writes. Use relaxed LSM only for caches, rebuildable data, or benchmark runs where recent-write loss is acceptable.

## Using Entity Framework Core

DataVo also includes an early Entity Framework Core bridge. In v0.1 Alpha, this is not a full standalone EF provider. It attaches DataVo behavior to an EF context while a host EF provider supplies EF's normal infrastructure services.

The example below uses EF Core's in-memory provider as that host provider, then registers DataVo with `UseDataVo`. The context derives from `DataVoDbContext`, which adds DataVo-specific loading helpers and query validation.

```csharp
using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

public sealed class User
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public bool IsActive { get; set; }
}

public sealed class AppDbContext : DataVoDbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options)
        : base(options)
    {
    }

    public DbSet<User> Users => Set<User>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>(entity =>
        {
            entity.ToTable("Users");
            entity.HasKey(static user => user.Id);
            entity.Property(static user => user.Name).HasMaxLength(80);
            entity.Property(static user => user.IsActive);
        });
    }
}

```

Register the context using normal EF Core options, then add `UseDataVo`.

```csharp
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseInMemoryDatabase("datavo-ef-host")
    .UseDataVo(dataVo => dataVo
        .UseInMemoryStorage()
        .WithDataSource("App"))
    .Options;
```

You can then write through EF and query with ordinary LINQ for supported LINQ operators.

```csharp
using var ctx = new AppDbContext(options);

ctx.Database.EnsureCreated();

ctx.Users.Add(new User
{
    Id = 1,
    Name = "Ada",
    IsActive = true
});

ctx.SaveChanges();

List<User> activeUsers = ctx.Users
    .Where(static user => user.IsActive)
    .OrderBy(static user => user.Name)
    .ToList();

foreach (User user in activeUsers)
{
    Console.WriteLine(user.Name);
}
```

If data was written by another context session or directly through `DataVoContext`, call `LoadFromDataVo` before running EF LINQ so EF's change tracker mirrors the current DataVo tables.

```csharp
using var reader = new AppDbContext(options);

reader.LoadFromDataVo();

List<User> activeUsers = reader.Users
    .Where(static user => user.IsActive)
    .ToList();
```

For DataVo-native query evaluation, use `QueryFromDataVo`. This API validates the LINQ expression and either executes the supported subset or reports a clear unsupported-pattern error.

```csharp
List<User> firstTenActiveUsers = reader.QueryFromDataVo<User>(query => query
    .Where(static user => user.IsActive)
    .OrderBy(static user => user.Id)
    .Take(10));
```

## Entity Framework Core Support

| Feature | Status | Notes |
| --- | --- | --- |
| Basic `DbSet` querying | Supported | Simple LINQ over mapped entities works after data is written in the current context or loaded with `LoadFromDataVo`. |
| Basic inserts | Supported | `Add` plus `SaveChanges` is supported for simple mapped entities in the bridge workflow. |
| `QueryFromDataVo` queries | Supported | Useful for bounded `Where`, `OrderBy`, `Skip`, `Take`, and `Select` projections over mapped scalar properties. |
| Vector LINQ shims | Supported | `DataVoVectorDbFunctions.CosineDistance` and `L2Distance` are available for DataVo-native translation preview. |
| Native provider identity | Planned | Preview flags exist, but DataVo is not yet a mature standalone EF provider. |
| Migrations | Not Supported | v0.1 does not include EF migration scaffolding, diffing, or apply support. |
| Shadow Properties | Not Supported | The current bridge expects explicit mapped CLR properties. |
| Complex Joins | Not Supported | Arbitrary LINQ `Join`, `GroupJoin`, and complex relationship shaping are outside the v0.1 support claim. |
| Broad `GroupBy` translation | Not Supported | Unsupported grouped operators report unsupported-pattern errors. |
