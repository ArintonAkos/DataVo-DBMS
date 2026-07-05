# Entity Framework Example

> Source route: /manual/tutorial/entity-framework-example
> Source file: manual/tutorial/entity-framework-example.md

DataVo's Entity Framework Core integration is an alpha bridge. It lets you keep a familiar EF model while evaluating DataVo-backed persistence and DataVo-native query paths with explicit operator limits. It is not a complete EF provider in v0.1.

Start with a mapped entity. Keep the first model simple: an integer key, a string column, and a boolean flag.

```csharp
public sealed class User
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public bool IsActive { get; set; }
}
```

Create a context that derives from `DataVoDbContext`. The example uses EF Core's in-memory provider as the host provider and then attaches DataVo with `UseDataVo`.

```csharp
using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

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

Build the options once and pass them into the context.

```csharp
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseInMemoryDatabase("datavo-ef-host")
    .UseDataVo(dataVo => dataVo
        .UseInMemoryStorage()
        .WithDataSource("EfDemo"))
    .Options;
```

Insert through EF with `Add` and `SaveChanges`.

```csharp
using var writer = new AppDbContext(options);

writer.Database.EnsureCreated();

writer.Users.Add(new User
{
    Id = 1,
    Name = "Ada",
    IsActive = true
});

writer.SaveChanges();
```

Query with ordinary LINQ for supported LINQ operators.

```csharp
List<User> activeUsers = writer.Users
    .Where(static user => user.IsActive)
    .OrderBy(static user => user.Name)
    .ToList();
```

If a separate context needs to read data that already exists in DataVo, call `LoadFromDataVo` before querying.

```csharp
using var reader = new AppDbContext(options);

reader.LoadFromDataVo();

List<User> activeUsers = reader.Users
    .Where(static user => user.IsActive)
    .ToList();
```

For DataVo-native query evaluation, use `QueryFromDataVo`. This path validates the LINQ expression and keeps unsupported operators explicit.

```csharp
List<User> firstTen = reader.QueryFromDataVo<User>(query => query
    .Where(static user => user.IsActive)
    .OrderBy(static user => user.Id)
    .Take(10));
```

Vector queries use DataVo's EF function shims inside a supported DataVo-native query.

```csharp
float[] queryVector = [1f, 0f, 0f];

List<ItemEmbedding> nearest = ctx.QueryFromDataVo<ItemEmbedding>(query => query
    .Where(item => DataVoVectorDbFunctions.CosineDistance(EF.Functions, item.Vector, queryVector) < 0.3)
    .OrderBy(item => DataVoVectorDbFunctions.CosineDistance(EF.Functions, item.Vector, queryVector))
    .Take(5));
```

## EF Core Example Support

The bridge supports basic mapped `DbSet` querying, basic inserts via `Add` plus `SaveChanges`, `LoadFromDataVo` (refresh EF state before LINQ), `QueryFromDataVo` for supported LINQ query patterns, and vector function shims (`CosineDistance`, `L2Distance`) for preview translation. It does **not** support EF migrations, shadow properties, or complex provider-style LINQ joins in v0.1.
