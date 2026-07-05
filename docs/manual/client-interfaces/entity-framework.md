# Entity Framework Support

DataVo's Entity Framework Core integration is an alpha bridge. It helps EF users evaluate DataVo with familiar `DbContext` and `DbSet` patterns, but it does not yet replace a mature EF provider.

The bridge expects a host EF provider for EF infrastructure services. In examples, the EF in-memory provider is a convenient host, and `UseDataVo` attaches DataVo-specific behavior.

The following example is a complete `Program.cs` for a small EF-backed DataVo evaluation. It defines the entity, declares the context, registers EF's in-memory host provider, attaches DataVo, writes one row, reloads DataVo state in a second context, and runs a supported LINQ query pattern.

```csharp
using System;
using System.Collections.Generic;
using System.Linq;
using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseInMemoryDatabase("datavo-ef-host")
    .UseDataVo(dataVo => dataVo
        .UseInMemoryStorage()
        .WithDataSource("EfDemo"))
    .Options;

using (var writer = new AppDbContext(options))
{
    writer.Database.EnsureCreated();

    writer.Users.Add(new User
    {
        Id = 1,
        Name = "Ada",
        IsActive = true
    });

    writer.SaveChanges();
}

using (var reader = new AppDbContext(options))
{
    reader.LoadFromDataVo();

    List<User> firstTenActiveUsers = reader.QueryFromDataVo<User>(query => query
        .Where(static user => user.IsActive)
        .OrderBy(static user => user.Id)
        .Take(10));

    foreach (User user in firstTenActiveUsers)
    {
        Console.WriteLine($"{user.Id}: {user.Name}");
    }
}

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

Use query capability checks before running a LINQ query that may be outside the supported alpha subset.

## Entity Framework Support Summary

Supported: basic `DbSet` querying over mapped scalar CLR properties, basic inserts (`Add` plus `SaveChanges`), `LoadFromDataVo`, `QueryFromDataVo`/`QueryFromDataVoAsync` (`Where`, `OrderBy`, `ThenBy`, `Skip`, `Take`, `Select`), `ExplainQueryFromDataVo` capability checks, and vector function shims (`CosineDistance`, `L2Distance`). Native provider identity is planned. **Not** supported in v0.1: migrations, shadow properties, complex LINQ joins, and broad `GroupBy` translation.
