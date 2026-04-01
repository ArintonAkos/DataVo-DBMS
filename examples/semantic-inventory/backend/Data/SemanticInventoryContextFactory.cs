using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace SemanticInventory.Backend.Data;

public sealed class SemanticInventoryContextFactory : IDesignTimeDbContextFactory<SemanticInventoryContext>
{
    public SemanticInventoryContext CreateDbContext(string[] args)
    {
        var builder = new DbContextOptionsBuilder<SemanticInventoryContext>();
        builder.UseSqlite("Data Source=semantic-inventory.db");
        return new SemanticInventoryContext(builder.Options);
    }
}
