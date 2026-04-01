using Microsoft.EntityFrameworkCore;

namespace SemanticInventory.Backend.Data;

public sealed class SemanticInventoryContext(DbContextOptions<SemanticInventoryContext> options) : DbContext(options)
{
    public DbSet<Item> Items => Set<Item>();
    public DbSet<Inventory> Inventories => Set<Inventory>();
    public DbSet<Sale> Sales => Set<Sale>();
    public DbSet<ItemEmbedding> ItemEmbeddings => Set<ItemEmbedding>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Item>(entity =>
        {
            entity.ToTable("Items");
            entity.HasKey(item => item.Id);
            entity.Property(item => item.Name).HasMaxLength(120).IsRequired();
            entity.Property(item => item.Category).HasMaxLength(80).IsRequired();
            entity.Property(item => item.Description).HasMaxLength(1024).IsRequired();
            entity.Property(item => item.Price).HasPrecision(18, 2);
            entity.Property(item => item.CreatedUtc).IsRequired();
        });

        modelBuilder.Entity<Inventory>(entity =>
        {
            entity.ToTable("Inventory");
            entity.HasKey(inventory => inventory.ItemId);
            entity.Property(inventory => inventory.Location).HasMaxLength(64).IsRequired();
            entity.HasOne(inventory => inventory.Item)
                .WithOne(item => item.Inventory)
                .HasForeignKey<Inventory>(inventory => inventory.ItemId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<Sale>(entity =>
        {
            entity.ToTable("Sales");
            entity.HasKey(sale => sale.Id);
            entity.Property(sale => sale.UnitPrice).HasPrecision(18, 2);
            entity.Property(sale => sale.OccurredUtc).IsRequired();
            entity.HasOne(sale => sale.Item)
                .WithMany(item => item.Sales)
                .HasForeignKey(sale => sale.ItemId)
                .OnDelete(DeleteBehavior.Cascade);

            entity.HasIndex(sale => sale.OccurredUtc);
        });

        modelBuilder.Entity<ItemEmbedding>(entity =>
        {
            entity.ToTable("ItemEmbeddings");
            entity.HasKey(embedding => embedding.ItemId);
            entity.Property(embedding => embedding.VectorJson)
                .HasColumnType("TEXT")
                .HasDefaultValue("[]")
                .IsRequired();
            entity.Property(embedding => embedding.Dimensions)
                .HasDefaultValue(0)
                .IsRequired();
            entity.HasOne(embedding => embedding.Item)
                .WithOne(item => item.Embedding)
                .HasForeignKey<ItemEmbedding>(embedding => embedding.ItemId)
                .OnDelete(DeleteBehavior.Cascade);
        });

        base.OnModelCreating(modelBuilder);
    }
}
