namespace SemanticInventory.Backend.Data;

public sealed class Item
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public DateTime CreatedUtc { get; set; } = DateTime.UtcNow;

    public Inventory? Inventory { get; set; }
    public ItemEmbedding? Embedding { get; set; }
    public ICollection<Sale> Sales { get; set; } = new List<Sale>();
}

public sealed class Inventory
{
    public int ItemId { get; set; }
    public int Quantity { get; set; }
    public string Location { get; set; } = string.Empty;

    public Item Item { get; set; } = null!;
}

public sealed class Sale
{
    public long Id { get; set; }
    public int ItemId { get; set; }
    public int Qty { get; set; }
    public decimal UnitPrice { get; set; }
    public DateTime OccurredUtc { get; set; } = DateTime.UtcNow;

    public Item Item { get; set; } = null!;
}

public sealed class ItemEmbedding
{
    public int ItemId { get; set; }
    public string VectorJson { get; set; } = "[]";
    public int Dimensions { get; set; }
    public float E0 { get; set; }
    public float E1 { get; set; }
    public float E2 { get; set; }

    public Item Item { get; set; } = null!;
}
