using Microsoft.EntityFrameworkCore;

namespace SemanticInventory.Backend.Data;

public static class AppSeeder
{
    public static async Task SeedAsync(SemanticInventoryContext context, int itemCount, CancellationToken cancellationToken = default)
    {
        int safeItemCount = Math.Clamp(itemCount, 100, 20000);
        int existingCount = await context.Items.CountAsync(cancellationToken);
        if (existingCount >= safeItemCount)
        {
            return;
        }

        int startId = existingCount + 1;
        var now = DateTime.UtcNow;

        string[] categories = ["Furniture", "Lighting", "Electronics", "Office", "Storage", "Decor"];
        string[] adjectives = ["Ergonomic", "Compact", "Premium", "Industrial", "Smart", "Minimal"];
        string[] nouns = ["Chair", "Desk", "Lamp", "Keyboard", "Monitor", "Shelf", "Cabinet", "Sofa", "Table", "Drawer"];

        var random = new Random(42);
        var items = new List<Item>(safeItemCount);
        var inventories = new List<Inventory>(safeItemCount);
        var sales = new List<Sale>(safeItemCount * 2);

        long saleId = (await context.Sales.MaxAsync(s => (long?)s.Id, cancellationToken) ?? 1000) + 1;
        for (int i = startId; i <= safeItemCount; i++)
        {
            string category = categories[(i - 1) % categories.Length];
            string adjective = adjectives[random.Next(adjectives.Length)];
            string noun = nouns[random.Next(nouns.Length)];
            string name = $"{adjective} {noun} {i:0000}";
            decimal price = Math.Round((decimal)(25 + (random.NextDouble() * 1200)), 2);

            items.Add(new Item
            {
                Id = i,
                Name = name,
                Category = category,
                Description = $"{adjective} {category.ToLowerInvariant()} {noun.ToLowerInvariant()} for inventory showcase item {i:0000}.",
                Price = price,
                CreatedUtc = now.AddMinutes(-i)
            });

            inventories.Add(new Inventory
            {
                ItemId = i,
                Quantity = random.Next(5, 500),
                Location = $"{(char)('A' + ((i - 1) % 12))}{((i - 1) % 40) + 1}"
            });

            int salesEvents = random.Next(1, 4);
            for (int s = 0; s < salesEvents; s++)
            {
                sales.Add(new Sale
                {
                    Id = saleId++,
                    ItemId = i,
                    Qty = random.Next(1, 10),
                    UnitPrice = price,
                    OccurredUtc = now.AddDays(-random.Next(1, 90)).AddMinutes(-random.Next(0, 1440))
                });
            }
        }

        context.Items.AddRange(items);
        context.Inventories.AddRange(inventories);
        context.Sales.AddRange(sales);

        await context.SaveChangesAsync(cancellationToken);
    }
}
