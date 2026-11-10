using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.AotSmoke;

[DataVoTable("inventory")]
public sealed class InventoryItem
{
    [PrimaryKey] public int Id { get; set; }

    [Indexed] public int Slot { get; set; }

    public string? Name { get; set; }
}

/// <summary>Proves generated table access runs in a native binary, with no reflection or runtime code generation.</summary>
internal static class GeneratedTableSmoke
{
    public static void Run()
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        db.Execute("CREATE DATABASE GeneratedSmoke");
        db.Execute("USE GeneratedSmoke");
        db.EnsureInventoryItemTable();

        db.Insert(new InventoryItem { Id = 1, Slot = 4, Name = "Sword" });
        Require(db.GetInventoryItemById(1) is { Slot: 4, Name: "Sword" }, "Generated insert and lookup did not round-trip.");
        Require(db.Update(new InventoryItem { Id = 1, Slot = 9, Name = "Axe" }), "Generated update changed no row.");
        Require(db.GetInventoryItemById(1) is { Slot: 9, Name: "Axe" }, "Generated update was lost.");
        Require(db.GetInventoryItemById(2) is null, "A missing row did not return null.");

        // The table is fetched before any generated method runs in a fresh context, so this also proves the
        // module initializer registered the model.
        using var second = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        second.Execute("CREATE DATABASE LinqSmoke");
        second.Execute("USE LinqSmoke");
        DataVoTable<InventoryItem> items = second.Table<InventoryItem>();
        second.EnsureInventoryItemTable();
        for (int id = 1; id <= 20; id++)
        {
            second.Insert(new InventoryItem { Id = id, Slot = id % 5, Name = id % 2 == 0 ? "even" : null });
        }

        int key = 7;
        Require(items.FirstOrDefault(key, static (item, id) => item.Id == id) is { Slot: 2 }, "An intercepted key lookup returned the wrong row.");
        Require(items.FirstOrDefault(item => item.Id == 99) is null, "An intercepted miss did not return null.");
        var inSlot = items.Where((Slot: 3, MinId: 5), static (item, s) => item.Slot == s.Slot && item.Id >= s.MinId);
        Require(inSlot.IsCompiled, "The query was not compiled at build time.");
        Require(inSlot.Count() == 3, "An intercepted conjunction returned the wrong rows.");
        Require(items.Where(item => item.Name == null).Count() == 10, "A null comparison returned the wrong rows.");
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }
}
