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
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }
}
