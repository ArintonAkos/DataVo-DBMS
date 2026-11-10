using System.Numerics;
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

[DataVoTable("probes")]
public sealed class Probe
{
    [PrimaryKey] public int Id { get; set; }

    [SpatialIndex] public Vector3 Position { get; set; }
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

        // Spatial queries over a Vector3 column, with the HNSW index and with the exact scan.
        second.EnsureProbeTable();
        for (int cell = 0; cell < 64; cell++)
        {
            second.Insert(new Probe { Id = cell + 1, Position = new Vector3(cell % 4, cell / 4 % 4, cell / 16) }); // a 4x4x4 grid
        }

        DataVoTable<Probe> probes = second.Table<Probe>();
        Require(second.GetProbeById(7)!.Position == new Vector3(2, 1, 0), "A Vector3 column did not round-trip.");
        Require(probes.WithinRadius(new Vector3(0, 0, 0), 1f).Count() == 4, "WithinRadius returned the wrong rows.");
        Require(probes.WithinBoundingBox(Vector3.Zero, new Vector3(1, 1, 1)).Count() == 8, "WithinBoundingBox returned the wrong rows.");
        Require(probes.NearestNeighbors(new Vector3(3, 3, 3), 1)[0].Position == new Vector3(3, 3, 3), "The HNSW nearest neighbor was wrong.");
        Require(probes.NearestNeighbors(new Vector3(3, 3, 3), 1, exact: true)[0].Position == new Vector3(3, 3, 3), "The exact nearest neighbor was wrong.");

        // Rows read straight from storage as RowRef values, no models built.
        RowRefStream inventory = items.StreamRef();
        int name = inventory.GetOrdinal("Name");
        int streamed = 0;
        int named = 0;
        long idSum = 0;
        foreach (RowRef row in inventory)
        {
            idSum += row.GetInt32(0);
            if (!row.IsNull(name) && row.GetStringSpan(name).SequenceEqual("even".AsSpan()))
            {
                named++;
            }

            streamed++;
        }

        Require(streamed == 20 && named == 10 && idSum == 210, "StreamRef read the wrong rows.");

        Vector3 corner = default;
        foreach (RowRef row in probes.StreamRef())
        {
            if (row.GetInt32(0) == 64)
            {
                corner = row.GetVector3(1);
            }
        }

        Require(corner == new Vector3(3, 3, 3), "StreamRef read the wrong Vector3.");
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }
}
