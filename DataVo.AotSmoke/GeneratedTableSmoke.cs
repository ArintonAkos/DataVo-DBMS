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

[DataVoTable("quests")]
public sealed record Quest([PrimaryKey] int Id, string Title, int Reward, bool Done);

public sealed record QuestLine(int Id, string Title);

public readonly record struct QuestHudSlot(int Id, int Reward);

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
        Require(items.Where(item => item.Name == null).Sum(item => item.Slot) == 20, "A sum compiled into the scan returned the wrong total.");
        Require(inSlot.Sum(item => item.Id) == 8 + 13 + 18, "A sum over an indexed condition returned the wrong total.");
        Require(inSlot.Min(item => item.Id) == 8 && inSlot.Max(item => item.Id) == 18, "A minimum or maximum compiled into the scan returned the wrong row's value.");
        Require(items.Where(item => item.Name == null).Max(item => item.Slot) == 4, "A maximum over a null test returned the wrong value.");
        Require(items.Where(item => item.Slot > 100).Min(item => item.Name) == null, "A minimum through the rows did not return null for an empty result.");
        bool threw = false;
        try
        {
            items.Where(item => item.Slot > 100).Min(item => item.Slot);
        }
        catch (InvalidOperationException)
        {
            threw = true;
        }

        Require(threw, "A minimum over no rows did not throw.");

        ComposedQueries(second);

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

    /// <summary>Sorting, paging, column-minimal projections and bulk changes, compiled at build time.</summary>
    private static void ComposedQueries(DataVoContext db)
    {
        db.EnsureQuestTable();
        for (int id = 1; id <= 12; id++)
        {
            db.Insert(new Quest(id, $"quest-{id}", id * 10 % 70, id % 3 == 0));
        }

        DataVoTable<Quest> quests = db.Table<Quest>();
        var top = quests.Where(static q => !q.Done).OrderByDescending(static q => q.Reward).ThenBy(static q => q.Id).Skip(1).Take(3);
        Require(top.IsCompiled, "The sorted, paged query was not compiled at build time.");
        Require(string.Join(",", top.Select(static q => q.Id).ToList()) == "4,11,10", "OrderBy, ThenBy, Skip and Take returned the wrong rows.");

        var lines = quests.OrderBy(static q => q.Id).Take(2).Select(static q => new QuestLine(q.Id, q.Title.ToUpperInvariant()));
        Require(lines.IsCompiled && lines.Sql == "SELECT Id, Title FROM quests ORDER BY Id LIMIT @limit", "The projection did not select only its columns.");
        Require(lines.FirstOrDefault() == new QuestLine(1, "QUEST-1"), "A compiled record projection built the wrong value.");

        var anonymous = quests.Where(4, static (q, min) => q.Reward > min).Select(static q => new { q.Title, Doubled = q.Reward * 2 }).ToList();
        Require(anonymous.Count == 11 && anonymous[0].Doubled == 20, "A compiled anonymous projection built the wrong values.");
        Require(quests.Take(0).Count() == 0 && quests.Skip(-5).Count() == 12 && !quests.Skip(12).Any(), "Skip or Take edge cases did not match LINQ.");

        Require(quests.Where(static q => q.Done).ExecuteUpdate(5, static (set, bonus) => set.Set(q => q.Reward, q => q.Reward + bonus)) == 4, "ExecuteUpdate changed the wrong number of rows.");
        Require(quests.Where(12, static (q, id) => q.Id == id).Select(static q => q.Reward).SingleOrDefault() == 55, "ExecuteUpdate arithmetic was wrong.");
        Require(quests.Where(static q => q.Reward < 25).ExecuteDelete() == 4 && quests.Count() == 8, "ExecuteDelete removed the wrong rows.");

        // Prepared statement reactive subscriptions (Subscribe + SubscribeZeroAlloc with AST-isolated bindings)
        // + typed LINQ Subscribe & SubscribeInto.
        DataVoPreparedStatement preparedSub = db.Prepare("SELECT Id, Reward FROM quests WHERE Done = @done");
        DataVoPreparedStatement preparedTop = db.Prepare("SELECT Id, Reward FROM quests WHERE Done = @done ORDER BY Reward DESC LIMIT @k");
        int preparedAdded = 0;
        int preparedZeroAllocAdded = 0;
        using (db.Subscribe(preparedSub, change => preparedAdded += change.Added.Count, ("done", false)))
        using (db.SubscribeZeroAlloc(preparedTop, (in DataVo.Core.Runtime.Reactive.QueryChangeRef change) => preparedZeroAllocAdded += change.Added.Count, ("done", false), ("k", 2)))
        {
            db.Insert(new Quest(100, "quest-100", 85, false));
            db.DispatchPendingNotifications();
        }
        Require(preparedAdded == 1, "Prepared statement reactive Subscribe missed an inserted row.");
        Require(preparedZeroAllocAdded == 1, "Prepared statement reactive SubscribeZeroAlloc missed an inserted row.");

        int typedTopAdded = 0;
        var hudSlots = new QuestHudSlot[2];
        int hudCount = 0;
        using (quests.Where(50, static (q, min) => q.Reward >= min).OrderByDescending(static q => q.Reward).Take(2).Subscribe(change => typedTopAdded += change.Added.Count))
        using (quests.Where(50, static (q, min) => q.Reward >= min).OrderByDescending(static q => q.Reward).Take(2).Select(static q => new QuestHudSlot(q.Id, q.Reward)).SubscribeInto(hudSlots.AsMemory(), count => hudCount = count))
        {
            db.Insert(new Quest(101, "quest-101", 500, false));
            db.DispatchPendingNotifications();
        }

        Require(typedTopAdded == 1, "Typed LINQ reactive Subscribe did not receive the new top quest.");
        Require(hudCount == 2 && hudSlots[0] == new QuestHudSlot(101, 500), "Typed LINQ SubscribeInto did not write the live sorted window.");

        LsmPersistedDeleteReopen();
    }

    /// <summary>
    /// Verifies intercepted <c>FirstOrDefault</c> integer-key lookup on LSM across reopen and persisted delete.
    /// </summary>
    private static void LsmPersistedDeleteReopen()
    {
        string lsmPath = Path.Combine(Path.GetTempPath(), $"datavo-aot-lsm-{Guid.NewGuid():N}");
        try
        {
            var config = new DataVoConfig
            {
                StorageMode = StorageMode.Lsm,
                DiskStoragePath = lsmPath,
                LsmStrictFsync = true,
            };

            using (var db = new DataVoContext(config))
            {
                db.Execute("CREATE DATABASE LsmSmoke");
                db.Execute("USE LsmSmoke");
                db.EnsureInventoryItemTable();
                db.Insert(new InventoryItem { Id = 42, Slot = 7, Name = "Shield" });
            }

            using (var reopened = new DataVoContext(config))
            {
                reopened.Execute("USE LsmSmoke");
                DataVoTable<InventoryItem> items = reopened.Table<InventoryItem>();
                InventoryItem? loaded = items.FirstOrDefault(42, static (item, id) => item.Id == id);
                Require(loaded is { Id: 42, Slot: 7, Name: "Shield" }, "LSM reopen did not find inserted key 42.");

                int deleted = items.Where(42, static (item, id) => item.Id == id).ExecuteDelete();
                Require(deleted == 1, "LSM ExecuteDelete did not remove key 42.");
            }

            using (var afterDelete = new DataVoContext(config))
            {
                afterDelete.Execute("USE LsmSmoke");
                DataVoTable<InventoryItem> items = afterDelete.Table<InventoryItem>();
                InventoryItem? missing = items.FirstOrDefault(42, static (item, id) => item.Id == id);
                Require(missing is null, "LSM reopen after persisted delete did not return null for key 42.");
            }
        }
        finally
        {
            if (Directory.Exists(lsmPath))
            {
                try { Directory.Delete(lsmPath, recursive: true); } catch { /* best-effort cleanup */ }
            }
        }
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }
}
