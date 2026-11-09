#nullable enable

using System;
using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.UnitySmoke.Player
{
    [DataVoTable("inventory")]
    public sealed class InventoryItem
    {
        [PrimaryKey] public int Id { get; set; }

        [Indexed] public int Slot { get; set; }

        public string? Name { get; set; }

        public float Weight { get; set; }

        public Guid Owner { get; set; }

        public long Stack { get; set; }

        public double Value { get; set; }
    }

    internal static partial class PlayerSmokeBootstrap
    {
        static partial void RunPackageScenarios()
        {
            using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
            db.Execute("CREATE DATABASE GeneratedSmoke");
            db.Execute("USE GeneratedSmoke");
            db.EnsureInventoryItemTable();

            Guid owner = Guid.NewGuid();
            db.Insert(new InventoryItem { Id = 1, Slot = 4, Name = "Sword", Weight = 2.5f, Owner = owner, Stack = 9007199254740993, Value = 0.1 });
            InventoryItem? loaded = db.GetInventoryItemById(1);
            Require(loaded != null && loaded.Slot == 4 && loaded.Name == "Sword" && loaded.Weight == 2.5f && loaded.Owner == owner && loaded.Stack == 9007199254740993 && loaded.Value == 0.1, "Generated insert and lookup did not round-trip.");
            Require(db.Update(new InventoryItem { Id = 1, Slot = 9, Name = "Axe", Weight = 3f, Owner = owner, Stack = 1, Value = 2 }), "Generated update changed no row.");
            Require(db.GetInventoryItemById(1)!.Slot == 9, "Generated update was lost.");
            Require(db.GetInventoryItemById(2) == null, "A missing row did not return null.");
        }
    }
}
