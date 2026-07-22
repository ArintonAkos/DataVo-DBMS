#nullable enable

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

[assembly: InternalsVisibleTo("DataVo.UnityArena.EditorTests")]

namespace DataVo.UnityArena
{
    public static class ArenaCrossEngineWorkload
    {
        public const string SchemaRevision = "arena-live-views-v1";
        public const string CreatePlayersSql = "CREATE TABLE Players (Id INT PRIMARY KEY, Team VARCHAR(10), Zone VARCHAR(20), Health INT, Score INT)";
        public const string CreateInventorySql = "CREATE TABLE Inventory (InventoryId INT PRIMARY KEY, PlayerId INT, ItemId INT, Slot INT)";
        public const string LeaderboardSql = "SELECT Id, Score FROM Players ORDER BY Score DESC, Id ASC LIMIT 20";
        public const string AggregateSql = "SELECT Team, COUNT(*) AS Alive FROM Players WHERE Health > 0 GROUP BY Team ORDER BY Team ASC";
        public const string ZoneInventorySql = "SELECT p.Id, i.ItemId FROM Players p JOIN Inventory i ON p.Id = i.PlayerId WHERE p.Zone = 'arena-7' ORDER BY p.Id ASC, i.ItemId ASC";
        public const string ReactiveAggregateSql = "SELECT Team, COUNT(*) AS Alive FROM Players WHERE Health > 0 GROUP BY Team";
        public const string ReactiveZoneInventorySql = "SELECT p.Id, i.ItemId FROM Players p JOIN Inventory i ON p.Id = i.PlayerId WHERE p.Zone = 'arena-7'";

        private static readonly string[] Teams = { "amber", "violet", "teal", "coral" };
        private static readonly string[] Zones = { "arena-7", "arena-5", "arena-3", "arena-1" };

        internal static int FullHistoryExpectedViewCallsForTest { get; private set; }

        internal static void ResetFullHistoryExpectedViewCallsForTest()
        {
            FullHistoryExpectedViewCallsForTest = 0;
        }

        public static ArenaCrossEnginePlayer[] CreateInitialPlayers(ArenaCrossEngineProfile profile)
        {
            ValidateProfile(profile);
            var players = new ArenaCrossEnginePlayer[profile.logicalNpcCount];

            for (int index = 0; index < players.Length; index++)
            {
                int id = index + 1;
                players[index] = new ArenaCrossEnginePlayer(
                    id,
                    Teams[(id - 1) % Teams.Length],
                    Zones[(id - 1) % Zones.Length],
                    60 + id % 41,
                    (id * 37) % 1_000);
            }

            return players;
        }

        public static ArenaCrossEngineInventory[] CreateInitialInventory(ArenaCrossEngineProfile profile)
        {
            ValidateProfile(profile);
            var inventory = new ArenaCrossEngineInventory[profile.logicalNpcCount];

            for (int index = 0; index < inventory.Length; index++)
            {
                int id = index + 1;
                inventory[index] = new ArenaCrossEngineInventory(id, id, 100 + id % 64, id % 8);
            }

            return inventory;
        }

        public static ArenaCrossEngineMutation[] CreateBatch(ArenaCrossEngineProfile profile, int batchIndex)
        {
            ValidateProfile(profile);
            if (batchIndex < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(batchIndex));
            }

            uint state = Mix((uint)profile.seed ^ (uint)batchIndex * 0x9E3779B9u);
            var result = new ArenaCrossEngineMutation[profile.mutationsPerBatch];

            for (int index = 0; index < result.Length; index++)
            {
                int playerId = 1 + (int)(Next(ref state) % (uint)profile.logicalNpcCount);
                result[index] = new ArenaCrossEngineMutation(
                    playerId,
                    1 + (int)(Next(ref state) % 19u),
                    (int)(Next(ref state) % 101u),
                    Zones[Next(ref state) % (uint)Zones.Length],
                    100 + (int)(Next(ref state) % 64u),
                    (int)(Next(ref state) % 8u));
            }

            return result;
        }

        public static ArenaCrossEngineViews CreateExpectedViews(ArenaCrossEngineProfile profile, int batchCount)
        {
            ValidateProfile(profile);
            if (batchCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(batchCount));
            }

            FullHistoryExpectedViewCallsForTest++;

            ArenaCrossEnginePlayer[] players = CreateInitialPlayers(profile);
            ArenaCrossEngineInventory[] inventory = CreateInitialInventory(profile);

            for (int batchIndex = 0; batchIndex < batchCount; batchIndex++)
            {
                foreach (ArenaCrossEngineMutation mutation in CreateBatch(profile, batchIndex))
                {
                    int playerIndex = mutation.playerId - 1;
                    ArenaCrossEnginePlayer player = players[playerIndex];
                    players[playerIndex] = player with
                    {
                        score = player.score + mutation.scoreDelta,
                        health = mutation.replacementHealth,
                        zone = mutation.replacementZone
                    };

                    ArenaCrossEngineInventory item = inventory[playerIndex];
                    inventory[playerIndex] = item with
                    {
                        itemId = mutation.inventoryItemId,
                        slot = mutation.inventorySlot
                    };
                }
            }

            return new ArenaCrossEngineViews(
                players
                    .OrderByDescending(player => player.score)
                    .ThenBy(player => player.id)
                    .Take(20)
                    .Select(player => new ArenaCrossEngineLeaderboardRow(player.id, player.score))
                    .ToArray(),
                players
                    .Where(player => player.health > 0)
                    .GroupBy(player => player.team)
                    .OrderBy(group => group.Key, StringComparer.Ordinal)
                    .Select(group => new ArenaCrossEngineAggregateRow(group.Key, group.Count()))
                    .ToArray(),
                players
                    .Where(player => player.zone == "arena-7")
                    .Join(
                        inventory,
                        player => player.id,
                        item => item.playerId,
                        (player, item) => new ArenaCrossEngineZoneInventoryRow(player.id, item.itemId))
                    .OrderBy(row => row.playerId)
                    .ThenBy(row => row.itemId)
                    .ToArray());
        }

        internal static ArenaCrossEngineHashes[] CreateExpectedHashSequence(
            ArenaCrossEngineProfile profile,
            int batchCount)
        {
            ValidateProfile(profile);
            if (batchCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(batchCount));
            }

            var state = new ExpectedHashOracle(profile);
            var hashes = new ArenaCrossEngineHashes[batchCount];

            for (int batchIndex = 0; batchIndex < batchCount; batchIndex++)
            {
                state.ApplyBatch(CreateBatch(profile, batchIndex));
                hashes[batchIndex] = Hash(state.CreateViews());
            }

            return hashes;
        }

        public static ArenaCrossEngineHashes Hash(ArenaCrossEngineViews views)
        {
            if (views is null)
            {
                throw new ArgumentNullException(nameof(views));
            }

            return new ArenaCrossEngineHashes(
                HashLines(views.leaderboard
                    .Select(row => row.id.ToString(CultureInfo.InvariantCulture) + "|" +
                        row.score.ToString(CultureInfo.InvariantCulture) + "\n")),
                HashLines(views.aggregate
                    .Select(row => row.team + "|" + row.alive.ToString(CultureInfo.InvariantCulture) + "\n")),
                HashLines(views.zoneInventory
                    .Select(row => row.playerId.ToString(CultureInfo.InvariantCulture) + "|" +
                        row.itemId.ToString(CultureInfo.InvariantCulture) + "\n")));
        }

        public static void ValidateProfile(ArenaCrossEngineProfile profile)
        {
            if (profile is null)
            {
                throw new ArgumentNullException(nameof(profile));
            }

            ArenaCrossEngineProfile expected = ArenaCrossEngineProfile.Parse(profile.id);
            if (profile != expected)
            {
                throw new ArgumentException("Profile must exactly match the gameplay or ivm-stress definition.", nameof(profile));
            }

            if (profile.logicalNpcCount <= 0 || profile.mutationsPerBatch <= 0 || profile.warmupBatches <= 0 ||
                profile.measuredBatches <= 0 || profile.simulatedCadenceHz <= 0)
            {
                throw new ArgumentException("Profile counts must all be positive.", nameof(profile));
            }
        }

        private static string HashLines(IEnumerable<string> lines)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(string.Concat(lines));
            using SHA256 hasher = SHA256.Create();
            byte[] hash = hasher.ComputeHash(bytes);
            var result = new StringBuilder(hash.Length * 2);

            foreach (byte value in hash)
            {
                result.Append(value.ToString("x2", CultureInfo.InvariantCulture));
            }

            return result.ToString();
        }

        private static uint Next(ref uint state)
        {
            state += 0x9E3779B9u;
            return Mix(state);
        }

        private static uint Mix(uint value)
        {
            value ^= value >> 16;
            value *= 0x7FEB352Du;
            value ^= value >> 15;
            value *= 0x846CA68Bu;
            value ^= value >> 16;
            return value;
        }

        private sealed class ExpectedHashOracle
        {
            private const string ArenaSeven = "arena-7";

            private static readonly IComparer<ArenaCrossEnginePlayer> LeaderboardComparer =
                Comparer<ArenaCrossEnginePlayer>.Create((left, right) =>
                {
                    int scoreOrder = right.score.CompareTo(left.score);
                    return scoreOrder != 0 ? scoreOrder : left.id.CompareTo(right.id);
                });

            private readonly ArenaCrossEnginePlayer[] players;
            private readonly ArenaCrossEngineInventory[] inventory;
            private readonly SortedSet<ArenaCrossEnginePlayer> leaderboard = new(LeaderboardComparer);
            private readonly SortedDictionary<int, int> zoneInventory = new();
            private readonly Dictionary<string, int> aliveByTeam = new(StringComparer.Ordinal);

            public ExpectedHashOracle(ArenaCrossEngineProfile profile)
            {
                players = CreateInitialPlayers(profile);
                inventory = CreateInitialInventory(profile);

                foreach (ArenaCrossEnginePlayer player in players)
                {
                    leaderboard.Add(player);
                    if (player.health > 0)
                    {
                        IncrementAlive(player.team, 1);
                    }

                    if (player.zone == ArenaSeven)
                    {
                        zoneInventory.Add(player.id, inventory[player.id - 1].itemId);
                    }
                }
            }

            public void ApplyBatch(IReadOnlyList<ArenaCrossEngineMutation> batch)
            {
                foreach (ArenaCrossEngineMutation mutation in batch)
                {
                    int playerIndex = mutation.playerId - 1;
                    ArenaCrossEnginePlayer previousPlayer = players[playerIndex];
                    ArenaCrossEngineInventory previousInventory = inventory[playerIndex];

                    if (!leaderboard.Remove(previousPlayer))
                    {
                        throw new InvalidOperationException("Expected hash oracle could not remove a leaderboard player.");
                    }

                    if (previousPlayer.health > 0)
                    {
                        IncrementAlive(previousPlayer.team, -1);
                    }

                    if (previousPlayer.zone == ArenaSeven)
                    {
                        zoneInventory.Remove(previousPlayer.id);
                    }

                    ArenaCrossEnginePlayer updatedPlayer = previousPlayer with
                    {
                        score = previousPlayer.score + mutation.scoreDelta,
                        health = mutation.replacementHealth,
                        zone = mutation.replacementZone
                    };
                    ArenaCrossEngineInventory updatedInventory = previousInventory with
                    {
                        itemId = mutation.inventoryItemId,
                        slot = mutation.inventorySlot
                    };
                    players[playerIndex] = updatedPlayer;
                    inventory[playerIndex] = updatedInventory;

                    leaderboard.Add(updatedPlayer);
                    if (updatedPlayer.health > 0)
                    {
                        IncrementAlive(updatedPlayer.team, 1);
                    }

                    if (updatedPlayer.zone == ArenaSeven)
                    {
                        zoneInventory[updatedPlayer.id] = updatedInventory.itemId;
                    }
                }
            }

            public ArenaCrossEngineViews CreateViews()
            {
                ArenaCrossEngineLeaderboardRow[] leaderboardRows = leaderboard
                    .Take(20)
                    .Select(player => new ArenaCrossEngineLeaderboardRow(player.id, player.score))
                    .ToArray();
                ArenaCrossEngineAggregateRow[] aggregateRows = aliveByTeam
                    .Where(pair => pair.Value > 0)
                    .OrderBy(pair => pair.Key, StringComparer.Ordinal)
                    .Select(pair => new ArenaCrossEngineAggregateRow(pair.Key, pair.Value))
                    .ToArray();
                ArenaCrossEngineZoneInventoryRow[] zoneInventoryRows = zoneInventory
                    .Select(pair => new ArenaCrossEngineZoneInventoryRow(pair.Key, pair.Value))
                    .ToArray();

                return new ArenaCrossEngineViews(leaderboardRows, aggregateRows, zoneInventoryRows);
            }

            private void IncrementAlive(string team, int delta)
            {
                aliveByTeam.TryGetValue(team, out int existing);
                aliveByTeam[team] = existing + delta;
            }
        }
    }
}
