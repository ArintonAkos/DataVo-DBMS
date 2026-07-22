using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ArenaLiveViews;

public static class ArenaLiveViewsFixture
{
    private static readonly string[] Teams = ["amber", "violet", "teal", "coral"];
    private static readonly string[] Zones = ["arena-7", "arena-5", "arena-3", "arena-1"];

    public static ArenaLiveViewsPlayer[] CreateInitialPlayers(ArenaLiveViewsProfile profile)
    {
        ValidateProfile(profile);

        var players = new ArenaLiveViewsPlayer[profile.LogicalNpcCount];
        for (int index = 0; index < players.Length; index++)
        {
            int id = index + 1;
            players[index] = new ArenaLiveViewsPlayer(
                id,
                Teams[(id - 1) % Teams.Length],
                Zones[(id - 1) % Zones.Length],
                60 + id % 41,
                (id * 37) % 1_000);
        }

        return players;
    }

    public static ArenaLiveViewsInventory[] CreateInitialInventory(ArenaLiveViewsProfile profile)
    {
        ValidateProfile(profile);

        var inventory = new ArenaLiveViewsInventory[profile.LogicalNpcCount];
        for (int index = 0; index < inventory.Length; index++)
        {
            int id = index + 1;
            inventory[index] = new ArenaLiveViewsInventory(
                id,
                id,
                100 + id % 64,
                id % 8);
        }

        return inventory;
    }

    public static ArenaLiveViewsMutation[] CreateBatch(ArenaLiveViewsProfile profile, int batchIndex)
    {
        ValidateProfile(profile);
        ArgumentOutOfRangeException.ThrowIfNegative(batchIndex);

        uint state = Mix((uint)profile.Seed ^ (uint)batchIndex * 0x9E3779B9u);
        var result = new ArenaLiveViewsMutation[profile.MutationsPerBatch];

        for (int index = 0; index < result.Length; index++)
        {
            int playerId = 1 + (int)(Next(ref state) % (uint)profile.LogicalNpcCount);
            result[index] = new ArenaLiveViewsMutation(
                playerId,
                1 + (int)(Next(ref state) % 19u),
                (int)(Next(ref state) % 101u),
                Zones[Next(ref state) % (uint)Zones.Length],
                100 + (int)(Next(ref state) % 64u),
                (int)(Next(ref state) % 8u));
        }

        return result;
    }

    public static ArenaLiveViewsViews CreateExpectedViews(
        ArenaLiveViewsProfile profile,
        int appliedBatchCount)
    {
        ValidateProfile(profile);
        ArgumentOutOfRangeException.ThrowIfNegative(appliedBatchCount);

        ArenaLiveViewsPlayer[] players = CreateInitialPlayers(profile);
        ArenaLiveViewsInventory[] inventory = CreateInitialInventory(profile);

        for (int batchIndex = 0; batchIndex < appliedBatchCount; batchIndex++)
        {
            foreach (ArenaLiveViewsMutation mutation in CreateBatch(profile, batchIndex))
            {
                int playerIndex = mutation.PlayerId - 1;
                players[playerIndex] = players[playerIndex] with
                {
                    Score = players[playerIndex].Score + mutation.ScoreDelta,
                    Health = mutation.ReplacementHealth,
                    Zone = mutation.ReplacementZone
                };
                inventory[playerIndex] = inventory[playerIndex] with
                {
                    ItemId = mutation.InventoryItemId,
                    Slot = mutation.InventorySlot
                };
            }
        }

        return new ArenaLiveViewsViews(
            players
                .OrderByDescending(player => player.Score)
                .ThenBy(player => player.Id)
                .Take(20)
                .Select(player => new ArenaLeaderboardRow(player.Id, player.Score))
                .ToArray(),
            players
                .Where(player => player.Health > 0)
                .GroupBy(player => player.Team)
                .OrderBy(group => group.Key, StringComparer.Ordinal)
                .Select(group => new ArenaAliveByTeamRow(group.Key, group.Count()))
                .ToArray(),
            players
                .Where(player => player.Zone == "arena-7")
                .Join(
                    inventory,
                    player => player.Id,
                    item => item.PlayerId,
                    (player, item) => new ArenaZoneInventoryRow(player.Id, item.ItemId))
                .OrderBy(row => row.PlayerId)
                .ThenBy(row => row.ItemId)
                .ToArray());
    }

    private static void ValidateProfile(ArenaLiveViewsProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);

        if (profile.LogicalNpcCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(profile), "Logical NPC count must be positive.");
        }

        if (profile.MutationsPerBatch <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(profile), "Mutations per batch must be positive.");
        }

        if (profile.WarmupBatches <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(profile), "Warmup batches must be positive.");
        }

        if (profile.MeasuredBatches <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(profile), "Measured batches must be positive.");
        }

        if (profile.SimulatedCadenceHz <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(profile), "Simulated cadence must be positive.");
        }
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
}
