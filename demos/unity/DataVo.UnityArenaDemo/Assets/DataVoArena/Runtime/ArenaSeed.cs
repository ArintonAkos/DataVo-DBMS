#nullable enable

using System;
using System.Collections.Generic;
using UnityEngine;

namespace DataVo.UnityArena
{
    public static class ArenaSeed
    {
        public const int VectorDimension = 32;

        private static readonly string[] MemoryTags = { "danger", "ally", "loot", "objective" };
        private static readonly string[] Teams = { "amber", "violet", "teal", "coral" };
        private static readonly string[] Zones = { "arena-7", "arena-5", "arena-3", "arena-1" };

        public static IReadOnlyList<ArenaNpcSeed> CreatePlayers(int logicalNpcCount)
        {
            if (logicalNpcCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(logicalNpcCount));
            }

            var players = new ArenaNpcSeed[logicalNpcCount];

            for (int index = 0; index < players.Length; index++)
            {
                int npcId = index + 1;
                players[index] = new ArenaNpcSeed(
                    id: npcId,
                    team: TeamFor(npcId),
                    zone: ZoneFor(npcId, tick: 0),
                    position: PositionFor(npcId, tick: 0),
                    health: 60 + npcId % 41,
                    score: (npcId * 37) % 1_000);
            }

            return players;
        }

        public static IReadOnlyList<ArenaMemorySeed> CreateMemories(int logicalNpcCount, int memoriesPerNpc)
        {
            if (logicalNpcCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(logicalNpcCount));
            }

            if (memoriesPerNpc <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(memoriesPerNpc));
            }

            int total = checked(logicalNpcCount * memoriesPerNpc);
            var memories = new ArenaMemorySeed[total];
            int writeIndex = 0;

            for (int npcId = 1; npcId <= logicalNpcCount; npcId++)
            {
                for (int memoryIndex = 0; memoryIndex < memoriesPerNpc; memoryIndex++)
                {
                    memories[writeIndex] = new ArenaMemorySeed(
                        id: writeIndex + 1,
                        npcId: npcId,
                        tag: MemoryTag(memoryIndex),
                        embedding: MemoryVector(npcId, memoryIndex));
                    writeIndex++;
                }
            }

            return memories;
        }

        public static IReadOnlyList<ArenaMutation> CreateMutations(
            int seed,
            int tick,
            int logicalNpcCount,
            int mutationsPerTick)
        {
            if (tick < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(tick));
            }

            if (logicalNpcCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(logicalNpcCount));
            }

            if (mutationsPerTick <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(mutationsPerTick));
            }

            var mutations = new ArenaMutation[mutationsPerTick];
            uint state = Mix((uint)seed ^ (uint)tick * 0x9E3779B9u);

            for (int index = 0; index < mutations.Length; index++)
            {
                int playerId = 1 + (int)(Next(ref state) % (uint)logicalNpcCount);
                int scoreDelta = 1 + (int)(Next(ref state) % 19u);
                int healthDelta = (int)(Next(ref state) % 9u) - 4;
                int zoneOffset = (int)(Next(ref state) % (uint)Zones.Length);
                int inventoryItemId = 100 + (int)(Next(ref state) % 64u);
                int inventorySlot = (int)(Next(ref state) % 8u);

                mutations[index] = new ArenaMutation(
                    playerId: playerId,
                    scoreDelta: scoreDelta,
                    healthDelta: healthDelta,
                    position: PositionFor(playerId, tick + index + 1),
                    zone: Zones[zoneOffset],
                    inventoryId: playerId,
                    inventoryItemId: inventoryItemId,
                    inventorySlot: inventorySlot);
            }

            return mutations;
        }

        public static float[] MemoryVector(int npcId, int memoryIndex)
        {
            if (npcId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(npcId));
            }

            if (memoryIndex < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(memoryIndex));
            }

            var vector = new float[VectorDimension];
            int tagAxis = memoryIndex % MemoryTags.Length;
            double lengthSquared = 0d;

            for (int component = 0; component < VectorDimension; component++)
            {
                float signature = SignedUnit(npcId, memoryIndex, component) * 0.15f;
                float value = component == tagAxis ? 1f + signature : signature;
                vector[component] = value;
                lengthSquared += value * value;
            }

            float inverseLength = (float)(1d / Math.Sqrt(lengthSquared));

            for (int component = 0; component < VectorDimension; component++)
            {
                vector[component] *= inverseLength;
            }

            return vector;
        }

        public static string MemoryTag(int memoryIndex)
        {
            if (memoryIndex < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(memoryIndex));
            }

            return MemoryTags[memoryIndex % MemoryTags.Length];
        }

        public static Vector3 PositionFor(int npcId, int tick)
        {
            if (npcId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(npcId));
            }

            if (tick < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(tick));
            }

            uint horizontal = Mix((uint)npcId * 0x85EBCA6Bu ^ (uint)tick * 0xC2B2AE35u);
            uint vertical = Mix((uint)npcId * 0x27D4EB2Du ^ (uint)tick * 0x165667B1u);
            float x = ((horizontal & 0xFFFFu) / 65535f - 0.5f) * 36f;
            float z = ((vertical & 0xFFFFu) / 65535f - 0.5f) * 24f;
            return new Vector3(x, 0f, z);
        }

        public static string TeamFor(int npcId)
        {
            if (npcId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(npcId));
            }

            return Teams[(npcId - 1) % Teams.Length];
        }

        public static string ZoneFor(int npcId, int tick)
        {
            if (npcId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(npcId));
            }

            if (tick < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(tick));
            }

            return Zones[(npcId + tick - 1) % Zones.Length];
        }

        private static float SignedUnit(int npcId, int memoryIndex, int component)
        {
            uint value = Mix(
                (uint)npcId * 0x9E3779B9u ^
                (uint)memoryIndex * 0x85EBCA6Bu ^
                (uint)component * 0xC2B2AE35u);
            return (value & 0x00FFFFFFu) / 8388607.5f - 1f;
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
}
