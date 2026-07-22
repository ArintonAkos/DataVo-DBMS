#nullable enable

using System;
using System.Collections.Generic;
using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;
using DataVo.Unity;
using UnityEngine;

namespace DataVo.UnityArena
{
    public sealed class ArenaDataSession : IDisposable
    {
        public const string LeaderboardSql = "SELECT Id, Score FROM Players ORDER BY Score DESC LIMIT 20";
        public const string AliveByTeamSql = "SELECT Team, COUNT(*) AS Alive FROM Players WHERE Health > 0 GROUP BY Team";
        public const string ZoneInventorySql = "SELECT p.Id, i.ItemId FROM Players p JOIN Inventory i ON p.Id = i.PlayerId WHERE p.Zone = 'arena-7'";

        private const string DatabaseName = "UnityArena";
        private readonly DataVoContext context;
        private readonly ArenaRunOptions options;
        private bool disposed;

        public ArenaDataSession(ArenaRunOptions options)
        {
            this.options = options ?? throw new ArgumentNullException(nameof(options));
            ValidateDataOptions(options);

            context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
            context.UseUnityTypes();
            ExecuteSql("CREATE DATABASE " + DatabaseName);
            ExecuteSql("USE " + DatabaseName);
            CreateSchema();
            SeedData();
            ExecuteSql("CREATE INDEX IX_NpcMemories_Embedding ON NpcMemories (Embedding) USING FLAT");
        }

        public ArenaRunOptions Options => options;

        internal DataVoContext Context => context;

        public IReadOnlyList<ArenaMemoryMatch> FindNearestMemories(int npcId, int topK)
        {
            ThrowIfDisposed();

            if (npcId <= 0 || npcId > options.LogicalNpcCount)
            {
                throw new ArgumentOutOfRangeException(nameof(npcId));
            }

            if (topK <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(topK));
            }

            int stimulusMemoryIndex = (npcId - 1) % 4;
            float[] query = ArenaSeed.MemoryVector(npcId, stimulusMemoryIndex);
            List<Dictionary<string, object?>> rows = context.SearchNearest(
                "NpcMemories",
                "IX_NpcMemories_Embedding",
                query,
                topK);

            if (rows.Count == 0)
            {
                throw new InvalidOperationException("Seeded NPC memory search returned no matches.");
            }

            var matches = new ArenaMemoryMatch[rows.Count];

            for (int index = 0; index < rows.Count; index++)
            {
                Dictionary<string, object?> row = rows[index];
                float[] embedding = RequireVector(row, "Embedding");
                matches[index] = new ArenaMemoryMatch(
                    id: RequireInt(row, "Id"),
                    npcId: RequireInt(row, "NpcId"),
                    tag: RequireString(row, "Tag"),
                    distance: EuclideanDistance(query, embedding));
            }

            return matches;
        }

        public Vector3 ReadPlayerPosition(int playerId)
        {
            ThrowIfDisposed();

            if (playerId <= 0 || playerId > options.LogicalNpcCount)
            {
                throw new ArgumentOutOfRangeException(nameof(playerId));
            }

            IReadOnlyList<Dictionary<string, object?>> rows = QueryRows(
                "SELECT Position FROM Players WHERE Id = " + playerId.ToString(CultureInfo.InvariantCulture));

            if (rows.Count != 1)
            {
                throw new InvalidOperationException("Seeded player position was not found.");
            }

            return context.Get<Vector3>(rows[0], "Position");
        }

        internal IReadOnlyList<Dictionary<string, object?>> QueryRows(string sql)
        {
            ThrowIfDisposed();
            List<QueryResult> results = context.Execute(sql);
            ThrowOnErrors(results, sql);

            if (results.Count != 1)
            {
                throw new InvalidOperationException("Expected one query result for SQL: " + sql);
            }

            return results[0].Data;
        }

        internal void ApplyMutations(IReadOnlyList<ArenaMutation> mutations)
        {
            ThrowIfDisposed();

            if (mutations is null)
            {
                throw new ArgumentNullException(nameof(mutations));
            }

            foreach (ArenaMutation mutation in mutations)
            {
                if (mutation.playerId <= 0 || mutation.playerId > options.LogicalNpcCount)
                {
                    throw new ArgumentOutOfRangeException(nameof(mutations), "Mutation references an unknown player.");
                }

                string scoreExpression = "Score + " + mutation.scoreDelta.ToString(CultureInfo.InvariantCulture);
                string healthExpression = mutation.healthDelta >= 0
                    ? "Health + " + mutation.healthDelta.ToString(CultureInfo.InvariantCulture)
                    : "Health - " + (-mutation.healthDelta).ToString(CultureInfo.InvariantCulture);
                string position = "[" +
                    mutation.position.x.ToString("R", CultureInfo.InvariantCulture) + "," +
                    mutation.position.y.ToString("R", CultureInfo.InvariantCulture) + "," +
                    mutation.position.z.ToString("R", CultureInfo.InvariantCulture) + "]";

                ExecuteSql(
                    "UPDATE Players SET Score = " + scoreExpression + ", Health = " + healthExpression +
                    ", Position = '" + position + "', Zone = " + SqlStringLiteral(mutation.zone) +
                    " WHERE Id = " + mutation.playerId.ToString(CultureInfo.InvariantCulture));
                ExecuteSql(
                    "UPDATE Inventory SET ItemId = " + mutation.inventoryItemId.ToString(CultureInfo.InvariantCulture) +
                    ", Slot = " + mutation.inventorySlot.ToString(CultureInfo.InvariantCulture) +
                    " WHERE InventoryId = " + mutation.inventoryId.ToString(CultureInfo.InvariantCulture));
            }
        }

        public void Dispose()
        {
            if (disposed)
            {
                return;
            }

            disposed = true;
            context.Dispose();
        }

        private void CreateSchema()
        {
            ExecuteSql(
                "CREATE TABLE Players (" +
                "Id INT PRIMARY KEY, Team VARCHAR(10), Zone VARCHAR(20), " +
                "Position VECTOR(3), Health INT, Score INT" +
                ")");
            ExecuteSql(
                "CREATE TABLE Inventory (" +
                "InventoryId INT PRIMARY KEY, PlayerId INT, ItemId INT, Slot INT" +
                ")");
            ExecuteSql(
                "CREATE TABLE NpcMemories (" +
                "Id INT PRIMARY KEY, NpcId INT, Tag VARCHAR(20), Embedding VECTOR(32)" +
                ")");
        }

        private void SeedData()
        {
            IReadOnlyList<ArenaNpcSeed> players = ArenaSeed.CreatePlayers(options.LogicalNpcCount);

            foreach (ArenaNpcSeed player in players)
            {
                context.Insert("Players", new ArenaPlayerRow
                {
                    Id = player.id,
                    Team = player.team,
                    Zone = player.zone,
                    Position = player.position,
                    Health = player.health,
                    Score = player.score
                });
            }

            var inventoryRows = new IReadOnlyDictionary<string, object?>[players.Count];

            for (int index = 0; index < players.Count; index++)
            {
                ArenaNpcSeed player = players[index];
                inventoryRows[index] = new Dictionary<string, object?>
                {
                    ["InventoryId"] = player.id,
                    ["PlayerId"] = player.id,
                    ["ItemId"] = 100 + player.id % 64,
                    ["Slot"] = player.id % 8
                };
            }

            context.BulkInsert("Inventory", inventoryRows);

            IReadOnlyList<ArenaMemorySeed> memories = ArenaSeed.CreateMemories(
                options.LogicalNpcCount,
                options.MemoriesPerNpc);
            var memoryRows = new IReadOnlyDictionary<string, object?>[memories.Count];

            for (int index = 0; index < memories.Count; index++)
            {
                ArenaMemorySeed memory = memories[index];
                memoryRows[index] = new Dictionary<string, object?>
                {
                    ["Id"] = memory.id,
                    ["NpcId"] = memory.npcId,
                    ["Tag"] = memory.tag,
                    ["Embedding"] = memory.embedding
                };
            }

            context.BulkInsert("NpcMemories", memoryRows);
        }

        private void ExecuteSql(string sql)
        {
            ThrowOnErrors(context.Execute(sql), sql);
        }

        private static void ThrowOnErrors(IReadOnlyList<QueryResult> results, string sql)
        {
            foreach (QueryResult result in results)
            {
                if (result.IsError)
                {
                    throw new InvalidOperationException(
                        "DataVo rejected arena SQL '" + sql + "': " + string.Join(" | ", result.Messages));
                }
            }
        }

        private static void ValidateDataOptions(ArenaRunOptions options)
        {
            // This session owns only the logical database. Visual-count validation
            // belongs to ArenaRunOptions.Validate(), which the rendering runner uses.
            if (options.LogicalNpcCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options.LogicalNpcCount));
            }

            if (options.MemoriesPerNpc <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(options.MemoriesPerNpc));
            }

            if ((long)options.LogicalNpcCount * options.MemoriesPerNpc > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(options.MemoriesPerNpc));
            }
        }

        private static int RequireInt(IReadOnlyDictionary<string, object?> row, string column)
        {
            if (!row.TryGetValue(column, out object? value) || value is null)
            {
                throw new InvalidOperationException("Vector search result does not contain " + column + ".");
            }

            return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        private static string RequireString(IReadOnlyDictionary<string, object?> row, string column)
        {
            if (!row.TryGetValue(column, out object? value) || value is not string text || text.Length == 0)
            {
                throw new InvalidOperationException("Vector search result does not contain " + column + ".");
            }

            return text;
        }

        private static float[] RequireVector(IReadOnlyDictionary<string, object?> row, string column)
        {
            if (!row.TryGetValue(column, out object? value) || value is not float[] vector || vector.Length != ArenaSeed.VectorDimension)
            {
                throw new InvalidOperationException("Vector search result does not contain a VECTOR(32) " + column + ".");
            }

            return vector;
        }

        private static float EuclideanDistance(float[] left, float[] right)
        {
            double squared = 0d;

            for (int index = 0; index < left.Length; index++)
            {
                double delta = left[index] - right[index];
                squared += delta * delta;
            }

            return (float)Math.Sqrt(squared);
        }

        private static string SqlStringLiteral(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new ArgumentException("SQL text value is required.", nameof(value));
            }

            return "'" + value.Replace("'", "''") + "'";
        }

        private void ThrowIfDisposed()
        {
            if (disposed)
            {
                throw new ObjectDisposedException(nameof(ArenaDataSession));
            }
        }

        public sealed class ArenaPlayerRow
        {
            public int Id { get; set; }
            public string Team { get; set; } = string.Empty;
            public string Zone { get; set; } = string.Empty;
            public Vector3 Position { get; set; }
            public int Health { get; set; }
            public int Score { get; set; }
        }
    }
}
