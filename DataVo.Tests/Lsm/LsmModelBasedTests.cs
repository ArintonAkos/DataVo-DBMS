using DataVo.Core;
using System.Text;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Model-based randomized test of the LSM storage engine. A plain dictionary per table is the model of
/// what the engine must hold. Random operations (inserts, deletes, in-place overwrites, atomic batches
/// that commit or abort, flushes, compactions, clean reopens, and crashes) run against both, and after
/// every step reads, scans and counts must agree with the model. One table stores rows under keys the
/// test chooses, as a clustered primary key does: GUIDs and negative, small and huge integers.
/// </summary>
/// <remarks>
/// A failure message names the seed and the last operations, so the exact run can be replayed. Set
/// DATAVO_MODEL_SEEDS (count) and DATAVO_MODEL_STEPS to run longer locally than in CI, and
/// DATAVO_SEED_OFFSET to start at another seed (the nightly job uses fresh seeds every run).
/// </remarks>
public sealed class LsmModelBasedTests : IDisposable
{
    private const string Database = "db";
    private static readonly string[] Tables = ["alpha", "beta", KeyedTable];

    /// <summary>The table whose rows are inserted under chosen keys instead of engine-assigned ids.</summary>
    private const string KeyedTable = "gamma";

    private readonly string _root = TestStorageDirectories.Create("lsm_model_based");

    public static TheoryData<int> Seeds()
    {
        int count = int.TryParse(Environment.GetEnvironmentVariable("DATAVO_MODEL_SEEDS"), out int configured) ? configured : 4;
        int first = int.TryParse(Environment.GetEnvironmentVariable("DATAVO_SEED_OFFSET"), out int offset) ? offset + 1 : 1;
        var seeds = new TheoryData<int>();
        for (int seed = first; seed < first + count; seed++)
        {
            seeds.Add(seed);
        }

        return seeds;
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void RandomOperations_MatchTheModel(int seed)
    {
        int steps = int.TryParse(Environment.GetEnvironmentVariable("DATAVO_MODEL_STEPS"), out int configured) ? configured : 800;
        new Run(Path.Combine(_root, seed.ToString()), seed).Execute(steps);
    }

    private sealed class Run(string directory, int seed)
    {
        private readonly Random _random = new(seed);
        private readonly LsmReadMode _readMode = seed % 2 == 0 ? LsmReadMode.MemoryMapped : LsmReadMode.PositionalRead;
        private readonly Dictionary<string, SortedDictionary<RowKey, byte[]>> _model = Tables.ToDictionary(table => table, _ => new SortedDictionary<RowKey, byte[]>());
        private readonly Dictionary<string, long> _highestCommittedId = Tables.ToDictionary(table => table, _ => 0L);
        private readonly Queue<string> _recent = new();
        private LsmStorageEngine _engine = null!;
        private int _step;

        public void Execute(int steps)
        {
            _engine = Open();
            try
            {
                for (_step = 0; _step < steps; _step++)
                {
                    RunOneOperation();
                    if (_step % 25 == 0)
                    {
                        VerifyAll();
                    }
                }

                VerifyAll();
                Log("crash");
                _engine.SimulateCrashForTesting();
                _engine = Open();
                VerifyAll();
            }
            finally
            {
                _engine.Dispose();
            }
        }

        private void RunOneOperation()
        {
            string table = Tables[_random.Next(Tables.Length)];
            int roll = _random.Next(100);
            switch (roll)
            {
                case < 25:
                    InsertOne(table);
                    break;
                case < 33:
                    InsertMany(table);
                    break;
                case < 46:
                    Delete(table);
                    break;
                case < 56:
                    Overwrite(table);
                    break;
                case < 66:
                    Batch(table);
                    break;
                case < 82:
                    VerifyPointReads(table);
                    break;
                case < 87:
                    Log($"flush {table}");
                    _engine.FlushTableForTesting(Database, table);
                    break;
                case < 90:
                    Compact(table);
                    break;
                case < 95:
                    Log("reopen");
                    _engine.Dispose();
                    _engine = Open();
                    break;
                default:
                    Log("crash");
                    _engine.SimulateCrashForTesting();

                    _engine = Open();
                    break;
            }
        }

        private LsmStorageEngine Open()
        {
            var engine = new LsmStorageEngine(directory, LsmWalDurabilityMode.RelaxedOsBuffer, _readMode)
            {
                // Small MemTables so rotation, background flushes and compaction happen on their own too.
                MemTableFlushThresholdOverrideBytes = 16_384 << _random.Next(5),
            };
            return engine;
        }

        private void InsertOne(string table)
        {
            if (table == KeyedTable)
            {
                InsertKeyed(table);
                return;
            }

            byte[] value = NewValue();
            RowKey rowId = _engine.InsertRow(Database, table, value);
            Log($"insert {table} -> {rowId} ({value.Length} B)");
            Committed(table, rowId, value);
        }

        private void InsertMany(string table)
        {
            if (table == KeyedTable)
            {
                for (int i = _random.Next(1, 20); i > 0; i--)
                {
                    InsertKeyed(table);
                }

                return;
            }

            List<byte[]> values = [.. Enumerable.Range(0, 1 + _random.Next(20)).Select(_ => NewValue())];
            List<RowKey> rowIds = _engine.InsertRows(Database, table, [.. values]);
            Log($"insert-rows {table} x{values.Count} -> {string.Join(",", rowIds)}");
            Check(rowIds.Count == values.Count, "InsertRows returned a different number of row ids");
            for (int i = 0; i < rowIds.Count; i++)
            {
                Committed(table, rowIds[i], values[i]);
            }
        }

        /// <summary>Inserts under a chosen key; a key that is already live must be rejected.</summary>
        private void InsertKeyed(string table)
        {
            RowKey key = PickRowId(table);
            byte[] value = NewValue();
            bool inserted = _engine.TryInsertRowWithKey(Database, table, key, value);
            Log($"insert-keyed {table} {key} -> {inserted}");
            Check(inserted == !_model[table].ContainsKey(key), $"keyed insert of {key} returned {inserted}");
            if (inserted)
            {
                _model[table][key] = value;
            }
        }

        private void Delete(string table)
        {
            RowKey rowId = PickRowId(table);
            Log($"delete {table} {rowId}");
            _engine.DeleteRow(Database, table, rowId);
            _model[table].Remove(rowId);
        }

        private void Overwrite(string table)
        {
            RowKey rowId = PickRowId(table);
            byte[] value = NewValue();
            bool live = _engine.OverwriteRowForTesting(Database, table, rowId, value);
            Log($"overwrite {table} {rowId} -> {live}");
            Check(live == _model[table].ContainsKey(rowId), $"overwrite of row {rowId} reported live={live}");
            if (live)
            {
                _model[table][rowId] = value;
            }
        }

        /// <summary>Several writes in one atomic frame; the batch sees its own writes, and an abort leaves nothing.</summary>
        private void Batch(string table)
        {
            bool commit = _random.Next(5) != 0;
            var pending = new SortedDictionary<RowKey, byte[]>(_model[table]);
            var inserted = new List<RowKey>();
            Log($"batch {table} {(commit ? "commit" : "abort")}");
            using (IWriteBatchScope scope = _engine.BeginWriteBatch())
            {
                int operations = 1 + _random.Next(8);
                for (int i = 0; i < operations; i++)
                {
                    switch (_random.Next(3))
                    {
                        case 0 when table == KeyedTable:
                            RowKey key = PickRowId(table, pending);
                            byte[] keyedValue = NewValue();
                            bool added = _engine.TryInsertRowWithKey(Database, table, key, keyedValue);
                            Check(added == !pending.ContainsKey(key), $"batch keyed insert of {key} returned {added}");
                            if (added)
                            {
                                pending[key] = keyedValue;
                                inserted.Add(key);
                            }

                            break;
                        case 0:
                            byte[] value = NewValue();
                            RowKey rowId = _engine.InsertRow(Database, table, value);
                            pending[rowId] = value;
                            inserted.Add(rowId);
                            break;
                        case 1:
                            RowKey deleted = PickRowId(table, pending);
                            _engine.DeleteRow(Database, table, deleted);
                            pending.Remove(deleted);
                            break;
                        default:
                            RowKey target = PickRowId(table, pending);
                            byte[] next = NewValue();
                            if (_engine.OverwriteRowForTesting(Database, table, target, next))
                            {
                                Check(pending.ContainsKey(target), $"batch overwrite found row {target} the batch had removed");
                                pending[target] = next;
                            }

                            break;
                    }
                }

                // Read-your-writes inside the batch, including its own deletions.
                foreach (RowKey rowId in pending.Keys.Take(5).Concat(inserted))
                {
                    byte[]? seen = Read(table, rowId);
                    if (pending.TryGetValue(rowId, out byte[]? expected))
                    {
                        Check(seen is not null && seen.AsSpan().SequenceEqual(expected), $"batch reads its own row {rowId} as {Describe(seen)}");
                    }
                    else
                    {
                        Check(seen is null, $"batch still sees row {rowId} it deleted");
                    }
                }

                if (commit)
                {
                    scope.Complete();
                }
            }

            if (commit)
            {
                foreach (RowKey rowId in inserted.Where(_ => table != KeyedTable))
                {
                    Check((long)rowId > _highestCommittedId[table], $"row id {rowId} reused (highest committed {_highestCommittedId[table]})");
                    _highestCommittedId[table] = Math.Max(_highestCommittedId[table], (long)rowId);
                }

                _model[table] = pending;
            }
            else
            {
                foreach (RowKey rowId in inserted)
                {
                    // A chosen key the batch deleted and inserted again keeps its committed row on abort.
                    byte[]? seen = Read(table, rowId);
                    Check(
                        _model[table].TryGetValue(rowId, out byte[]? committed)
                            ? seen is not null && seen.AsSpan().SequenceEqual(committed)
                            : seen is null,
                        $"aborted insert {rowId} is visible");
                }
            }
        }

        private void Compact(string table)
        {
            Log($"compact {table}");
            List<(RowKey NewRowId, byte[] RawRow)> rows = _engine.CompactTable(Database, table);
            CheckRowsEqual(table, rows, "compaction result");
        }

        private void VerifyPointReads(string table)
        {
            SortedDictionary<RowKey, byte[]> model = _model[table];
            for (int i = 0; i < 4; i++)
            {
                RowKey rowId = PickRowId(table);
                byte[]? actual = Read(table, rowId);
                if (model.TryGetValue(rowId, out byte[]? expected))
                {
                    Check(actual is not null && actual.AsSpan().SequenceEqual(expected), $"row {rowId} differs from the model");
                }
                else
                {
                    Check(actual is null, $"row {rowId} should not exist");
                }
            }
        }

        private void VerifyAll()
        {
            foreach (string table in Tables)
            {
                CheckRowsEqual(table, [.. _engine.ReadAllRows(Database, table)], "full scan");
                SortedDictionary<RowKey, byte[]> model = _model[table];
                Check(_engine.EstimateRowCount(Database, table) == model.Count, $"row count {_engine.EstimateRowCount(Database, table)} != {model.Count}");
                Check(_engine.HasAnyRows(Database, table) == model.Count > 0, "HasAnyRows disagrees with the model");

                var versions = new List<RowKey>();
                ulong maxSequence = 0;
                _engine.ForEachLiveRowVersion(Database, table, (rowId, sequence) =>
                {
                    versions.Add(rowId);
                    maxSequence = Math.Max(maxSequence, sequence);
                });
                Check(versions.SequenceEqual(model.Keys), "live row versions differ from the model");
                Check(maxSequence <= (ulong)_engine.AppliedLsn, $"row sequence {maxSequence} is above the applied LSN {_engine.AppliedLsn}");
            }
        }

        private void CheckRowsEqual(string table, List<(RowKey RowId, byte[] RawRow)> rows, string what)
        {
            SortedDictionary<RowKey, byte[]> model = _model[table];
            Check(rows.Count == model.Count, $"{what} of {table}: {rows.Count} rows, model has {model.Count}");
            int index = 0;
            foreach ((RowKey rowId, byte[] expected) in model)
            {
                Check(rows[index].RowId == rowId, $"{what} of {table}: row {index} is id {rows[index].RowId}, expected {rowId}");
                Check(rows[index].RawRow.AsSpan().SequenceEqual(expected), $"{what} of {table}: row {rowId} has different bytes");
                index++;
            }
        }

        private byte[]? Read(string table, RowKey rowId)
        {
            try
            {
                return _engine.ReadRow(Database, table, rowId);
            }
            catch (Exception ex) when (ex.GetType().Name is "RowNotFoundException" or "RowDeletedException")
            {
                return null;
            }
        }

        private static string Describe(byte[]? value) => value is null ? "missing" : $"{value.Length} different bytes";

        private void Committed(string table, RowKey rowId, byte[] value)
        {
            Check((long)rowId > _highestCommittedId[table], $"row id {rowId} reused (highest committed {_highestCommittedId[table]})");
            _highestCommittedId[table] = (long)rowId;
            _model[table][rowId] = value;
        }

        // Mostly live rows, sometimes deleted or never-used ids.
        private RowKey PickRowId(string table, SortedDictionary<RowKey, byte[]>? rows = null)
        {
            rows ??= _model[table];
            if (rows.Count > 0 && _random.Next(4) != 0)
            {
                return rows.Keys.ElementAt(_random.Next(rows.Count));
            }

            if (table == KeyedTable)
            {
                return _random.Next(5) switch
                {
                    0 => RowKey.FromGuid(new Guid(NextBytes(16))),
                    1 => -_random.NextInt64(1, long.MaxValue),
                    2 => _random.NextInt64(LsmLatestRowStore.DenseLimit, long.MaxValue),
                    3 => _random.Next(5) == 0 ? (_random.Next(2) == 0 ? long.MinValue : long.MaxValue) : 0,
                    _ => _random.Next(-50, 50), // small keys collide often: duplicates and re-inserts after deletes
                };
            }

            return 1 + _random.Next((int)Math.Max(1, _highestCommittedId[table] + 3));
        }

        private byte[] NextBytes(int count)
        {
            byte[] bytes = new byte[count];
            _random.NextBytes(bytes);
            return bytes;
        }

        // Mostly small rows, sometimes empty or a few kilobytes (several per block).
        private byte[] NewValue()
        {
            int length = _random.Next(10) switch
            {
                0 => 0,
                1 => 2_000 + _random.Next(6_000),
                _ => 1 + _random.Next(120),
            };

            byte[] value = new byte[length];
            _random.NextBytes(value);
            return value;
        }

        private void Log(string operation)
        {
            _recent.Enqueue($"#{_step} {operation}");
            if (_recent.Count > 30)
            {
                _recent.Dequeue();
            }
        }

        private void Check(bool condition, string message)
        {
            if (!condition)
            {
                var report = new StringBuilder()
                    .AppendLine($"seed {seed}, step {_step}, read mode {_readMode}: {message}")
                    .AppendLine("last operations:");
                foreach (string operation in _recent)
                {
                    report.AppendLine("  " + operation);
                }

                Assert.Fail(report.ToString());
            }
        }
    }
}
