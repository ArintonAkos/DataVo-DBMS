using DataVo.Core;
using System.Text;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Model-based randomized test of the LSM storage engine. A plain dictionary per table is the model of
/// what the engine must hold. Random operations (inserts, deletes, in-place overwrites, atomic batches
/// that commit or abort, flushes, compactions, clean reopens, and crashes) run against both, and after
/// every step reads, scans and counts must agree with the model. One table stores rows under keys the
/// test chooses, as a clustered primary key does: GUIDs and negative, small and huge integers. Another holds
/// real rows under a columnar index, and its covered aggregates (counts, sums, minimums and maximums under
/// random AND/OR conditions, with NULLs and NaNs, pruned by zone maps) must agree with the model too.
/// </summary>
/// <remarks>
/// A failure message names the seed and the last operations, so the exact run can be replayed. Set
/// DATAVO_MODEL_SEEDS (count) and DATAVO_MODEL_STEPS to run longer locally than in CI, and
/// DATAVO_SEED_OFFSET to start at another seed (the nightly job uses fresh seeds every run).
/// </remarks>
public sealed class LsmModelBasedTests : IDisposable
{
    private const string Database = "db";
    private static readonly string[] Tables = ["alpha", "beta", KeyedTable, ColumnarTable];

    /// <summary>The table whose rows are inserted under chosen keys instead of engine-assigned ids.</summary>
    private const string KeyedTable = "gamma";

    /// <summary>The table whose rows are rows of <see cref="ColumnarColumns"/>, with a columnar index on the first three.</summary>
    private const string ColumnarTable = "delta";

    private static readonly Column[] ColumnarColumns =
    [
        new() { Name = "Level", Type = "INT" },
        new() { Name = "Gold", Type = "BIGINT" },
        new() { Name = "Score", Type = "DOUBLE" },
        new() { Name = "Name", Type = "VARCHAR" },
    ];

    private static readonly RowSerializer.StorageColumnType[] ColumnarKinds = [.. ColumnarColumns.Select(static c => RowSerializer.ClassifyColumnType(c.Type))];

    // Count, then SUM(Level), SUM/MIN/MAX(Gold), MIN/MAX/COUNT(Score).
    private static readonly ColumnarFold[] ColumnarFolds =
    [
        new() { Slot = 0, Kind = RowFoldKind.SumInt32 },
        new() { Slot = 1, Kind = RowFoldKind.SumInt64 },
        new() { Slot = 1, Kind = RowFoldKind.MinInt64 },
        new() { Slot = 1, Kind = RowFoldKind.MaxInt64 },
        new() { Slot = 2, Kind = RowFoldKind.MinDouble },
        new() { Slot = 2, Kind = RowFoldKind.MaxDouble },
        new() { Slot = 2, Kind = RowFoldKind.None },
    ];

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

    /// <summary>The indexed cells of one row of the columnar table.</summary>
    private readonly record struct ColumnarRow(int Level, long? Gold, double? Score);

    private sealed class Run(string directory, int seed)
    {
        private readonly Random _random = new(seed);
        private readonly LsmReadMode _readMode = seed % 2 == 0 ? LsmReadMode.MemoryMapped : LsmReadMode.PositionalRead;
        private readonly Dictionary<string, SortedDictionary<RowKey, byte[]>> _model = Tables.ToDictionary(table => table, _ => new SortedDictionary<RowKey, byte[]>());
        private readonly Dictionary<string, long> _highestCommittedId = Tables.ToDictionary(table => table, _ => 0L);
        private readonly Queue<string> _recent = new();

        // The cells of every row value generated for the columnar table, by the array the model holds.
        private readonly Dictionary<byte[], ColumnarRow> _columnarRows = new(ReferenceEqualityComparer.Instance);
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
                case < 81:
                    VerifyPointReads(table);
                    if (table == ColumnarTable)
                    {
                        VerifyColumnar();
                    }

                    break;
                case < 82:
                    // As DROP INDEX: files flushed from now on lack the segment until a query builds it again.
                    Log("drop columnar");
                    _engine.DropColumnar(Database, ColumnarTable);
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

            // Sometimes a query asks for the columnar index right away; otherwise the next verification builds it.
            if (_random.Next(2) == 0)
            {
                Log("build columnar");
                BuildColumnar(engine);
            }

            return engine;
        }

        private static ColumnarIndex BuildColumnar(LsmStorageEngine engine) =>
            engine.BuildColumnar(Database, ColumnarTable, ColumnarKinds, [0, 1, 2], schemaVersion: 1, keyOrdinal: -1);

        private void InsertOne(string table)
        {
            if (table == KeyedTable)
            {
                InsertKeyed(table);
                return;
            }

            byte[] value = NewValue(table);
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

            List<byte[]> values = [.. Enumerable.Range(0, 1 + _random.Next(20)).Select(_ => NewValue(table))];
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
            byte[] value = NewValue(table);
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
            byte[] value = NewValue(table);
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
                            byte[] keyedValue = NewValue(table);
                            bool added = _engine.TryInsertRowWithKey(Database, table, key, keyedValue);
                            Check(added == !pending.ContainsKey(key), $"batch keyed insert of {key} returned {added}");
                            if (added)
                            {
                                pending[key] = keyedValue;
                                inserted.Add(key);
                            }

                            break;
                        case 0:
                            byte[] value = NewValue(table);
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
                            byte[] next = NewValue(table);
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
            VerifyColumnar();
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

        // Mostly small rows, sometimes empty or a few kilobytes (several per block); real rows for the columnar table.
        private byte[] NewValue(string table)
        {
            if (table == ColumnarTable)
            {
                return NewColumnarRow();
            }

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

        private byte[] NewColumnarRow()
        {
            var row = new ColumnarRow(
                _random.Next(-50, 150),
                _random.Next(6) == 0 ? null : _random.NextInt64(-1_000_000, 1_000_000),
                _random.Next(25) switch
                {
                    0 => double.NaN,
                    1 or 2 => null,
                    _ => Math.Round((_random.NextDouble() - 0.5) * 2_000, 2),
                });
            byte[] value = RowSerializer.SerializeCells(
                ColumnarColumns,
                [
                    CellValue.From(row.Level),
                    row.Gold is { } gold ? CellValue.From(gold) : CellValue.Null,
                    row.Score is { } score ? CellValue.From(score) : CellValue.Null,
                    CellValue.From(new string('n', _random.Next(1, 60))),
                ]);
            _columnarRows[value] = row;
            return value;
        }

        /// <summary>
        /// Runs a few covered scans of the columnar table under random conditions, as a query does (building the
        /// index when it is missing or asks for a rebuild), and compares each with the model's rows.
        /// </summary>
        private void VerifyColumnar()
        {
            List<ColumnarRow> rows = [.. _model[ColumnarTable].Values.Select(value => _columnarRows[value])];
            for (int query = 0; query < 3; query++)
            {
                (ColumnarStep[] steps, Func<ColumnarRow, bool> filter, string text) = RandomCondition();
                int limit = _random.Next(6) == 0 ? 1 + _random.Next(50) : 0;
                var totals = new RowAggregate[ColumnarFolds.Length];
                int matched = 0;
                bool answered = false;
                for (int attempt = 0; attempt < 3 && !answered; attempt++)
                {
                    ColumnarIndex index = _engine.Columnar(Database, ColumnarTable) is { WantsRebuild: false } current ? current : BuildColumnar(_engine);
                    Check(ColumnarScan.TryBind(index, steps), $"columnar steps of '{text}' do not bind");
                    answered = _engine.TryScanColumnar(Database, ColumnarTable, index, steps, ColumnarFolds, totals, limit, out matched);
                    if (!answered && !index.WantsRebuild && _engine.Columnar(Database, ColumnarTable) == index)
                    {
                        Log($"columnar scan '{text}' refused, retrying");
                    }
                }

                Check(answered, $"columnar scan '{text}' was not answered");
                List<ColumnarRow> kept = [.. rows.Where(filter)];
                Check(matched == (limit > 0 ? Math.Min(limit, kept.Count) : kept.Count), $"columnar scan '{text}' (limit {limit}) matched {matched}, the model {kept.Count}");
                if (limit == 0)
                {
                    CheckFolds(kept, totals, text);
                }
            }
        }

        private void CheckFolds(List<ColumnarRow> kept, RowAggregate[] totals, string text)
        {
            List<long> gold = [.. kept.Where(static r => r.Gold is not null).Select(static r => r.Gold!.Value)];
            List<double> scores = [.. kept.Where(static r => r.Score is not null).Select(static r => r.Score!.Value)];
            Check(totals[0].Values == kept.Count && totals[0].Whole == kept.Sum(static r => (long)r.Level), $"'{text}': SUM(Level) {totals[0].Whole} over {totals[0].Values}");
            Check(totals[1].Values == gold.Count && totals[1].Whole == gold.Sum(), $"'{text}': SUM(Gold) {totals[1].Whole} over {totals[1].Values}");
            Check(totals[2].Values == gold.Count && (gold.Count == 0 || totals[2].Whole == gold.Min()), $"'{text}': MIN(Gold) {totals[2].Whole}");
            Check(totals[3].Values == gold.Count && (gold.Count == 0 || totals[3].Whole == gold.Max()), $"'{text}': MAX(Gold) {totals[3].Whole}");
            Check(totals[4].Values == scores.Count && (scores.Count == 0 || SameBits(totals[4].Real, scores.Min())), $"'{text}': MIN(Score) {totals[4].Real}");
            Check(totals[5].Values == scores.Count && (scores.Count == 0 || SameBits(totals[5].Real, scores.Max())), $"'{text}': MAX(Score) {totals[5].Real}");
            Check(totals[6].Values == scores.Count, $"'{text}': COUNT(Score) {totals[6].Values}, the model {scores.Count}");
        }

        private static bool SameBits(double a, double b) => BitConverter.DoubleToInt64Bits(a) == BitConverter.DoubleToInt64Bits(b);

        // Up to three comparisons and NULL tests joined by AND/OR in postfix order, with the same test on model rows.
        private (ColumnarStep[] Steps, Func<ColumnarRow, bool> Filter, string Text) RandomCondition()
        {
            var steps = new List<ColumnarStep>();
            var filters = new Stack<(Func<ColumnarRow, bool> Filter, string Text)>();
            int terms = _random.Next(8) == 0 ? 0 : 1 + _random.Next(3);
            for (int term = 0; term < terms; term++)
            {
                steps.Add(RandomTerm(out Func<ColumnarRow, bool> filter, out string text));
                filters.Push((filter, text));
                if (term > 0)
                {
                    bool and = _random.Next(2) == 0;
                    steps.Add(new ColumnarStep { Kind = and ? ColumnarStep.And : ColumnarStep.Or });
                    (Func<ColumnarRow, bool> right, string rightText) = filters.Pop();
                    (Func<ColumnarRow, bool> left, string leftText) = filters.Pop();
                    filters.Push(and
                        ? (r => left(r) && right(r), $"({leftText} AND {rightText})")
                        : (r => left(r) || right(r), $"({leftText} OR {rightText})"));
                }
            }

            return filters.Count == 0 ? ([], static _ => true, "all rows") : ([.. steps], filters.Peek().Filter, filters.Peek().Text);
        }

        private ColumnarStep RandomTerm(out Func<ColumnarRow, bool> filter, out string text)
        {
            var test = (ColumnarTest)_random.Next(6);
            bool nullsMatch = _random.Next(4) == 0;
            switch (_random.Next(6))
            {
                case 0 or 1:
                    int level = _random.Next(-60, 160);
                    filter = r => Compare(r.Level, level, test);
                    text = $"Level {test} {level}";
                    return new ColumnarStep { Kind = ColumnarStep.Compare, Column = 0, Test = test, ValueKind = 'i', Bits = level, NullsMatch = nullsMatch };
                case 2:
                    long gold = _random.NextInt64(-1_100_000, 1_100_000);
                    filter = r => r.Gold is { } g ? Compare(g, gold, test) : nullsMatch;
                    text = $"Gold {test} {gold} (NULL {(nullsMatch ? "matches" : "does not")})";
                    return new ColumnarStep { Kind = ColumnarStep.Compare, Column = 1, Test = test, ValueKind = 'l', Bits = gold, NullsMatch = nullsMatch };
                case 3:
                    double score = _random.Next(10) == 0 ? double.NaN : Math.Round((_random.NextDouble() - 0.5) * 2_200, 2);
                    filter = r => r.Score is { } v ? Compare(v, score, test) : nullsMatch;
                    text = $"Score {test} {score} (NULL {(nullsMatch ? "matches" : "does not")})";
                    return new ColumnarStep { Kind = ColumnarStep.Compare, Column = 2, Test = test, ValueKind = 'd', Bits = BitConverter.DoubleToInt64Bits(score), NullsMatch = nullsMatch };
                default:
                    int column = _random.Next(1, 3);
                    bool isNull = _random.Next(2) == 0;
                    filter = column == 1 ? r => (r.Gold is null) == isNull : r => (r.Score is null) == isNull;
                    text = $"{(column == 1 ? "Gold" : "Score")} IS {(isNull ? "" : "NOT ")}NULL";
                    return new ColumnarStep { Kind = ColumnarStep.NullTest, Column = column, Test = isNull ? ColumnarTest.Equal : ColumnarTest.NotEqual };
            }
        }

        // IEEE comparison: a NaN meets every test but !=, as in the columnar kernels.
        private static bool Compare(double value, double operand, ColumnarTest test) => test switch
        {
            ColumnarTest.Equal => value == operand,
            ColumnarTest.NotEqual => value != operand,
            ColumnarTest.Less => value < operand,
            ColumnarTest.LessOrEqual => value <= operand,
            ColumnarTest.Greater => value > operand,
            _ => value >= operand,
        };

        private static bool Compare(long value, long operand, ColumnarTest test) => test switch
        {
            ColumnarTest.Equal => value == operand,
            ColumnarTest.NotEqual => value != operand,
            ColumnarTest.Less => value < operand,
            ColumnarTest.LessOrEqual => value <= operand,
            ColumnarTest.Greater => value > operand,
            _ => value >= operand,
        };

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
