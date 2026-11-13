using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Tests.Lsm;

/// <summary>
/// A damaged columnar segment in a table's SSTables, met through the storage engine and through SQL: the
/// covered aggregate must fail with <see cref="InvalidDataException"/> or give the rows' answer, never a wrong
/// one, and damage confined to the segment must leave the rows readable.
/// </summary>
public sealed partial class LsmOutOfCoreColumnarScanTests
{
    /// <summary>The part of a file's columnar segment a case damages.</summary>
    public enum DamagedPart
    {
        ChunkKeys,
        ChunkValues,
        NullMask,
        Tombstones,
        Directory,
        Footer,
    }

    public static TheoryData<LsmReadMode, DamagedPart> DamageCases()
    {
        var cases = new TheoryData<LsmReadMode, DamagedPart>();
        foreach (LsmReadMode mode in (LsmReadMode[])[LsmReadMode.MemoryMapped, LsmReadMode.PositionalRead])
        {
            foreach (DamagedPart part in Enum.GetValues<DamagedPart>())
            {
                cases.Add(mode, part);
            }
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(DamageCases))]
    public void ADamagedSegment_FailsTheScanLoudly_AndNeverAnswersWrongly(LsmReadMode mode, DamagedPart part)
    {
        // Three overlapping files: the newer ones update and delete rows of the older, so scans read tombstones.
        string clean = Path.Combine(_root, "clean");
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        var random = new Random(61);
        using (LsmStorageEngine engine = Open(mode, flushThreshold: 1L << 30, root: clean))
        {
            Build(engine);
            foreach (int operations in (int[])[9_000, 3_000, 2_000])
            {
                Mutate(engine, model, random, operations);
                engine.FlushTableForTesting(Db, Table);
            }
        }

        string[] files = TableFiles(clean);
        Assert.True(files.Length >= 3, $"expected at least 3 SSTables, found {files.Length}");
        int loud = 0;
        int damaged = 0;
        for (int file = 0; file < files.Length; file++)
        {
            byte[] bytes = File.ReadAllBytes(files[file]);
            SsTableDamage.Layout layout = SsTableDamage.LayoutOf(bytes);
            if (OffsetOf(layout, part) is not { } offset)
            {
                continue;
            }

            string root = Path.Combine(_root, $"case{file}");
            CopyDirectory(clean, root);
            File.WriteAllBytes(Path.Combine(root, Path.GetRelativePath(clean, files[file])), SsTableDamage.Flip(bytes, offset, 0x10));
            damaged++;
            loud += ScanDamaged(root, mode, part, model) ? 1 : 0;
        }

        Assert.True(damaged > 0, $"no file has a {part}");

        // A tombstone run is only read when an older file overlaps; every other part is read by a full scan.
        if (part == DamagedPart.Tombstones)
        {
            Assert.True(loud > 0, "no damaged tombstone run was noticed");
        }
        else
        {
            Assert.Equal(damaged, loud);
        }
    }

    // Opens a table with one damaged file and scans it. Returns whether the damage surfaced as InvalidDataException;
    // every scan that answers must give the model's answer.
    private bool ScanDamaged(string root, LsmReadMode mode, DamagedPart part, Dictionary<RowKey, (int Level, long? Gold)> model)
    {
        LsmStorageEngine? engine = null;
        bool loud = false;
        try
        {
            engine = Open(mode, flushThreshold: 1L << 30, root: root);
            Build(engine);
            foreach (int minLevel in (int[])[int.MinValue, 0, 50, 1_000_000])
            {
                if (TryScan(engine, minLevel, out Answer answer))
                {
                    Assert.Equal(Expected(model.Values, minLevel), answer);
                }
            }
        }
        catch (InvalidDataException)
        {
            loud = true;
        }

        try
        {
            // Damage inside the segment leaves the rows as they were: a query that reads rows still works.
            if (engine is not null && part is DamagedPart.ChunkKeys or DamagedPart.ChunkValues or DamagedPart.NullMask or DamagedPart.Tombstones)
            {
                AssertRowsReadAsModel(engine, model);
            }
        }
        finally
        {
            engine?.Dispose();
        }

        return loud;
    }

    private static long? OffsetOf(SsTableDamage.Layout layout, DamagedPart part) => part switch
    {
        DamagedPart.ChunkKeys => layout.Chunks[^1].Offset + 3,
        DamagedPart.ChunkValues => layout.Columns[0].ValuesOffset + 5,
        DamagedPart.NullMask => layout.Columns.FirstOrDefault(static c => c.NullsOffset != 0) is { NullsOffset: not 0 } column ? column.NullsOffset + 1 : null,
        DamagedPart.Tombstones => layout.Directory > layout.Tombstones ? layout.Tombstones + 2 : null,
        DamagedPart.Directory => layout.Directory + 70,
        _ => layout.Footer + (3 * SsTableV2Format.BlockHandleSize) + 4, // the columnar handle's offset
    };

    [Theory]
    [MemberData(nameof(Modes))]
    public void ACoveredSqlAggregate_OverADamagedChunk_IsAnError_AndRowsStillRead(LsmReadMode mode)
    {
        string root = Path.Combine(_root, "sql");
        const string Sum = "SELECT COUNT(*), SUM(Gold), MIN(Gold), MAX(Level) FROM T WHERE Level >= 0";
        const string Rows = "SELECT Id, Level, Gold FROM T";
        QueryResult expected;
        QueryResult rows;
        using (DataVoContext db = OpenSql(root, mode))
        {
            Sql(db, "CREATE DATABASE S");
            Sql(db, "USE S");
            Sql(db, "CREATE TABLE T (Id INT PRIMARY KEY, Level INT, Gold BIGINT)");
            Sql(db, "CREATE INDEX cx ON T (Level, Gold) USING COLUMNAR");
            for (int first = 1; first <= 9_000; first += 500)
            {
                IEnumerable<string> values = Enumerable.Range(first, 500).Select(static i => $"({i}, {(i * 37) % 100 - 20}, {(i % 9 == 0 ? "NULL" : (i * 7919L % 100_000).ToString())})");
                Sql(db, $"INSERT INTO T (Id, Level, Gold) VALUES {string.Join(", ", values)}");
            }

            expected = Sql(db, Sum);
            rows = Sql(db, Rows);
        }

        string file = Assert.Single(TableFiles(root, "T"));
        byte[] bytes = File.ReadAllBytes(file);
        SsTableDamage.Layout layout = SsTableDamage.LayoutOf(bytes);
        File.WriteAllBytes(file, SsTableDamage.Flip(bytes, layout.Columns[^1].ValuesOffset + 9, 0x04));

        using (DataVoContext db = OpenSql(root, mode))
        {
            Sql(db, "USE S");
            // The pushdown does not fall back to rows on damage: the statement fails and says why.
            QueryResult answer = db.Execute(Sum)[^1];
            Assert.True(answer.IsError, $"the damaged chunk gave {string.Join(",", answer.Data.SelectMany(static r => r.Values))}; the rows say {string.Join(",", expected.Data[0].Values)}");
            Assert.Contains(answer.Messages, static m => m.Contains("failed its checksum", StringComparison.Ordinal));

            // Rows are untouched by damage to the segment.
            QueryResult after = Sql(db, Rows);
            Assert.Equal(rows.Data.Count, after.Data.Count);
            Assert.Equal(rows.Data.Select(static r => string.Join(",", r.Values)), after.Data.Select(static r => string.Join(",", r.Values)));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ADamagedOrTruncatedSpecFile_IsDropped_AndTheNextBuildStoresItAndRewritesTheFiles(bool truncate)
    {
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        var random = new Random(71);
        LsmStorageEngine crashed = Open(LsmReadMode.Auto, flushThreshold: 1L << 30);
        Build(crashed);
        Mutate(crashed, model, random, 3_000);
        crashed.FlushTableForTesting(Db, Table);
        Mutate(crashed, model, random, 500); // left unflushed: replayed from the commit log on reopen
        crashed.SimulateCrashForTesting();

        string spec = Assert.Single(Directory.GetFiles(_root, "COLUMNAR", SearchOption.AllDirectories));
        byte[] bytes = File.ReadAllBytes(spec);
        File.WriteAllBytes(spec, truncate ? bytes.AsSpan(0, bytes.Length / 2).ToArray() : SsTableDamage.Flip(bytes, bytes.Length / 2, 0x01));

        using (LsmStorageEngine engine = Open(LsmReadMode.Auto, flushThreshold: 1L << 30))
        {
            // Without a spec the table writes and reads as a table that never had a columnar index.
            Assert.Null(engine.ColumnarSpecOf(Db, Table));
            engine.FlushTableForTesting(Db, Table);
            Assert.Contains(SstVersions(engine), static version => version == SsTableV2Format.WideVersion);
            AssertRowsReadAsModel(engine, model);

            // The next build stores the spec again and gives every file its segment.
            Build(engine);
            Assert.NotNull(engine.ColumnarSpecOf(Db, Table));
            Assert.All(SstVersions(engine), static version => Assert.Equal(SsTableV2Format.ColumnarVersion, version));
            AssertScans(engine, model);
        }

        using (LsmStorageEngine engine = Open(LsmReadMode.Auto, flushThreshold: 1L << 30))
        {
            Assert.NotNull(engine.ColumnarSpecOf(Db, Table));
            Build(engine);
            AssertScans(engine, model);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void FilesReplacedByCompactionAndBackfill_AreDeleted_AndTheirHandlesForgotten(LsmReadMode mode)
    {
        // Columnar scans hold the table's files while they read them; once a file is replaced (by compaction, or
        // by a backfill after the index was dropped and created again) and no scan holds it, it must be unmapped
        // and deleted (Windows refuses to delete a mapped file), and the registry must not keep its handle.
        var model = new Dictionary<RowKey, (int Level, long? Gold)>();
        var random = new Random(83);
        using LsmStorageEngine engine = Open(mode, flushThreshold: 1L << 30);
        Build(engine);
        for (int round = 0; round < 12; round++)
        {
            for (int file = 0; file < 3; file++)
            {
                Mutate(engine, model, random, 400);
                engine.FlushTableForTesting(Db, Table);
                AssertScans(engine, model);
            }

            if (round % 3 == 2)
            {
                engine.DropColumnar(Db, Table);
                Mutate(engine, model, random, 200);
                engine.FlushTableForTesting(Db, Table); // written without a segment
            }

            Build(engine); // rewrites any file that lacks its segment
            engine.CompactTable(Db, Table);
            AssertScans(engine, model);

            string[] onDisk = SstFiles(engine);
            Assert.Equal(onDisk.Length, engine.SstableHandleCountForTesting(Db, Table));
        }

        AssertRowsReadAsModel(engine, model);
    }

    private static DataVoContext OpenSql(string root, LsmReadMode mode) =>
        new(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = root, LsmReadMode = mode });

    private static QueryResult Sql(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    // The SSTables in the directory of one table, in file-number order.
    private static string[] TableFiles(string root, string table = Table) =>
        [.. Directory.GetFiles(root, "*.sst", SearchOption.AllDirectories)
            .Where(path => Path.GetExtension(path) == ".sst" && string.Equals(Path.GetFileName(Path.GetDirectoryName(path)), table, StringComparison.OrdinalIgnoreCase))
            .Order()];

    private static void CopyDirectory(string from, string to)
    {
        foreach (string directory in Directory.GetDirectories(from, "*", SearchOption.AllDirectories))
        {
            Directory.CreateDirectory(Path.Combine(to, Path.GetRelativePath(from, directory)));
        }

        Directory.CreateDirectory(to);
        foreach (string path in Directory.GetFiles(from, "*", SearchOption.AllDirectories))
        {
            File.Copy(path, Path.Combine(to, Path.GetRelativePath(from, path)));
        }
    }
}
