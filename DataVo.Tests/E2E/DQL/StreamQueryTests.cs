using System.Runtime.CompilerServices;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DQL;

/// <summary>StreamQuery must return exactly what SELECT returns, while holding only a small page in memory.</summary>
public sealed class StreamQueryTests : IDisposable
{
    private const int RowCount = 12_000;
    private static readonly string Padding = new('x', 180);

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_stream_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Disk, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void FullScan_MatchesSelect(StorageMode mode)
    {
        using DataVoContext db = Seed(mode, RowCount);
        AssertMatchesSelect(db, "SELECT * FROM Items", RowCount);
    }

    [Theory]
    [InlineData(StorageMode.InMemory, "SELECT Id, Grp FROM Items WHERE Grp > 5")]
    [InlineData(StorageMode.Disk, "SELECT Id, Grp FROM Items WHERE Grp > 5")]
    [InlineData(StorageMode.Lsm, "SELECT Id, Grp FROM Items WHERE Grp > 5")]
    [InlineData(StorageMode.Lsm, "SELECT Id FROM Items WHERE Id >= 3000 AND Id < 3100")]
    [InlineData(StorageMode.Lsm, "SELECT Id AS RowNo, Note AS Remark FROM Items WHERE Grp <= 2")]
    [InlineData(StorageMode.Lsm, "SELECT Grp, Id FROM Items")]
    public void FilteredAndProjectedScans_MatchSelect(StorageMode mode, string sql)
    {
        using DataVoContext db = Seed(mode, RowCount);
        AssertMatchesSelect(db, sql, expectedRows: null);
    }

    [Theory]
    [InlineData("SELECT Id, COUNT(*) AS C FROM Items GROUP BY Id LIMIT 5")]
    [InlineData("SELECT Id FROM Items ORDER BY Grp DESC, Id LIMIT 20")]
    [InlineData("SELECT DISTINCT Grp FROM Items")]
    [InlineData("SELECT Id FROM Items WHERE Id = 7")]
    [InlineData("SELECT 'x' AS Tag")]
    [InlineData("SELECT Id FROM Items UNION SELECT Grp FROM Items")]
    public void QueriesNeedingTheWholeResult_StillMatchSelect(string sql)
    {
        using DataVoContext db = Seed(StorageMode.Lsm, 2_000);
        AssertMatchesSelect(db, sql, expectedRows: null);
    }

    [Theory]
    [InlineData(0, 10)]
    [InlineData(2_500, 10)]
    [InlineData(11_990, 50)]
    public void LimitAndOffset_MatchSelectAcrossPages(int offset, int limit)
    {
        using DataVoContext db = Seed(StorageMode.Lsm, RowCount);
        AssertMatchesSelect(db, $"SELECT Id FROM Items LIMIT {limit} OFFSET {offset}", expectedRows: null);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Deletes_AreSkippedAcrossPages(StorageMode mode)
    {
        using DataVoContext db = Seed(mode, RowCount);
        Ok(db, "DELETE FROM Items WHERE Grp = 3");

        AssertMatchesSelect(db, "SELECT Id FROM Items", expectedRows: null);
    }

    [Fact]
    public void Parameters_AreBound()
    {
        using DataVoContext db = Seed(StorageMode.Lsm, 2_000);

        Assert.Equal(1_800, db.StreamQuery("SELECT Id FROM Items WHERE Grp > @min", ("min", 0)).Count());
    }

    [Fact]
    public void EnumeratingTwice_RunsTheScanAgain()
    {
        using DataVoContext db = Seed(StorageMode.Lsm, 3_000);
        IEnumerable<DataVoRow> rows = db.StreamQuery("SELECT Id FROM Items");

        Assert.Equal(3_000, rows.Count());
        Assert.Equal(3_000, rows.Count());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SlowReader_DoesNotBlockWritersBetweenRows(StorageMode mode)
    {
        using DataVoContext db = Seed(mode, 3_000);
        using IEnumerator<DataVoRow> rows = db.StreamQuery("SELECT Id FROM Items").GetEnumerator();
        Assert.True(rows.MoveNext());

        Task write = Task.Run(() => Ok(db, "UPDATE Items SET Grp = 77 WHERE Id = 2500"));

        Assert.True(write.Wait(TimeSpan.FromSeconds(20)), "A writer was blocked by an open stream.");
    }

    [Fact]
    public void Transaction_SeesItsOwnWrites()
    {
        using DataVoContext db = Seed(StorageMode.Lsm, 100);
        Ok(db, "BEGIN");
        Ok(db, "INSERT INTO Items (Id, Grp, Note) VALUES (5000, 1, 'tx')");

        Assert.Equal(101, db.StreamQuery("SELECT Id FROM Items").Count());
        Ok(db, "ROLLBACK");
    }

    [Theory]
    [InlineData(StorageMode.Disk)]
    [InlineData(StorageMode.InMemory)]
    public void Vacuum_DuringAStream_StopsItInsteadOfReturningWrongRows(StorageMode mode)
    {
        using DataVoContext db = Seed(mode, 3_000);
        Ok(db, "DELETE FROM Items WHERE Id = 5");
        using IEnumerator<DataVoRow> rows = db.StreamQuery("SELECT Id FROM Items").GetEnumerator();
        Assert.True(rows.MoveNext());

        Ok(db, "VACUUM Items");

        Assert.Throws<InvalidOperationException>(() =>
        {
            while (rows.MoveNext())
            {
            }
        });
    }

    [Fact]
    public void AlterTable_DuringAStream_StopsIt()
    {
        using DataVoContext db = Seed(StorageMode.Lsm, 3_000);
        using IEnumerator<DataVoRow> rows = db.StreamQuery("SELECT Id FROM Items").GetEnumerator();
        Assert.True(rows.MoveNext());

        Ok(db, "ALTER TABLE Items ADD COLUMN Extra INT");

        Assert.Throws<InvalidOperationException>(() =>
        {
            while (rows.MoveNext())
            {
            }
        });
    }

    [Theory]
    [InlineData("DELETE FROM Items")]
    [InlineData("SELECT Id FROM Items; SELECT Grp FROM Items")]
    public void AnythingButOneSelect_IsRejectedWithoutRunning(string sql)
    {
        using DataVoContext db = Seed(StorageMode.InMemory, 10);

        Assert.Throws<ArgumentException>(() => db.StreamQuery(sql));
        Assert.Equal(10, db.StreamQuery("SELECT Id FROM Items").Count());
    }

    [Fact]
    public void InvalidQuery_FailsAtTheCall()
    {
        using DataVoContext db = Seed(StorageMode.InMemory, 10);

        Assert.Throws<InvalidOperationException>(() => db.StreamQuery("SELECT Missing FROM Items"));
    }

    [Fact]
    public void Row_ReadsByNameAndPosition()
    {
        using DataVoContext db = Seed(StorageMode.InMemory, 1);

        DataVoRow row = db.StreamQuery("SELECT Id, Grp FROM Items").Single();

        Assert.Equal(1, row["id"]);
        Assert.Equal(row[1], row["Grp"]);
        Assert.Equal(["Id", "Grp"], row.Fields);
        Assert.Throws<KeyNotFoundException>(() => row["Nope"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Streaming_HoldsAboutOnePageOfRowsAtATime(StorageMode mode)
    {
        // Heap totals are too noisy for this: the same sawtooth appears with no DataVo code running. Weak
        // references answer the real question, which is how many rows the stream keeps alive.
        const int PageRows = 1024;
        const int SampleEvery = 8;
        using DataVoContext db = Seed(mode, RowCount);
        var tracked = new List<WeakReference>(RowCount / SampleEvery);
        int peakAlive = 0;
        int seen = 0;

        foreach (DataVoRow row in db.StreamQuery("SELECT Id, Grp, Note FROM Items"))
        {
            if (seen % SampleEvery == 0)
            {
                tracked.Add(Track(row));
            }

            if (++seen % 1_500 == 0)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                peakAlive = Math.Max(peakAlive, tracked.Count(reference => reference.IsAlive));
            }
        }

        Assert.Equal(RowCount, seen);
        Assert.Equal(RowCount / SampleEvery, tracked.Count);
        Assert.True(
            peakAlive <= (PageRows / SampleEvery) + 2,
            $"{peakAlive} of {tracked.Count} sampled rows were alive at once; one page is {PageRows / SampleEvery}.");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference Track(DataVoRow row) => new(row);

    private static void AssertMatchesSelect(DataVoContext db, string sql, int? expectedRows)
    {
        QueryResult selected = Ok(db, sql);
        List<DataVoRow> streamed = [.. db.StreamQuery(sql)];

        Assert.Equal(selected.Data.Count, streamed.Count);
        if (expectedRows is { } count)
        {
            Assert.Equal(count, streamed.Count);
        }

        if (streamed.Count > 0)
        {
            Assert.Equal(selected.Fields, streamed[0].Fields);
        }

        var left = selected.Data.Select(Describe).Order(StringComparer.Ordinal);
        var right = streamed.Select(row => Describe(row.ToDictionary())).Order(StringComparer.Ordinal);
        Assert.Equal(left, right);
    }

    private static string Describe(Dictionary<string, object?> row) =>
        string.Join("|", row.OrderBy(pair => pair.Key, StringComparer.Ordinal).Select(pair => $"{pair.Key}={pair.Value}"));

    private DataVoContext Seed(StorageMode mode, int rows)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
        });
        Ok(db, "CREATE DATABASE S");
        Ok(db, "USE S");
        Ok(db, "CREATE TABLE Items (Id INT PRIMARY KEY, Grp INT, Note VARCHAR(200))");
        db.BulkInsert("Items", Enumerable.Range(1, rows).Select(id => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?>
        {
            ["Id"] = id,
            ["Grp"] = id % 10,
            ["Note"] = $"{id}-{Padding}",
        }));
        return db;
    }

    private static QueryResult Ok(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
