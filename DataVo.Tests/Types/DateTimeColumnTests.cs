using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Types;

/// <summary>SQL DATETIME / TIMESTAMP columns on the InMemory engine.</summary>
public sealed class InMemoryDateTimeColumnTests() : DateTimeColumnTests(StorageMode.InMemory);

/// <summary>SQL DATETIME / TIMESTAMP columns on the LSM engine, including a reopen.</summary>
public sealed class LsmDateTimeColumnTests() : DateTimeColumnTests(StorageMode.Lsm);

/// <summary>
/// SQL DATETIME / TIMESTAMP: a point in time stored as 8-byte UTC ticks. Literals, parameters, typed inserts, filters,
/// ordering, aggregates and indexes all agree on the instant, to the tick.
/// </summary>
public abstract class DateTimeColumnTests : IDisposable
{
    private static readonly DateTime Base = new(2026, 11, 24, 10, 15, 0, DateTimeKind.Utc);
    private readonly StorageMode _mode;
    private readonly string? _root;
    private DataVoContext _db;

    protected DateTimeColumnTests(StorageMode mode)
    {
        _mode = mode;
        _root = mode == StorageMode.Lsm ? TestStorageDirectories.Create("datetime_columns") : null;
        _db = Open();
        Ok("CREATE DATABASE Times");
        Ok("USE Times");
        Ok("CREATE TABLE events (Id INT PRIMARY KEY, At DATETIME, Seen TIMESTAMP, Label VARCHAR(40))");
    }

    public void Dispose()
    {
        _db.Dispose();
        if (_root is not null)
        {
            try
            {
                Directory.Delete(_root, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    [Fact]
    public void TimestampIsAnAliasOfDatetime_AndTimeIsStillRejected()
    {
        var columns = _db.Engine.Catalog.GetTableColumns("events", "Times");
        Assert.Equal("DATETIME", columns.Single(c => c.Name == "At").Type, ignoreCase: true);
        Assert.Equal("DATETIME", columns.Single(c => c.Name == "Seen").Type, ignoreCase: true);

        QueryResult time = _db.Execute("CREATE TABLE bad (Id INT PRIMARY KEY, T TIME)").Last();
        Assert.True(time.IsError);
        Assert.Contains("DATETIME", string.Join(" ", time.Messages));
    }

    [Fact]
    public void IsoLiterals_WithOrWithoutAnOffset_StoreTheSameUtcInstant()
    {
        Ok("INSERT INTO events (Id, At, Label) VALUES (1, '2026-11-24 10:15:00', 'space')");
        Ok("INSERT INTO events (Id, At, Label) VALUES (2, '2026-11-24T10:15:00Z', 'zulu')");
        Ok("INSERT INTO events (Id, At, Label) VALUES (3, '2026-11-24T12:15:00+02:00', 'offset')");
        Ok("INSERT INTO events (Id, At, Label) VALUES (4, '2026-11-24T10:15:00.1234567Z', 'ticks')");

        List<Dictionary<string, object?>> rows = Rows("SELECT Id, At FROM events ORDER BY Id");
        Assert.All(rows.Take(3), row => Assert.Equal(Base, Assert.IsType<DateTime>(row["At"])));
        Assert.All(rows, row => Assert.Equal(DateTimeKind.Utc, ((DateTime)row["At"]!).Kind));
        Assert.Equal(Base.AddTicks(1_234_567), rows[3]["At"]);
    }

    [Fact]
    public void Filters_Ordering_AndAggregates_UseTheInstant()
    {
        for (int i = 1; i <= 100; i++)
        {
            Ok($"INSERT INTO events (Id, At, Label) VALUES ({i}, '{Base.AddMinutes(i):yyyy-MM-ddTHH:mm:ssZ}', 'e{i}')");
        }

        Assert.Equal(10, Rows("SELECT Id FROM events WHERE At > '2026-11-24T11:45:00Z'").Count);
        Assert.Equal(30, Rows("SELECT Id FROM events WHERE At >= '2026-11-24 10:16:00' AND At <= '2026-11-24T12:45:00+02:00'").Count);
        Assert.Single(Rows("SELECT Id FROM events WHERE At = '2026-11-24T11:00:00Z'"));

        List<Dictionary<string, object?>> latest = Rows("SELECT Id, At FROM events ORDER BY At DESC LIMIT 3");
        Assert.Equal([100, 99, 98], latest.Select(row => Convert.ToInt32(row["Id"])));

        Dictionary<string, object?> extremes = Assert.Single(Rows("SELECT MIN(At) AS First, MAX(At) AS Last, COUNT(At) AS N FROM events"));
        Assert.Equal(Base.AddMinutes(1), extremes["First"]);
        Assert.Equal(Base.AddMinutes(100), extremes["Last"]);
    }

    [Fact]
    public void PreparedParameters_AcceptDateTimeAndDateTimeOffset()
    {
        var local = new DateTimeOffset(2026, 11, 24, 12, 15, 0, TimeSpan.FromHours(2));
        DataVoPreparedStatement insert = _db.Prepare("INSERT INTO events (Id, At, Label) VALUES (@id, @at, @label)");
        Assert.False(insert.Execute(("id", 1), ("at", Base), ("label", "utc")).IsError);
        Assert.False(insert.Execute(("id", 2), ("at", local), ("label", "offset")).IsError);
        Assert.False(insert.Execute(("id", 3), ("at", new DateTime(2026, 11, 24, 10, 15, 0, DateTimeKind.Unspecified)), ("label", "unspecified")).IsError);

        DataVoPreparedStatement select = _db.Prepare("SELECT Id FROM events WHERE At = @at");
        QueryResult found = select.Execute(("at", local));
        Assert.False(found.IsError, string.Join(" | ", found.Messages));
        Assert.Equal(3, found.Data.Count);
    }

    [Fact]
    public void TypedInsert_RoundTripsCells()
    {
        var schema = new ReactiveRowSchema("Id", "At", "Seen", "Label");
        _db.InsertTyped("events", schema, [CellValue.From(1), CellValue.From(Base), CellValue.Null, CellValue.From("typed")]);
        _db.InsertTyped("events", schema, [CellValue.From(2), CellValue.From(new DateTimeOffset(Base).ToOffset(TimeSpan.FromHours(-5))), CellValue.From(Base.AddDays(1)), CellValue.From("offset")]);

        List<Dictionary<string, object?>> rows = Rows("SELECT Id, At, Seen FROM events ORDER BY Id");
        Assert.Equal(Base, rows[0]["At"]);
        Assert.Null(rows[0]["Seen"]);
        Assert.Equal(Base, rows[1]["At"]);
        Assert.Equal(Base.AddDays(1), rows[1]["Seen"]);
    }

    [Fact]
    public void SecondaryIndex_FindsAnInstantByAnySpelling()
    {
        Ok("CREATE INDEX idx_events_at ON events (At)");
        for (int i = 1; i <= 50; i++)
        {
            Ok($"INSERT INTO events (Id, At, Label) VALUES ({i}, '{Base.AddSeconds(i):yyyy-MM-ddTHH:mm:ssZ}', 'e{i}')");
        }

        Assert.Equal(7, Convert.ToInt32(Assert.Single(Rows("SELECT Id FROM events WHERE At = '2026-11-24T12:15:07+02:00'"))["Id"]));
        Dictionary<string, object?> plan = Assert.Single(Rows("EXPLAIN SELECT Id FROM events WHERE At = '2026-11-24 10:15:07'"));
        Assert.Equal("CompiledIndexLookup", plan["Operator"]);
    }

    [Fact]
    public void UpdateAndNull_Work()
    {
        Ok("INSERT INTO events (Id, At, Label) VALUES (1, '2026-01-01T00:00:00Z', 'a')");
        Ok("INSERT INTO events (Id, At, Label) VALUES (2, NULL, 'b')");
        Ok("UPDATE events SET At = '2027-06-30T23:59:59.9999999Z' WHERE Id = 1");

        Assert.Equal(new DateTime(2027, 6, 30, 23, 59, 59, DateTimeKind.Utc).AddTicks(9_999_999), Assert.Single(Rows("SELECT At FROM events WHERE Id = 1"))["At"]);
        Assert.Single(Rows("SELECT Id FROM events WHERE At IS NULL"));
    }

    [Fact]
    public void InvalidText_IsRejected()
    {
        QueryResult result = _db.Execute("INSERT INTO events (Id, At, Label) VALUES (1, 'not a time', 'x')").Last();
        Assert.True(result.IsError);
    }

    [Fact]
    public void ColumnarIndex_FiltersAndFoldsInstants_WithSimdKernels()
    {
        Ok("CREATE TABLE ticks (Id INT PRIMARY KEY, At DATETIME, Value INT)");
        Ok("CREATE INDEX cx_ticks ON ticks (At, Value) USING COLUMNAR");
        for (int start = 1; start <= 20_000; start += 2_000)
        {
            Ok("INSERT INTO ticks (Id, At, Value) VALUES "
                + string.Join(", ", Enumerable.Range(start, 2_000).Select(i => $"({i}, '{Base.AddSeconds(i):yyyy-MM-ddTHH:mm:ssZ}', {i % 7})")));
        }

        if (_mode == StorageMode.Lsm)
        {
            _db.Dispose(); // flush into columnar SSTables: the aggregate then reads segments and their zone maps
            _db = Open();
            Ok("USE Times");
        }

        string window = $"At >= '{Base.AddSeconds(19_001):yyyy-MM-ddTHH:mm:ssZ}'";
        Dictionary<string, object?> plan = Assert.Single(Rows($"EXPLAIN SELECT COUNT(*), MIN(At), MAX(At) FROM ticks WHERE {window}"));
        Assert.Equal("ColumnarSimdAggregate", plan["Operator"]);
        if (_mode == StorageMode.Lsm)
        {
            Assert.True((long)plan["SegmentsSkipped"]! > 0, $"skipped {plan["SegmentsSkipped"]}");
        }

        Dictionary<string, object?> result = Assert.Single(Rows($"SELECT COUNT(*) AS N, MIN(At) AS First, MAX(At) AS Last FROM ticks WHERE {window}"));
        Assert.Equal(1_000L, Convert.ToInt64(result["N"]));
        Assert.Equal(Base.AddSeconds(19_001), result["First"]);
        Assert.Equal(Base.AddSeconds(20_000), result["Last"]);
    }

    [Fact]
    public void Values_SurviveAReopen()
    {
        if (_mode != StorageMode.Lsm)
        {
            return; // InMemory has nothing to reopen
        }

        Ok("INSERT INTO events (Id, At, Label) VALUES (1, '2026-11-24T10:15:00.0000001Z', 'a')");
        _db.Dispose();
        _db = Open();
        Ok("USE Times");
        Assert.Equal(Base.AddTicks(1), Assert.Single(Rows("SELECT At FROM events"))["At"]);
    }

    private DataVoContext Open() => new(new DataVoConfig { StorageMode = _mode, DiskStoragePath = _root, LsmStrictFsync = false });

    private List<Dictionary<string, object?>> Rows(string sql)
    {
        QueryResult result = _db.Execute(sql).Last();
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        return result.Data;
    }

    private void Ok(string sql)
    {
        QueryResult result = _db.Execute(sql).Last();
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }
}
