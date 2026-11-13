using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.CrashHarness;

// Usage: DataVo.CrashHarness <directory> <lsm|disk> <firstId> [rows|txn|columnar]
// Writes until killed. A line is printed only after the write call returned successfully:
//   INS <id> <value>   row <id> was inserted with <value>
//   UPD <id> <value>   row <id> was updated to <value>
//   TRY <id> <value>   an update of row <id> is about to start (diagnostic, not acknowledged)
//   TXN <id> 0         transaction <id> committed (txn workload)
// The columnar workload (see ColumnarCrashWorkload) prints four-part lines instead:
//   TRY <op> <a> <b>   a transaction is about to start (not acknowledged)
//   ACK <op> <a> <b>   that transaction committed
//   DIA <F|C> 0 0      a flush or compaction finished (diagnostic)
if (args.Length is not (3 or 4))
{
    Console.Error.WriteLine("usage: DataVo.CrashHarness <directory> <lsm|disk> <firstId> [rows|txn|columnar]");
    return 2;
}

string workload = args.Length == 4 ? args[3] : "rows";

string directory = args[0];
bool lsm = string.Equals(args[1], "lsm", StringComparison.OrdinalIgnoreCase);
long nextId = long.Parse(args[2], System.Globalization.CultureInfo.InvariantCulture);

var config = lsm
    ? new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = directory, LsmStrictFsync = true }
    : new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = directory, SyncDiskWrites = true };

using var db = new DataVoContext(config);
Require(db.Execute("USE CrashDb"));

TextWriter output = Console.Out;
Console.WriteLine("READY");
Console.Out.Flush();

if (workload == "columnar")
{
    RunColumnar(db, nextId, output);
}

if (workload == "txn")
{
    // Each transaction touches two tables and rewrites every Left row with one multi-row UPDATE, so a
    // partially recovered transaction or statement breaks an invariant the test can see.
    for (long id = nextId; ; id++)
    {
        Require(db.Execute("BEGIN"));
        Require(db.Execute($"INSERT INTO Lefts (Id, Gen) VALUES ({id}, {id})"));
        Require(db.Execute($"INSERT INTO Rights (Id) VALUES ({id})"));
        Require(db.Execute($"UPDATE Lefts SET Gen = {id} WHERE Id <= {id}"));
        Require(db.Execute("COMMIT"));
        output.WriteLine($"TXN {id} 0");
        output.Flush();
    }
}

var random = new Random(unchecked((int)nextId));
for (long id = nextId; ; id++)
{
    long value = id * 7;
    Require(db.Execute($"INSERT INTO Rows (Id, Value) VALUES ({id}, {value})"));
    output.WriteLine($"INS {id} {value}");
    output.Flush();

    // Periodically rewrite an earlier row, possibly one written before a previous crash, so recovery
    // is checked for updates too. Only an update that changed exactly one row is acknowledged: a row
    // whose insert was in flight during an earlier crash may legitimately not exist.
    if (id % 5 == 0)
    {
        long target = 1 + random.NextInt64(id);
        long updated = id * 1000 + 1;
        output.WriteLine($"TRY {target} {updated}"); // intent only, not an acknowledgement
        output.Flush();
        List<QueryResult> results = db.Execute($"UPDATE Rows SET Value = {updated} WHERE Id = {target}");
        Require(results);
        if (results.SelectMany(r => r.Messages).Contains("Rows affected: 1"))
        {
            output.WriteLine($"UPD {target} {updated}");
            output.Flush();
        }
    }
}

// Interleaved inserts, updates, deletes and multi-row statements against a table with a columnar index and an
// identical one without, each pair in one transaction; frequent flushes and compactions, so the columnar
// segments of many SSTables (and their rewrites) are on disk when the kill lands. Covered aggregates run
// in between, so the child also rebuilds the columnar tail and backfills files while it may be killed.
static void RunColumnar(DataVoContext db, long nextId, TextWriter output)
{
    const string Database = "CrashDb";
    string[] tables = [ColumnarCrashWorkload.IndexedTable, ColumnarCrashWorkload.PlainTable];
    LsmStorageEngine engine = db.Engine.StorageContext.LsmEngine
        ?? throw new InvalidOperationException("The columnar workload needs the LSM engine.");

    var live = new List<long>();
    foreach (Dictionary<string, object?> row in Single(db.Execute($"SELECT Id FROM {ColumnarCrashWorkload.IndexedTable}")).Data)
    {
        live.Add(Convert.ToInt64(row["Id"]));
    }

    var insert = tables.ToDictionary(t => t, t => db.Prepare($"INSERT INTO {t} (Id, Level, Gold, Score, Ratio, Tag) VALUES (@id, @level, @gold, @score, @ratio, @tag)"));
    var update = tables.ToDictionary(t => t, t => db.Prepare($"UPDATE {t} SET Level = @level, Gold = @gold, Score = @score, Ratio = @ratio, Tag = @tag WHERE Id = @id"));
    var delete = tables.ToDictionary(t => t, t => db.Prepare($"DELETE FROM {t} WHERE Id = @id"));
    var deleteRange = tables.ToDictionary(t => t, t => db.Prepare($"DELETE FROM {t} WHERE Id >= @low AND Id < @high"));
    var nullGold = tables.ToDictionary(t => t, t => db.Prepare($"UPDATE {t} SET Gold = NULL WHERE Id >= @low AND Id < @high"));

    var random = new Random(unchecked((int)(nextId * 7919)));
    for (long step = 1; ; step++)
    {
        int roll = random.Next(100);
        long generation = random.NextInt64(1, long.MaxValue);
        if (roll < 8)
        {
            // Hundreds of rows in one transaction, so files hold several 4,096-row chunk groups.
            long first = nextId;
            long count = 50 + random.Next(700);
            nextId += count;
            Commit(ColumnarCrashWorkload.BulkInsert, first, count, table =>
            {
                for (long id = first; id < first + count; id++)
                {
                    RequireOne(insert[table].Execute(CellParameters(id, first)));
                }
            });
            for (long id = first; id < first + count; id++)
            {
                live.Add(id);
            }
        }
        else if (live.Count == 0 || roll < 40)
        {
            long id = nextId++;
            Commit(ColumnarCrashWorkload.Insert, id, generation, table => RequireOne(insert[table].Execute(CellParameters(id, generation))));
            live.Add(id);
        }
        else if (roll < 65)
        {
            long id = live[random.Next(live.Count)];
            Commit(ColumnarCrashWorkload.Update, id, generation, table => RequireOne(update[table].Execute(CellParameters(id, generation))));
        }
        else if (roll < 80)
        {
            int at = random.Next(live.Count);
            long id = live[at];
            Commit(ColumnarCrashWorkload.Delete, id, 0, table => RequireOne(delete[table].Execute(new DataVoQueryParameter("id", id))));
            live[at] = live[^1];
            live.RemoveAt(live.Count - 1);
        }
        else if (roll < 90)
        {
            long low = live[random.Next(live.Count)];
            long width = 1 + random.Next(4);
            Commit(ColumnarCrashWorkload.DeleteRange, low, width, table => RequireResult(deleteRange[table].Execute(new DataVoQueryParameter("low", low), new DataVoQueryParameter("high", low + width))));
            live.RemoveAll(id => id >= low && id < low + width);
        }
        else
        {
            long low = live[random.Next(live.Count)];
            long width = 1 + random.Next(6);
            Commit(ColumnarCrashWorkload.NullGoldRange, low, width, table => RequireResult(nullGold[table].Execute(new DataVoQueryParameter("low", low), new DataVoQueryParameter("high", low + width))));
        }

        if (step % 7 == 0)
        {
            Require(db.Execute($"SELECT COUNT(*), SUM(Gold), MIN(Level), MAX(Level) FROM {ColumnarCrashWorkload.IndexedTable} WHERE Level >= 0"));
        }

        if (step % 10 == 0)
        {
            foreach (string table in tables)
            {
                engine.FlushTableForTesting(Database, table);
            }

            Diagnostic('F');
        }

        if (step % 45 == 0)
        {
            foreach (string table in tables)
            {
                engine.CompactTable(Database, table);
            }

            Diagnostic('C');
        }
    }

    void Commit(char op, long a, long b, Action<string> write)
    {
        output.WriteLine($"TRY {op} {a} {b}"); // intent only, not an acknowledgement
        output.Flush();
        Require(db.Execute("BEGIN"));
        foreach (string table in tables)
        {
            write(table);
        }

        Require(db.Execute("COMMIT"));
        output.WriteLine($"ACK {op} {a} {b}");
        output.Flush();
    }

    void Diagnostic(char what)
    {
        output.WriteLine($"DIA {what} 0 0");
        output.Flush();
    }
}

static DataVoQueryParameter[] CellParameters(long id, long generation)
{
    ColumnarCrashWorkload.Cells cells = ColumnarCrashWorkload.CellsOf(id, generation);
    return
    [
        new("id", id),
        new("level", cells.Level),
        cells.Gold is { } gold ? new("gold", gold) : DataVoQueryParameter.Null("gold"),
        cells.Score is { } score ? new("score", score) : DataVoQueryParameter.Null("score"),
        cells.Ratio is { } ratio ? new("ratio", ratio) : DataVoQueryParameter.Null("ratio"),
        new("tag", cells.Tag),
    ];
}

static QueryResult Single(List<QueryResult> results)
{
    Require(results);
    return results[^1];
}

static void RequireOne(QueryResult result)
{
    RequireResult(result);
    if (!result.Messages.Contains("Rows affected: 1"))
    {
        Console.Error.WriteLine("expected exactly one row affected: " + string.Join("; ", result.Messages));
        Environment.Exit(4);
    }
}

static void Require(List<QueryResult> results)
{
    foreach (QueryResult result in results)
    {
        RequireResult(result);
    }
}

static void RequireResult(QueryResult result)
{
    if (result.IsError)
    {
        Console.Error.WriteLine(string.Join("; ", result.Messages));
        Environment.Exit(3);
    }
}
