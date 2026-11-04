using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

// Usage: DataVo.CrashHarness <directory> <lsm|disk> <firstId> [rows|txn]
// Writes until killed. A line is printed only after the write call returned successfully:
//   INS <id> <value>   row <id> was inserted with <value>
//   UPD <id> <value>   row <id> was updated to <value>
//   TRY <id> <value>   an update of row <id> is about to start (diagnostic, not acknowledged)
//   TXN <id> 0         transaction <id> committed (txn workload)
if (args.Length is not (3 or 4))
{
    Console.Error.WriteLine("usage: DataVo.CrashHarness <directory> <lsm|disk> <firstId> [rows|txn]");
    return 2;
}

bool transactionWorkload = args.Length == 4 && args[3] == "txn";

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

if (transactionWorkload)
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

static void Require(List<QueryResult> results)
{
    QueryResult? error = results.FirstOrDefault(r => r.IsError);
    if (error is not null)
    {
        Console.Error.WriteLine(string.Join("; ", error.Messages));
        Environment.Exit(3);
    }
}
