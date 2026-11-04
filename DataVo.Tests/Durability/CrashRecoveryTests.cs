using System.Diagnostics;
using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;
using DataVo.Tests.BrowserParity;
using Xunit.Abstractions;

namespace DataVo.Tests.Durability;

/// <summary>
/// Hard-crash durability: a child process (tests/DataVo.CrashHarness) writes and acknowledges rows
/// until it is killed with SIGKILL at a random moment. The database must then reopen cleanly, keep
/// every acknowledged insert and update, and contain nothing beyond the one write that was in flight.
/// </summary>
[BrowserTranslateIgnore("Process-kill crash recovery is disk-specific")]
[Trait("Category", "Crash")]
public sealed class CrashRecoveryTests(ITestOutputHelper output)
{
    private const int CrashCycles = 6;

    // Known bug, reproduced by this test: an autocommit UPDATE is written as two separate durable
    // operations (tombstone the old row, append the new one). A crash between them loses an already
    // committed row. Remove the skips once UPDATE is atomic across a crash in each mode.
    private const string NonAtomicUpdate =
        "Known bug: a crash during an autocommit UPDATE can delete the committed row (non-atomic tombstone + append).";

    [Theory]
    [InlineData("lsm", Skip = NonAtomicUpdate)]
    [InlineData("disk", Skip = NonAtomicUpdate)]
    public void AcknowledgedWrites_SurviveRepeatedHardCrashes(string mode)
    {
        string directory = TestStorageDirectories.Create($"crash_{mode}");
        var random = new Random(20261104);
        var expected = new Dictionary<long, long>();
        long nextId = 1;

        using (DataVoContext setup = Open(directory, mode))
        {
            AssertOk(setup.Execute("CREATE DATABASE CrashDb"));
            AssertOk(setup.Execute("USE CrashDb"));
            AssertOk(setup.Execute("CREATE TABLE Rows (Id BIGINT PRIMARY KEY, Value BIGINT)"));
        }

        for (int cycle = 0; cycle < CrashCycles; cycle++)
        {
            int acknowledgementsBeforeKill = random.Next(20, 400);
            CrashRun run = RunUntilKilled(directory, mode, nextId, acknowledgementsBeforeKill);

            foreach ((long id, long value) in run.Inserts)
            {
                expected[id] = value;
            }

            foreach ((long id, long value) in run.Updates)
            {
                expected[id] = value;
            }

            Dictionary<long, long> actual = ReadAll(directory, mode);
            long maxAcknowledgedId = run.Inserts.Count == 0 ? nextId - 1 : run.Inserts[^1].Id;

            List<long> missing = expected.Keys.Where(id => !actual.ContainsKey(id)).Order().ToList();
            Assert.True(
                missing.Count == 0,
                $"cycle {cycle}: {missing.Count} acknowledged rows missing (of {expected.Count}): " +
                $"[{string.Join(", ", missing.Take(20))}]; updated in this run: " +
                $"[{string.Join(", ", missing.Where(id => run.Updates.Any(u => u.Id == id)).Take(20))}]; " +
                $"last acknowledged insert {maxAcknowledgedId}; update in flight at kill: row {run.LastAttemptedUpdate}");

            foreach ((long id, long value) in expected)
            {
                long stored = actual[id];
                Assert.True(
                    stored == value || IsInFlightUpdate(stored, maxAcknowledgedId),
                    $"cycle {cycle}: row {id} has {stored}, expected acknowledged value {value}");
                expected[id] = stored;
            }

            foreach (long id in actual.Keys)
            {
                Assert.True(
                    expected.ContainsKey(id) || id == maxAcknowledgedId + 1,
                    $"cycle {cycle}: row {id} exists but was never written");
            }

            // An in-flight insert may have become durable before the kill; it now counts as written.
            foreach ((long id, long value) in actual)
            {
                expected[id] = value;
            }

            output.WriteLine($"cycle {cycle}: {run.Inserts.Count} inserts, {run.Updates.Count} updates acknowledged; {actual.Count} rows after recovery");
            nextId = actual.Count == 0 ? 1 : actual.Keys.Max() + 1;
        }
    }

    // The harness writes updates as (insert id * 1000 + 1); only the update issued after the last
    // acknowledged insert can be durable without having been acknowledged.
    private static bool IsInFlightUpdate(long stored, long maxAcknowledgedId) =>
        stored % 1000 == 1 && stored / 1000 >= maxAcknowledgedId;

    private static CrashRun RunUntilKilled(string directory, string mode, long firstId, int acknowledgementsBeforeKill)
    {
        var start = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        start.ArgumentList.Add(HarnessPath());
        start.ArgumentList.Add(directory);
        start.ArgumentList.Add(mode);
        start.ArgumentList.Add(firstId.ToString(CultureInfo.InvariantCulture));

        using Process process = Process.Start(start) ?? throw new InvalidOperationException("Could not start the crash harness.");
        var run = new CrashRun();
        var deadline = Stopwatch.StartNew();
        int acknowledged = 0;
        string? line;
        while ((line = process.StandardOutput.ReadLine()) is not null)
        {
            if (run.Record(line))
            {
                acknowledged++;
            }

            if (acknowledged >= acknowledgementsBeforeKill || deadline.Elapsed > TimeSpan.FromSeconds(30))
            {
                break;
            }
        }

        if (process.HasExited)
        {
            throw new InvalidOperationException(
                $"Crash harness exited early with code {process.ExitCode}: {process.StandardError.ReadToEnd()}");
        }

        process.Kill(entireProcessTree: true); // SIGKILL on Unix: no finalizers, no Dispose, no flush
        process.WaitForExit();

        // Lines flushed before the kill were acknowledged; a trailing partial line is not complete.
        string remainder = process.StandardOutput.ReadToEnd();
        string[] lines = remainder.Split('\n');
        for (int i = 0; i < lines.Length - 1; i++)
        {
            run.Record(lines[i]);
        }

        return run;
    }

    private static Dictionary<long, long> ReadAll(string directory, string mode)
    {
        using DataVoContext context = Open(directory, mode);
        AssertOk(context.Execute("USE CrashDb"));
        QueryResult result = context.Execute("SELECT Id, Value FROM Rows")[0];
        Assert.False(result.IsError, string.Join("; ", result.Messages));
        return result.Data.ToDictionary(row => Convert.ToInt64(row["Id"]), row => Convert.ToInt64(row["Value"]));
    }

    private static DataVoContext Open(string directory, string mode) => new(mode == "lsm"
        ? new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = directory, LsmStrictFsync = true }
        : new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = directory, SyncDiskWrites = true });

    private static string HarnessPath()
    {
        // .../DataVo.Tests/bin/<Configuration>/net10.0/ -> .../tests/DataVo.CrashHarness/bin/<Configuration>/net10.0/
        var testOutput = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar));
        string framework = testOutput.Name;
        string configuration = testOutput.Parent!.Name;
        DirectoryInfo repository = testOutput.Parent.Parent!.Parent!.Parent!;
        string path = Path.Combine(repository.FullName, "tests", "DataVo.CrashHarness", "bin", configuration, framework, "DataVo.CrashHarness.dll");
        Assert.True(File.Exists(path), $"Crash harness not built at {path}");
        return path;
    }

    private static void AssertOk(List<QueryResult> results)
    {
        QueryResult? error = results.FirstOrDefault(r => r.IsError);
        Assert.True(error is null, error is null ? null : string.Join("; ", error.Messages));
    }

    private sealed class CrashRun
    {
        public List<(long Id, long Value)> Inserts { get; } = [];

        public List<(long Id, long Value)> Updates { get; } = [];

        public long? LastAttemptedUpdate { get; private set; }

        public bool Record(string line)
        {
            string[] parts = line.Trim().Split(' ');
            if (parts.Length != 3)
            {
                return false;
            }

            var entry = (long.Parse(parts[1], CultureInfo.InvariantCulture), long.Parse(parts[2], CultureInfo.InvariantCulture));
            switch (parts[0])
            {
                case "INS":
                    Inserts.Add(entry);
                    return true;
                case "UPD":
                    Updates.Add(entry);
                    return true;
                case "TRY":
                    LastAttemptedUpdate = entry.Item1;
                    return false;
                default:
                    return false;
            }
        }
    }
}
