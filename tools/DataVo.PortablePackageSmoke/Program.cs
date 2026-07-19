using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Indexing.Flat;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;

internal static class Program
{
    private static int Main()
    {
        string candidate = RequiredEnvironment("DATAVO_PORTABLE_ASSEMBLY");
        candidate = Path.GetFullPath(candidate);
        Assembly loaded = AssemblyLoadContext.Default.LoadFromAssemblyPath(candidate);
        return PortableSmoke.Run(candidate, loaded);
    }

    private static string RequiredEnvironment(string name) =>
        Environment.GetEnvironmentVariable(name)
        ?? throw new InvalidOperationException($"Required environment variable {name} is not set.");
}

internal static class PortableSmoke
{
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static int Run(string candidatePath, Assembly explicitlyLoaded)
    {
        Assembly core = typeof(DataVoContext).Assembly;
        Require(ReferenceEquals(core, explicitlyLoaded), "Typed API bound to another DataVo.Core assembly.");
        Require(
            string.Equals(Path.GetFullPath(core.Location), candidatePath, StringComparison.Ordinal),
            $"Loaded DataVo.Core from '{core.Location}', expected '{candidatePath}'.");

        Type[] forbidden = core.GetExportedTypes()
            .Where(t => t.Namespace is string ns
                && (ns == "System" || ns.StartsWith("System.", StringComparison.Ordinal)))
            .ToArray();
        Require(forbidden.Length == 0,
            "Portable asset exports forbidden System types: "
            + string.Join(", ", forbidden.Select(t => t.FullName)));

        DataVoDate expectedDate = DataVoDate.FromDateTime(new DateTime(2026, 7, 19));
        CellValue dateCell = CellValue.From(expectedDate);
        Require(dateCell.AsDate() == expectedDate, "CellValue.AsDate did not round-trip DataVoDate.");
        Require(dateCell.ToObject() is DataVoDate boxed && boxed == expectedDate,
            "CellValue.ToObject did not materialize DataVoDate.");

        VerifyPortableHotPathFallbacks();

        using (var storage = new StorageContext(new DataVoConfig { StorageMode = StorageMode.InMemory }))
        {
            Require(storage.Config.StorageMode == StorageMode.InMemory,
                "StorageContext did not retain the in-memory configuration.");
        }

        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk(db, "CREATE DATABASE PortableSmoke");
        ExecuteOk(db, "USE PortableSmoke");
        ExecuteOk(db, "CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(32), Score INT)");
        ExecuteOk(db, "INSERT INTO Players VALUES (1, 'Alice', 10)");

        QueryResult selected = ExecuteOk(db, "SELECT Name FROM Players WHERE Id = 1").Last();
        Require(selected.Data.Count == 1
                && string.Equals(selected.Data[0]["Name"]?.ToString(), "Alice", StringComparison.Ordinal),
            "Public in-memory insert/select flow returned the wrong row.");

        QueryChange? change = null;
        using (db.Subscribe("SELECT Id, Score FROM Players WHERE Id = 1", value => change = value))
        {
            ExecuteOk(db, "UPDATE Players SET Score = 20 WHERE Id = 1");
            db.DispatchPendingNotifications();
        }
        Require(change is not null
                && change.Updated.Count == 1
                && Convert.ToInt64(change.Updated[0]["Score"]) == 20L,
            "Reactive query did not report the public UPDATE.");

        var flat = new FlatVectorIndex { Metric = "cosine" };
        flat.Insert(101, [1f, 0f]);
        flat.Insert(202, [0f, 1f]);
        Require(flat.SearchTopK([0.9f, 0.1f], 1).SequenceEqual([101L]),
            "Flat vector lookup returned the wrong nearest row.");

        string testRoot = RequiredEnvironment("DATAVO_PORTABLE_TEST_ROOT");
        ExpectDurableRejection(StorageMode.Disk, Path.Combine(testRoot, "disk"));
        ExpectDurableRejection(StorageMode.Lsm, Path.Combine(testRoot, "lsm"));

        Console.WriteLine("Portable candidate .NET-host smoke passed.");
        return 0;
    }

    private static void VerifyPortableHotPathFallbacks()
    {
        int[] lengths = [0, 1, 2, 3, 4, 7, 8, 9, 31, 32, 33, 4096];
        foreach (int length in lengths)
        {
            var payload = new byte[length];
            new Random(0x5A17).NextBytes(payload);
            Require(WalCrc32C.HashToUInt32(payload) == ReferenceCrc32C(payload),
                $"Portable CRC32C mismatch at length {length}.");
        }

        Require(DataVoCompiledQuery.BuildScalarComparisonKey(42) == "[42]",
            "Portable INT scalar-key formatting changed.");
        Require(DataVoCompiledQuery.BuildScalarComparisonKey(4_294_967_296L) == "[4294967296]",
            "Portable BIGINT scalar-key formatting changed.");
        Require(DataVoCompiledQuery.BuildScalarComparisonKey(12.5d) == "[12.5]",
            "Portable FLOAT scalar-key formatting changed.");
        Require(DataVoCompiledQuery.BuildScalarComparisonKey((object)12.5m) == "[12.5]",
            "Portable DECIMAL scalar-key formatting changed.");

        Guid expected = new("00112233-4455-6677-8899-aabbccddeeff");
        Require(CellValue.From(expected).AsGuid() == expected,
            "Portable GUID cell extraction changed.");
    }

    private static uint ReferenceCrc32C(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
        for (int i = 0; i < data.Length; i++)
        {
            crc ^= data[i];
            for (int bit = 0; bit < 8; bit++)
            {
                crc = (crc & 1u) != 0
                    ? (crc >> 1) ^ 0x82F63B78u
                    : crc >> 1;
            }
        }

        return ~crc;
    }

    private static List<QueryResult> ExecuteOk(DataVoContext db, string sql)
    {
        List<QueryResult> results = db.Execute(sql);
        QueryResult? failed = results.FirstOrDefault(result => result.IsError);
        if (failed is not null)
        {
            throw new InvalidOperationException($"SQL failed: {sql}: {string.Join("; ", failed.Messages)}");
        }
        return results;
    }

    private static void ExpectDurableRejection(StorageMode mode, string path)
    {
        Require(!Directory.Exists(path), $"Test path already exists: {path}");
        try
        {
            using var unexpected = new StorageContext(new DataVoConfig
            {
                StorageMode = mode,
                DiskStoragePath = path
            });
            throw new InvalidOperationException($"Portable {mode} construction unexpectedly succeeded.");
        }
        catch (PlatformNotSupportedException error)
        {
            Require(error.Message.Contains(mode.ToString(), StringComparison.Ordinal),
                $"{mode} rejection did not name the requested mode.");
            Require(error.Message.Contains("netstandard2.1", StringComparison.Ordinal),
                $"{mode} rejection did not name the experimental target.");
            Require(error.Message.Contains(nameof(StorageMode.InMemory), StringComparison.Ordinal),
                $"{mode} rejection did not name the supported candidate scope.");
        }
        Require(!Directory.Exists(path), $"Portable {mode} rejection created '{path}'.");
    }

    private static string RequiredEnvironment(string name) =>
        Environment.GetEnvironmentVariable(name)
        ?? throw new InvalidOperationException($"Required environment variable {name} is not set.");

    private static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}
