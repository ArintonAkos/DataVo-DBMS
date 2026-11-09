using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using Microsoft.Win32.SafeHandles;
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Compat;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Indexing;
using DataVo.Core.Indexing.Flat;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Runtime.Security;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Disk;
using DataVo.Core.StorageEngine.Lsm;
using DataVo.Core.Transactions;

internal static class Program
{
    private static int Main(string[] args)
    {
        string candidate = RequiredEnvironment("DATAVO_PORTABLE_ASSEMBLY");
        candidate = Path.GetFullPath(candidate);
        Assembly loaded = AssemblyLoadContext.Default.LoadFromAssemblyPath(candidate);
        if (args is ["crash-writer", ..])
        {
            PortableSmoke.WriteThenCrash(args);
        }

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

        string testRoot = RequiredEnvironment("DATAVO_PORTABLE_TEST_ROOT");
        VerifyPortableCompatibilitySeams();

        DataVoDate expectedDate = DataVoDate.FromDateTime(new DateTime(2026, 7, 19));
        CellValue dateCell = CellValue.From(expectedDate);
        Require(dateCell.AsDate() == expectedDate, "CellValue.AsDate did not round-trip DataVoDate.");
        Require(dateCell.ToObject() is DataVoDate boxed && boxed == expectedDate,
            "CellValue.ToObject did not materialize DataVoDate.");

        VerifyPortableHotPathFallbacks();
        VerifyPortableLibraryFallbacks();

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

        VerifyPersistence(StorageMode.Lsm, Path.Combine(testRoot, "lsm"));
        VerifyPersistence(StorageMode.Disk, Path.Combine(testRoot, "disk"));
        VerifyCommittedRowsSurviveProcessCrash(StorageMode.Lsm, Path.Combine(testRoot, "lsm-crash"));

        Console.WriteLine("Portable candidate .NET-host smoke passed.");
        VerifyPreparedStatementsAndNameCase();
        return 0;
    }

    private const int CrashTestRows = 500;

    private static DataVoContext OpenSaveData(StorageMode mode, string path) =>
        new(new DataVoConfig { StorageMode = mode, DiskStoragePath = path });

    private static void VerifyPersistence(StorageMode mode, string path)
    {
        using (DataVoContext writer = OpenSaveData(mode, path))
        {
            ExecuteOk(writer, "CREATE DATABASE Save");
            ExecuteOk(writer, "USE save");
            ExecuteOk(writer, "CREATE TABLE Players (Id INT PRIMARY KEY, Hp INT, Name VARCHAR(30))");
            ExecuteOk(writer, "CREATE INDEX ix_hp ON Players (Hp)");
            for (int id = 1; id <= 5000; id++)
            {
                ExecuteOk(writer, $"INSERT INTO Players (Id, Hp, Name) VALUES ({id}, {id % 50}, 'n{id}')");
            }

            ExecuteOk(writer, "UPDATE Players SET Hp = 777 WHERE Id = 1");
            ExecuteOk(writer, "DELETE FROM Players WHERE Id = 2");
            ExecuteOk(writer, "BEGIN");
            ExecuteOk(writer, "INSERT INTO Players (Id, Hp, Name) VALUES (6000, 1, 'tx')");
            ExecuteOk(writer, "COMMIT");
        }

        using DataVoContext reader = OpenSaveData(mode, path);
        ExecuteOk(reader, "USE Save");
        Require(ExecuteOk(reader, "SELECT Id FROM Players")[^1].Data.Count == 5000, $"{mode}: wrong row count after reopen.");
        Require(Equals(ExecuteOk(reader, "SELECT Hp FROM Players WHERE Id = 1")[^1].Data.Single()["Hp"], 777), $"{mode}: update lost.");
        Require(ExecuteOk(reader, "SELECT Id FROM Players WHERE Id = 2")[^1].Data.Count == 0, $"{mode}: delete lost.");
        Require(ExecuteOk(reader, "SELECT Id FROM Players WHERE Id = 6000")[^1].Data.Count == 1, $"{mode}: transaction lost.");
        Require(ExecuteOk(reader, "SELECT Id FROM Players WHERE Hp = 3")[^1].Data.Count == 100, $"{mode}: index wrong after reopen.");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void WriteThenCrash(string[] args)
    {
        StorageMode mode = Enum.Parse<StorageMode>(args[1]);
        string path = args[2];
        int rows = int.Parse(args[3]);
        DataVoContext writer = OpenSaveData(mode, path);
        ExecuteOk(writer, "CREATE DATABASE Save");
        ExecuteOk(writer, "USE Save");
        ExecuteOk(writer, "CREATE TABLE Players (Id INT PRIMARY KEY, Hp INT)");
        for (int id = 1; id <= rows; id++)
        {
            ExecuteOk(writer, $"INSERT INTO Players (Id, Hp) VALUES ({id}, {id * 2})");
        }

        Environment.FailFast("Simulated crash after committed writes.");
    }

    private static void VerifyCommittedRowsSurviveProcessCrash(StorageMode mode, string path)
    {
        var start = new System.Diagnostics.ProcessStartInfo("dotnet")
        {
            RedirectStandardError = true,
            RedirectStandardOutput = true,
        };
        foreach (string argument in new[] { Assembly.GetEntryAssembly()!.Location, "crash-writer", mode.ToString(), path, CrashTestRows.ToString() })
        {
            start.ArgumentList.Add(argument);
        }

        using var process = System.Diagnostics.Process.Start(start)!;
        process.StandardOutput.ReadToEndAsync();
        process.StandardError.ReadToEnd();
        process.WaitForExit();
        Require(process.ExitCode != 0, "The crash writer was expected to crash.");

        using DataVoContext reader = OpenSaveData(mode, path);
        ExecuteOk(reader, "USE Save");
        int rows = ExecuteOk(reader, "SELECT Id FROM Players")[^1].Data.Count;
        Require(rows == CrashTestRows, $"{mode}: {rows} of {CrashTestRows} committed rows survived a process crash.");
    }

    private static void VerifyPreparedStatementsAndNameCase()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        context.Execute("CREATE DATABASE PreparedSmoke");
        context.Execute("USE preparedsmoke");
        context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Qty INT)");
        context.Execute("INSERT INTO ITEMS (Id, Qty) VALUES (1, 10)");

        DataVoPreparedStatement select = context.Prepare("SELECT Qty FROM items WHERE Id = @id");
        Require(Equals(select.Execute(("id", 1)).Data.Single()["Qty"], 10), "Prepared SELECT returned the wrong row.");

        QueryResult borrowed = select.ExecuteBorrowed(new DataVoQueryParameter("id", 1));
        Require(!borrowed.IsError && borrowed.Data.Count == 1, "ExecuteBorrowed returned the wrong row.");

        DataVoPreparedStatement update = context.Prepare("UPDATE iTeMs SET Qty = @qty WHERE Id = @id");
        Require(!update.Execute(("qty", 11), ("id", 1)).IsError, "Prepared UPDATE failed.");
        Require(Equals(select.Execute(("id", 1)).Data.Single()["Qty"], 11), "Prepared UPDATE did not reach the row.");
    }

    private static void VerifyPortableCompatibilitySeams()
    {
        VerifyPortableGuards();
        VerifyPortableCancellationWaiter();
        VerifyPortableMonotonicClock();
        VerifyPortablePriorityQueue();
    }

    private static void VerifyPortableGuards()
    {
        object? value = null;
        ArgumentNullException nullError = Expect<ArgumentNullException>(
            () => ThrowHelper.ThrowIfNull(value),
            "Portable ThrowIfNull accepted null.");
        Require(nullError.ParamName == nameof(value),
            $"Portable ThrowIfNull caller name was '{nullError.ParamName}'.");

        string? path = null;
        ArgumentNullException nullWhitespaceError = Expect<ArgumentNullException>(
            () => ThrowHelper.ThrowIfNullOrWhiteSpace(path),
            "Portable ThrowIfNullOrWhiteSpace accepted null.");
        Require(nullWhitespaceError.ParamName == nameof(path),
            $"Portable null/whitespace guard caller name was '{nullWhitespaceError.ParamName}'.");

        string whitespace = " \t";
        ArgumentException whitespaceError = Expect<ArgumentException>(
            () => ThrowHelper.ThrowIfNullOrWhiteSpace(whitespace),
            "Portable ThrowIfNullOrWhiteSpace accepted whitespace.");
        Require(whitespaceError.GetType() == typeof(ArgumentException),
            $"Portable whitespace guard threw '{whitespaceError.GetType().FullName}'.");
        Require(whitespaceError.ParamName == nameof(whitespace),
            $"Portable whitespace guard caller name was '{whitespaceError.ParamName}'.");

        long rowId = -4_294_967_296L;
        ArgumentOutOfRangeException negativeError = Expect<ArgumentOutOfRangeException>(
            () => ThrowHelper.ThrowIfNegative(rowId),
            "Portable ThrowIfNegative accepted a negative value.");
        Require(negativeError.ParamName == nameof(rowId),
            $"Portable negative guard caller name was '{negativeError.ParamName}'.");
        Require(negativeError.ActualValue is long actual && actual == rowId,
            $"Portable negative guard actual value was '{negativeError.ActualValue}'.");

        var marker = new PortableDisposedMarker();
        ObjectDisposedException disposedError = Expect<ObjectDisposedException>(
            () => ThrowHelper.ThrowIfDisposed(true, marker),
            "Portable disposed guard accepted a disposed instance.");
        Require(disposedError.ObjectName == typeof(PortableDisposedMarker).FullName,
            $"Portable disposed guard object name was '{disposedError.ObjectName}'.");
    }

    private static void VerifyPortableCancellationWaiter()
    {
        TaskCancellationWaiter.WaitAsync(Task.CompletedTask, CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        var completion = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        Task wait = TaskCancellationWaiter.WaitAsync(completion.Task, cancellation.Token);
        cancellation.Cancel();

        OperationCanceledException cancellationError = Expect<OperationCanceledException>(
            () => wait.GetAwaiter().GetResult(),
            "Portable cancellation waiter ignored cancellation.");
        Require(cancellationError.CancellationToken == cancellation.Token,
            "Portable cancellation waiter did not preserve the cancellation token.");
        Require(!completion.Task.IsCompleted,
            "Portable cancellation waiter completed the underlying task.");

        completion.SetResult(null);
        completion.Task.GetAwaiter().GetResult();
    }

    private static void VerifyPortableMonotonicClock()
    {
        long before = MonotonicClock.GetMilliseconds();
        Thread.Sleep(2);
        long after = MonotonicClock.GetMilliseconds();
        Require(after >= before,
            $"Portable monotonic clock moved backward ({before} -> {after}).");
    }

    private static void VerifyPortablePriorityQueue()
    {
        var queue = new DataVoPriorityQueue<string, int>();
        queue.Enqueue("later", 20);
        queue.Enqueue("first", 10);

        Require(queue.TryPeek(out string? peeked, out int priority)
                && peeked == "first"
                && priority == 10,
            "Portable priority queue returned the wrong minimum.");
        Require(queue.Dequeue() == "first" && queue.Dequeue() == "later" && queue.Count == 0,
            "Portable priority queue dequeued elements out of order.");
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

    private static void VerifyPortableLibraryFallbacks()
    {
        VerifyPortablePasswordAndPermissions();
        VerifyPortableAggregateMaintenance();
        VerifyPortableIntegerScanAndTypedBatch();
        VerifyPortableGuidEquality();
    }

    private static void VerifyPortablePasswordAndPermissions()
    {
        const string password = "DataVo-stage2b!";
        const string persistedHash = "rb/gy2xJgDHSNv4ThGEcdnh+Dlu8dEVmWN+qVF3It4w=";
        const string persistedSalt = "AAECAwQFBgcICQoLDA0ODw==";
        const string minimumLegacySaltHash = "+p95m+Nw04sUo7SfCTGGVw9SMOFDGE7cFyFRQsXf3k8=";
        const string minimumLegacySalt = "AAECAwQFBgc=";

        Require(PasswordHasher.Verify(password, persistedHash, persistedSalt),
            "Portable PBKDF2 fallback rejected the fixed modern password vector.");
        Require(!PasswordHasher.Verify("wrong-password", persistedHash, persistedSalt),
            "Portable PBKDF2 fallback accepted the wrong password.");
        Require(!PasswordHasher.Verify(password, persistedHash, "AA=="),
            "Portable password verification accepted a decoded salt shorter than eight bytes.");
        Require(PasswordHasher.Verify(password, minimumLegacySaltHash, minimumLegacySalt),
            "Portable password verification rejected the eight-byte compatibility boundary.");

        (string generatedHash, string generatedSalt) = PasswordHasher.HashPassword("portable-generated-password");
        Require(Convert.FromBase64String(generatedHash).Length == 32,
            "Portable password hashing changed the derived-key size.");
        Require(Convert.FromBase64String(generatedSalt).Length == 16,
            "Portable password hashing changed the salt size.");
        Require(PasswordHasher.Verify("portable-generated-password", generatedHash, generatedSalt),
            "Portable password hashing could not verify its generated hash.");

        using var secured = new DataVoContext(new DataVoConfig
        {
            StorageMode = StorageMode.InMemory,
            EnableAuthorization = true,
            AllowAnonymousSession = false,
            AuthorizationUsers =
            [
                new DataVoAuthUser
                {
                    Username = "admin",
                    PasswordHash = persistedHash,
                    PasswordSalt = persistedSalt,
                    Role = DatabaseRole.Admin
                }
            ]
        });
        Require(secured.Login("admin", password),
            "Portable session initialization rejected persisted credentials.");
        SessionPrincipal principal = secured.Engine.SessionSecurity.Get(secured.SessionId)
            ?? throw new InvalidOperationException("Portable login did not create a session principal.");
        Require(Enum.GetValues<DatabasePermission>()
                .All(permission => principal.EffectivePermissions.Contains(permission)),
            "Portable session initialization omitted an admin permission.");
        ExecuteOk(secured, "CREATE DATABASE PortableSecurity");
    }

    private static void VerifyPortableAggregateMaintenance()
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk(db, "CREATE DATABASE PortableAggregate");
        ExecuteOk(db, "USE PortableAggregate");
        ExecuteOk(db, "CREATE TABLE Sales (Id INT PRIMARY KEY, GroupId INT, Amount INT)");

        QueryChange? change = null;
        using IDisposable subscription = db.Subscribe(
            "SELECT GroupId, SUM(Amount) AS Total, COUNT(*) AS Cnt FROM Sales GROUP BY GroupId",
            value => change = value);

        ExecuteOk(db, "INSERT INTO Sales VALUES (1, 7, 10)");
        db.DispatchPendingNotifications();
        change = null;

        db.InsertTyped(
            "Sales",
            new ReactiveRowSchema("Id", "GroupId", "Amount"),
            [CellValue.From(2), CellValue.From(7), CellValue.From(20)]);
        db.DispatchPendingNotifications();

        Require(change is not null && change.Updated.Count == 1,
            "Portable aggregate maintenance did not update the existing numeric group.");
        Require(Convert.ToInt64(change!.Updated[0]["GroupId"]) == 7L
                && Convert.ToInt64(change.Updated[0]["Total"]) == 30L
                && Convert.ToInt64(change.Updated[0]["Cnt"]) == 2L,
            "Portable aggregate maintenance returned the wrong derived row.");
    }

    private static void VerifyPortableIntegerScanAndTypedBatch()
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk(db, "CREATE DATABASE PortableCompiled");
        ExecuteOk(db, "USE PortableCompiled");
        ExecuteOk(db, "CREATE TABLE Levels (Id INT PRIMARY KEY, Level INT, Name VARCHAR(20))");

        var schema = new ReactiveRowSchema("Id", "Level", "Name");
        IReadOnlyList<RowKey> rowIds = db.InsertTypedBatch(
            "Levels",
            schema,
            [
                [CellValue.From(1), CellValue.From(1), CellValue.From("one")],
                [CellValue.From(2), CellValue.From(2), CellValue.From("two")],
                [CellValue.From(3), CellValue.From(1), CellValue.From("three")],
                [CellValue.From(4), CellValue.From(2), CellValue.From("four")],
                [CellValue.From(5), CellValue.From(1), CellValue.From("five")]
            ]);
        Require(rowIds.SequenceEqual([1L, 2L, 3L, 4L, 5L]),
            "Portable typed batch insert returned the wrong row ids.");

        var plan = DataVoCompiledQueryPlan.SelectMany(
            "Levels",
            ["Id", "Level", "Name"],
            "Level",
            "level");
        DataVoPreparedSelectMany<int> prepared = DataVoCompiledQuery.PrepareSelectManyTyped(
            db,
            plan,
            static row => row.GetInt32("Id"));
        IReadOnlyList<int> matches = prepared.Execute(1);

        Require(matches.Order().SequenceEqual([1, 3, 5]),
            "Portable scalar integer scan returned the wrong rows.");
    }

    private static void VerifyPortableGuidEquality()
    {
        Guid value = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        Guid different = Guid.Parse("00112233-4455-6677-8899-aabbccddeefe");
        Require(GuidSimdEqualityComparer.Instance.Equals(value, value),
            "Portable GUID comparer rejected equal values.");
        Require(!GuidSimdEqualityComparer.Instance.Equals(value, different),
            "Portable GUID comparer accepted different values.");
        Require(GuidSimdEqualityComparer.Instance.GetHashCode(value) == value.GetHashCode(),
            "Portable GUID comparer changed hash-code behavior.");
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

    private static string RequiredEnvironment(string name) =>
        Environment.GetEnvironmentVariable(name)
        ?? throw new InvalidOperationException($"Required environment variable {name} is not set.");

    private static TException Expect<TException>(Action action, string noExceptionMessage)
        where TException : Exception
    {
        try
        {
            action();
        }
        catch (TException error)
        {
            return error;
        }

        throw new InvalidOperationException(noExceptionMessage);
    }

    private static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }

    private sealed class PortableDisposedMarker
    {
    }

    private delegate long PortableGetLength(SafeFileHandle handle);
    private delegate void PortableWrite(SafeFileHandle handle, ReadOnlySpan<byte> bytes, long fileOffset);
    private delegate int PortableRead(SafeFileHandle handle, Span<byte> buffer, long fileOffset);
    private delegate void PortableSetLength(SafeFileHandle handle, long length);
    private delegate void PortableFlushToDisk(SafeFileHandle handle);
}
