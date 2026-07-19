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

        string testRoot = RequiredEnvironment("DATAVO_PORTABLE_TEST_ROOT");
        VerifyPortableCompatibilitySeams();
        VerifyPortableDurableSecondFences(core, testRoot);

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

        ExpectDurableRejection(StorageMode.Disk, Path.Combine(testRoot, "disk"));
        ExpectDurableRejection(StorageMode.Lsm, Path.Combine(testRoot, "lsm"));

        Console.WriteLine("Portable candidate .NET-host smoke passed.");
        return 0;
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

    private static void VerifyPortableDurableSecondFences(Assembly core, string testRoot)
    {
        var failures = new List<string>();
        RunFenceProbe("random access", () => VerifyPortableRandomAccessFence(core), failures);
        RunFenceProbe("file-handle acquisition", () => VerifyPortableFileHandlePoolFence(core, testRoot), failures);
        RunFenceProbe("LSM publication", () => VerifyPortableLsmPublicationFence(core, testRoot), failures);

        Require(
            failures.Count == 0,
            "Portable durable second-fence failures:\n- " + string.Join("\n- ", failures));
    }

    private static void VerifyPortableRandomAccessFence(Assembly core)
    {
        Type facade = RequireInternalType(core, "DataVo.Core.Compat.RandomAccessCompat");
        PortableGetLength getLength = RequireDelegate<PortableGetLength>(facade, "GetLength");
        PortableWrite write = RequireDelegate<PortableWrite>(facade, "Write");
        PortableRead read = RequireDelegate<PortableRead>(facade, "Read");
        PortableSetLength setLength = RequireDelegate<PortableSetLength>(facade, "SetLength");
        PortableFlushToDisk flushToDisk = RequireDelegate<PortableFlushToDisk>(facade, "FlushToDisk");
        byte[] empty = [];

        var probes = new (string Name, Action Invoke)[]
        {
            ("GetLength", () => getLength(null!)),
            ("Write", () => write(null!, empty, 0)),
            ("Read", () => read(null!, empty, 0)),
            ("SetLength", () => setLength(null!, 0)),
            ("FlushToDisk", () => flushToDisk(null!))
        };

        foreach ((string name, Action invoke) in probes)
        {
            PlatformNotSupportedException error = ExpectReflectionPlatformNotSupported(
                invoke,
                $"Portable random-access {name} accepted a null handle instead of rejecting durable I/O.");
            RequirePortableFenceMessage(error, $"random-access {name}");
        }
    }

    private static void VerifyPortableFileHandlePoolFence(Assembly core, string testRoot)
    {
        string acquireDirectory = Path.Combine(testRoot, "file-handle-acquire-second-fence");
        string acquirePath = Path.Combine(acquireDirectory, "table.dat");
        string removeDirectory = Path.Combine(testRoot, "file-handle-remove-second-fence");
        string removePath = Path.Combine(removeDirectory, "table.dat");
        Require(!Directory.Exists(acquireDirectory),
            $"Portable handle-pool acquire probe path already exists: {acquireDirectory}");
        Require(!Directory.Exists(removeDirectory),
            $"Portable handle-pool remove probe path already exists: {removeDirectory}");

        Type poolType = RequireInternalType(core, "DataVo.Core.StorageEngine.Disk.FileHandlePool");
        object pool = Activator.CreateInstance(
            poolType,
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null,
            args: [8],
            culture: null)
            ?? throw new InvalidOperationException("Could not construct the portable file-handle pool probe.");

        try
        {
            MethodInfo acquire = RequireMethod(poolType, "Acquire", BindingFlags.Public | BindingFlags.Instance);
            PlatformNotSupportedException acquireError = ExpectReflectionPlatformNotSupported(
                () => acquire.Invoke(pool, [acquirePath]),
                "Portable file-handle acquisition unexpectedly succeeded.");
            RequirePortableFenceMessage(acquireError, "file-handle acquisition");
            Require(!Directory.Exists(acquireDirectory),
                $"Portable file-handle acquisition rejection created directory '{acquireDirectory}'.");
            Require(!File.Exists(acquirePath),
                $"Portable file-handle acquisition rejection created file '{acquirePath}'.");

            MethodInfo flushToDisk = RequireMethod(
                poolType,
                "FlushToDisk",
                BindingFlags.Public | BindingFlags.Instance);
            PlatformNotSupportedException flushError = ExpectReflectionPlatformNotSupported(
                () => flushToDisk.Invoke(pool, null),
                "Portable file-handle pool flush unexpectedly succeeded.");
            RequirePortableFenceMessage(flushError, "file-handle pool flush");

            MethodInfo remove = RequireMethod(poolType, "Remove", BindingFlags.Public | BindingFlags.Instance);
            PlatformNotSupportedException removeError = ExpectReflectionPlatformNotSupported(
                () => remove.Invoke(pool, [removePath]),
                "Portable file-handle removal unexpectedly succeeded.");
            RequirePortableFenceMessage(removeError, "file-handle removal");
            Require(!Directory.Exists(removeDirectory),
                $"Portable file-handle removal rejection created directory '{removeDirectory}'.");
            Require(!File.Exists(removePath),
                $"Portable file-handle removal rejection created file '{removePath}'.");

            Type leaseType = poolType.GetNestedType(
                "FileHandleLease",
                BindingFlags.Public | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException(
                    $"Portable type '{poolType.FullName}' is missing nested type 'FileHandleLease'.");
            object lease = Activator.CreateInstance(leaseType, nonPublic: true)
                ?? throw new InvalidOperationException("Could not construct the portable file-handle lease probe.");
            try
            {
                PropertyInfo handle = leaseType.GetProperty(
                    "Handle",
                    BindingFlags.Public | BindingFlags.Instance)
                    ?? throw new InvalidOperationException(
                        $"Portable type '{leaseType.FullName}' is missing property 'Handle'.");
                PlatformNotSupportedException handleError = ExpectReflectionPlatformNotSupported(
                    () => handle.GetValue(lease),
                    "Portable file-handle lease exposed a handle.");
                RequirePortableFenceMessage(handleError, "file-handle lease access");
            }
            finally
            {
                ((IDisposable)lease).Dispose();
            }
        }
        finally
        {
            ((IDisposable)pool).Dispose();
        }
    }

    private static void VerifyPortableLsmPublicationFence(Assembly core, string testRoot)
    {
        string directory = Path.Combine(testRoot, "lsm-publication-second-fence");
        string path = Path.Combine(directory, "table.sst");
        Require(!Directory.Exists(directory), $"Portable LSM probe path already exists: {directory}");

        Type operations = RequireInternalType(core, "DataVo.Core.StorageEngine.Lsm.LsmDurableFileOperations");
        Type crashPointType = RequireInternalType(core, "DataVo.Core.StorageEngine.Lsm.LsmCrashPoint");
        MethodInfo publish = RequireMethod(operations, "WriteFileAtomically", BindingFlags.Public | BindingFlags.Static);
        object crashPoint = Enum.ToObject(crashPointType, 0);
        bool writeInvoked = false;
        bool crashHookInvoked = false;
        Action<Stream> write = _ => writeInvoked = true;
        Delegate crashHook = CreateFlaggingAction(crashPointType, () => crashHookInvoked = true);

        PlatformNotSupportedException error = ExpectReflectionPlatformNotSupported(
            () => publish.Invoke(null, [path, true, write, crashHook, crashPoint, crashPoint, crashPoint]),
            "Portable LSM publication unexpectedly succeeded.");
        RequirePortableFenceMessage(error, "LSM publication");

        Require(!writeInvoked, "Portable LSM rejection invoked the write callback.");
        Require(!crashHookInvoked, "Portable LSM rejection invoked the crash callback.");
        Require(!Directory.Exists(directory),
            $"Portable LSM rejection created directory '{directory}'.");
        Require(!File.Exists(path),
            $"Portable LSM rejection created destination file '{path}'.");
        Require(
            !Directory.Exists(directory)
            || !Directory.EnumerateFiles(directory, Path.GetFileName(path) + ".tmp-*").Any(),
            "Portable LSM rejection created a temporary publication file.");
    }

    private static Delegate CreateFlaggingAction(Type argumentType, Action callback)
    {
        MethodInfo factory = typeof(PortableSmoke)
            .GetMethod(nameof(CreateFlaggingActionCore), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(argumentType);
        return (Delegate)factory.Invoke(null, [callback])!;
    }

    private static Delegate CreateFlaggingActionCore<T>(Action callback) =>
        (Action<T>)(_ => callback());

    private static Type RequireInternalType(Assembly assembly, string fullName) =>
        assembly.GetType(fullName, throwOnError: false)
        ?? throw new InvalidOperationException($"Portable assembly is missing internal type '{fullName}'.");

    private static MethodInfo RequireMethod(Type type, string name, BindingFlags flags) =>
        type.GetMethod(name, flags)
        ?? throw new InvalidOperationException($"Portable type '{type.FullName}' is missing method '{name}'.");

    private static TDelegate RequireDelegate<TDelegate>(Type type, string name)
        where TDelegate : Delegate =>
        (TDelegate)RequireMethod(type, name, BindingFlags.Public | BindingFlags.Static)
            .CreateDelegate(typeof(TDelegate));

    private static PlatformNotSupportedException ExpectReflectionPlatformNotSupported(
        Action action,
        string noExceptionMessage)
    {
        try
        {
            action();
        }
        catch (TargetInvocationException error) when (error.InnerException is PlatformNotSupportedException unsupported)
        {
            return unsupported;
        }
        catch (PlatformNotSupportedException unsupported)
        {
            return unsupported;
        }
        catch (Exception error)
        {
            Exception actual = error is TargetInvocationException invocation
                && invocation.InnerException is Exception inner
                ? inner
                : error;
            throw new InvalidOperationException(
                $"{noExceptionMessage} It threw {actual.GetType().FullName} instead.",
                actual);
        }

        throw new InvalidOperationException(noExceptionMessage);
    }

    private static void RequirePortableFenceMessage(PlatformNotSupportedException error, string operation)
    {
        Require(error.Message.Contains("netstandard2.1", StringComparison.Ordinal),
            $"Portable {operation} rejection did not name the experimental target.");
        Require(error.Message.Contains(nameof(StorageMode.InMemory), StringComparison.Ordinal),
            $"Portable {operation} rejection did not name the supported storage mode.");
    }

    private static void RunFenceProbe(string name, Action probe, ICollection<string> failures)
    {
        try
        {
            probe();
        }
        catch (Exception error)
        {
            failures.Add($"{name}: {error.Message}");
        }
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

        Require(PasswordHasher.Verify(password, persistedHash, persistedSalt),
            "Portable PBKDF2 fallback rejected the fixed modern password vector.");
        Require(!PasswordHasher.Verify("wrong-password", persistedHash, persistedSalt),
            "Portable PBKDF2 fallback accepted the wrong password.");

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
        IReadOnlyList<long> rowIds = db.InsertTypedBatch(
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
