using System.Diagnostics;
using Microsoft.Data.Sqlite;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ConcurrentOps;

/// <summary>
/// Concurrent 8-reader, 2-writer benchmark runner for SQLite supporting both shared-cache in-memory
/// mode and file-backed WAL (<c>synchronous=NORMAL</c>) mode.
/// </summary>
/// <example>
/// <code>
/// await using var engine = new SqliteConcurrentOpsEngine(useWal: true);
/// await engine.InitializeAsync(ConcurrentOpsOptions.Default);
/// ConcurrentOpsResult result = await engine.RunAsync(ConcurrentOpsOptions.Default);
/// </code>
/// </example>
public sealed class SqliteConcurrentOpsEngine : IConcurrentOpsEngine
{
    private const int SqliteBusy = 5;
    private const int SqliteLocked = 6;

    private readonly bool _useWal;
    private string? _connectionString;
    private string? _tempDirectory;
    private SqliteConnection? _rootConnection;
    private int _initialRecords;
    private long _nextInsertId;
    private int _busyTimeoutMs;

    /// <summary>
    /// Initializes a new instance of <see cref="SqliteConcurrentOpsEngine"/>.
    /// </summary>
    /// <param name="useWal">
    /// When <see langword="true"/>, runs against a temporary file-backed SQLite database in
    /// <c>journal_mode=WAL</c> with <c>synchronous=NORMAL</c>; otherwise uses shared-cache in-memory SQLite.
    /// </param>
    public SqliteConcurrentOpsEngine(bool useWal = false)
    {
        _useWal = useWal;
    }

    /// <summary>
    /// Gets the display name used in benchmark reports for the configured SQLite mode.
    /// </summary>
    public string Name => _useWal ? "SQLite (WAL,normal)" : "SQLite";

    /// <summary>
    /// Creates the schema and preloads <see cref="ConcurrentOpsOptions.InitialRecords"/> rows before timed measurement.
    /// </summary>
    public async Task InitializeAsync(ConcurrentOpsOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        await DisposeAsync();

        _connectionString = CreateConnectionString();
        _busyTimeoutMs = checked((int)Math.Clamp(options.BusyTimeout.TotalMilliseconds, 1d, int.MaxValue));
        _rootConnection = OpenConnection(initializeWal: _useWal);

        using (SqliteCommand schema = _rootConnection.CreateCommand())
        {
            schema.CommandText = """
                CREATE TABLE Records (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    Value INTEGER NOT NULL,
                    Score REAL NOT NULL
                );
                """;
            schema.ExecuteNonQuery();
        }

        using SqliteTransaction transaction = _rootConnection.BeginTransaction();
        using SqliteCommand insert = _rootConnection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = "INSERT INTO Records (Id, Name, Value, Score) VALUES ($id, $name, $value, $score);";
        insert.Parameters.Add("$id", SqliteType.Integer);
        insert.Parameters.Add("$name", SqliteType.Text);
        insert.Parameters.Add("$value", SqliteType.Integer);
        insert.Parameters.Add("$score", SqliteType.Real);
        insert.Prepare();

        for (int i = 1; i <= options.InitialRecords; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            insert.Parameters["$id"].Value = i;
            insert.Parameters["$name"].Value = $"name-{i}";
            insert.Parameters["$value"].Value = i;
            insert.Parameters["$score"].Value = i * 1.5d;
            insert.ExecuteNonQuery();
        }

        transaction.Commit();
        if (_useWal)
        {
            using SqliteCommand checkpoint = _rootConnection.CreateCommand();
            checkpoint.CommandText = "PRAGMA wal_checkpoint(TRUNCATE);";
            checkpoint.ExecuteNonQuery();
        }

        _initialRecords = options.InitialRecords;
        _nextInsertId = options.InitialRecords;
    }

    /// <summary>
    /// Runs concurrent reader and writer worker tasks for the configured duration and returns aggregate metrics.
    /// </summary>
    public async Task<ConcurrentOpsResult> RunAsync(ConcurrentOpsOptions options, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linkedCts.CancelAfter(options.Duration);
        CancellationToken token = linkedCts.Token;

        var stopwatch = Stopwatch.StartNew();
        Task<ConcurrentWorkerResult>[] readTasks = Enumerable.Range(0, options.ReaderWorkers)
            .Select(workerId => Task.Run(() => RunReadWorker(workerId, token), CancellationToken.None))
            .ToArray();
        Task<ConcurrentWorkerResult>[] writeTasks = Enumerable.Range(0, options.WriterWorkers)
            .Select(workerId => Task.Run(() => RunWriteWorker(workerId, token), CancellationToken.None))
            .ToArray();

        await Task.WhenAll(readTasks.Concat(writeTasks));
        stopwatch.Stop();

        return ConcurrentOpsMetrics.Build(
            readTasks.Select(static task => task.Result).ToArray(),
            writeTasks.Select(static task => task.Result).ToArray(),
            stopwatch.Elapsed);
    }

    /// <summary>
    /// Releases open SQLite connections and deletes any temporary WAL database directory.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        _rootConnection?.Dispose();
        _rootConnection = null;
        _connectionString = null;
        CleanupTempDirectory();
        return ValueTask.CompletedTask;
    }

    private ConcurrentWorkerResult RunReadWorker(int workerId, CancellationToken cancellationToken)
    {
        using SqliteConnection connection = OpenConnection();
        using SqliteCommand lookup = connection.CreateCommand();
        lookup.CommandText = "SELECT Id, Name, Value, Score FROM Records WHERE Id = $id;";
        lookup.Parameters.Add("$id", SqliteType.Integer);
        lookup.Prepare();

        var random = new Random(unchecked(Environment.TickCount * 31 + workerId));
        var latencies = new List<double>(16_384);
        long operations = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            lookup.Parameters["$id"].Value = random.Next(1, _initialRecords + 1);
            long start = Stopwatch.GetTimestamp();
            try
            {
                using SqliteDataReader reader = lookup.ExecuteReader();
                if (reader.Read())
                {
                    _ = reader.GetInt64(0);
                    _ = reader.GetString(1);
                    _ = reader.GetInt32(2);
                    _ = reader.GetDouble(3);
                }

                operations++;
            }
            catch (SqliteException ex) when (IsBusyOrLocked(ex))
            {
                // Busy timeout expired. Keep the run alive so contention degrades metrics instead of aborting.
            }
            finally
            {
                latencies.Add(ConcurrentOpsMetrics.ElapsedMs(start));
            }
        }

        return new ConcurrentWorkerResult(operations, latencies);
    }

    private ConcurrentWorkerResult RunWriteWorker(int workerId, CancellationToken cancellationToken)
    {
        using SqliteConnection connection = OpenConnection();
        using SqliteCommand insert = connection.CreateCommand();
        insert.CommandText = "INSERT INTO Records (Id, Name, Value, Score) VALUES ($id, $name, $value, $score);";
        insert.Parameters.Add("$id", SqliteType.Integer);
        insert.Parameters.Add("$name", SqliteType.Text);
        insert.Parameters.Add("$value", SqliteType.Integer);
        insert.Parameters.Add("$score", SqliteType.Real);
        insert.Prepare();

        using SqliteCommand update = connection.CreateCommand();
        update.CommandText = "UPDATE Records SET Value = $value, Score = $score WHERE Id = $id;";
        update.Parameters.Add("$value", SqliteType.Integer);
        update.Parameters.Add("$score", SqliteType.Real);
        update.Parameters.Add("$id", SqliteType.Integer);
        update.Prepare();

        var random = new Random(unchecked(Environment.TickCount * 37 + workerId));
        var latencies = new List<double>(4096);
        long operations = 0;
        bool insertNext = workerId % 2 == 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            long start = Stopwatch.GetTimestamp();
            try
            {
                if (insertNext)
                {
                    long id = Interlocked.Increment(ref _nextInsertId);
                    insert.Parameters["$id"].Value = id;
                    insert.Parameters["$name"].Value = $"name-{id}";
                    insert.Parameters["$value"].Value = id;
                    insert.Parameters["$score"].Value = id * 1.5d;
                    insert.ExecuteNonQuery();
                }
                else
                {
                    int value = random.Next();
                    update.Parameters["$id"].Value = random.Next(1, _initialRecords + 1);
                    update.Parameters["$value"].Value = value;
                    update.Parameters["$score"].Value = value * 0.5d;
                    update.ExecuteNonQuery();
                }

                operations++;
            }
            catch (SqliteException ex) when (IsBusyOrLocked(ex))
            {
                // Busy timeout expired. Keep the run alive so contention degrades metrics instead of aborting.
            }
            finally
            {
                latencies.Add(ConcurrentOpsMetrics.ElapsedMs(start));
                insertNext = !insertNext;
            }
        }

        return new ConcurrentWorkerResult(operations, latencies);
    }

    private string CreateConnectionString()
    {
        if (!_useWal)
        {
            string databaseName = $"concurrentdb_{Guid.NewGuid():N}";
            return $"Data Source=file:{databaseName}?mode=memory&cache=shared";
        }

        _tempDirectory = Path.Combine(Path.GetTempPath(), $"datavo-sqlite-wal-concurrent-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDirectory);
        string dbPath = Path.Combine(_tempDirectory, "concurrent.sqlite");
        return $"Data Source={dbPath};Pooling=False";
    }

    private SqliteConnection OpenConnection(bool initializeWal = false)
    {
        string connectionString = _connectionString
            ?? throw new InvalidOperationException("SQLite concurrent-ops engine has not been initialized.");
        var connection = new SqliteConnection(connectionString);
        connection.Open();

        if (_useWal)
        {
            using SqliteCommand walPragma = connection.CreateCommand();
            walPragma.CommandText = initializeWal
                ? "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;"
                : "PRAGMA synchronous = NORMAL;";
            walPragma.ExecuteNonQuery();
        }

        using SqliteCommand busy = connection.CreateCommand();
        busy.CommandText = $"PRAGMA busy_timeout = {_busyTimeoutMs};";
        busy.ExecuteNonQuery();
        return connection;
    }

    private void CleanupTempDirectory()
    {
        if (_tempDirectory is null)
        {
            return;
        }

        try
        {
            if (Directory.Exists(_tempDirectory))
            {
                Directory.Delete(_tempDirectory, recursive: true);
            }
        }
        catch
        {
            // Best-effort temp cleanup after benchmark disposal.
        }
        finally
        {
            _tempDirectory = null;
        }
    }

    private void EnsureInitialized()
    {
        if (_rootConnection is null || _connectionString is null)
        {
            throw new InvalidOperationException("SQLite concurrent-ops engine has not been initialized.");
        }
    }

    private static bool IsBusyOrLocked(SqliteException exception) =>
        exception.SqliteErrorCode is SqliteBusy or SqliteLocked;
}
