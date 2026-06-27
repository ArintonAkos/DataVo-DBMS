using System.Diagnostics;
using Microsoft.Data.Sqlite;
using Research.Benchmark.Abstractions;

namespace Research.Benchmark.Runners.ConcurrentOps;

public sealed class SqliteConcurrentOpsEngine : IConcurrentOpsEngine
{
    private const int SqliteBusy = 5;
    private const int SqliteLocked = 6;

    private string? _connectionString;
    private SqliteConnection? _rootConnection;
    private int _initialRecords;
    private long _nextInsertId;
    private int _busyTimeoutMs;

    public string Name => "SQLite";

    public async Task InitializeAsync(ConcurrentOpsOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        await DisposeAsync();

        string databaseName = $"concurrentdb_{Guid.NewGuid():N}";
        _connectionString = $"Data Source=file:{databaseName}?mode=memory&cache=shared";
        _busyTimeoutMs = checked((int)Math.Clamp(options.BusyTimeout.TotalMilliseconds, 1d, int.MaxValue));
        _rootConnection = OpenConnection();

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
        _initialRecords = options.InitialRecords;
        _nextInsertId = options.InitialRecords;
    }

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

    public ValueTask DisposeAsync()
    {
        _rootConnection?.Dispose();
        _rootConnection = null;
        _connectionString = null;
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

    private SqliteConnection OpenConnection()
    {
        string connectionString = _connectionString
            ?? throw new InvalidOperationException("SQLite concurrent-ops engine has not been initialized.");
        var connection = new SqliteConnection(connectionString);
        connection.Open();

        using SqliteCommand busy = connection.CreateCommand();
        busy.CommandText = $"PRAGMA busy_timeout = {_busyTimeoutMs};";
        busy.ExecuteNonQuery();
        return connection;
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
