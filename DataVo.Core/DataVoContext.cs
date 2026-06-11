using DataVo.Core.Contracts.Results;
using DataVo.Core.MVCC;
using DataVo.Core.Parser.DML;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Utils;

namespace DataVo.Core;

/// <summary>
/// Provides a small developer-facing entry point for executing SQL against a dedicated <see cref="DataVoEngine"/> instance.
/// </summary>
/// <remarks>
/// <para>
/// This type is intended for embedders that want a simple API surface without manually wiring
/// <see cref="DataVoEngine"/>, <see cref="QueryEngine"/>, and session identifiers together.
/// </para>
/// <para>
/// The context owns the underlying engine and should be disposed when no longer needed.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// using var context = new DataVoContext(new DataVoConfig
/// {
///     StorageMode = StorageMode.InMemory
/// });
///
/// context.Execute("CREATE DATABASE Demo");
/// context.Execute("USE Demo");
/// context.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(50))");
/// List&lt;QueryResult&gt; results = context.Execute("SELECT * FROM Users");
/// </code>
/// </example>
public sealed class DataVoContext : IDisposable
{
    /// <summary>
    /// Initializes a new context and underlying engine using the supplied configuration.
    /// </summary>
    /// <param name="config">The storage and durability settings for the engine instance.</param>
    public DataVoContext(DataVoConfig config)
    {
        Engine = DataVoEngine.Initialize(config);
        SessionId = Guid.NewGuid();
    }

    /// <summary>
    /// Gets the engine instance owned by this context.
    /// </summary>
    public DataVoEngine Engine { get; }

    /// <summary>
    /// Gets the change-capture service exposing committed <see cref="ChangeSet"/> notifications.
    /// </summary>
    public ChangeCapture Changes => Engine.Changes;

    /// <summary>
    /// Gets or sets the default session identifier used by <see cref="Execute(string)"/>.
    /// </summary>
    public Guid SessionId { get; set; }

    /// <summary>
    /// Gets the diagnostics facade for runtime query instrumentation.
    /// </summary>
    public DataVoDiagnostics Diagnostics => Engine.Diagnostics;

    /// <summary>
    /// Registers a standing reactive query that reports exactly what changed in its result set, and
    /// returns a handle whose disposal tears the subscription down.
    /// </summary>
    /// <remarks>
    /// The SQL is parsed, validated, and compiled into an incremental dataflow operator
    /// (filter/aggregate/top-K/join/distinct/union/subquery/recursive-CTE per the query shape), then
    /// seeded from current table contents — the seed is the baseline and is never re-delivered, so the
    /// first callback is the first post-subscribe change. Notifications are <b>pull-drain</b>: nothing
    /// fires until <see cref="DispatchPendingNotifications"/> is called, and only committed changes are
    /// ever delivered (rollbacks emit nothing). Subscribing requires an active database for the session
    /// (execute <c>USE &lt;database&gt;</c> first) and auto-enables change capture on the first
    /// subscription.
    /// </remarks>
    /// <param name="sql">The <c>SELECT</c> statement to observe; unsupported constructs throw <see cref="NotSupportedException"/>.</param>
    /// <param name="onChanged">The callback invoked with non-empty changes on each drain.</param>
    /// <returns>A disposable subscription handle.</returns>
    public IDisposable Subscribe(string sql, Action<QueryChange> onChanged)
        => Engine.Reactive.Add(this, sql, onChanged);

    /// <summary>
    /// Subscribes on the zero-allocation fast lane: delivers a borrowed <see cref="QueryChangeRef"/>
    /// (valid only during the callback) without materializing to an owned <see cref="QueryChange"/>.
    /// Throws <see cref="NotSupportedException"/> for query shapes not yet migrated to borrowed emit.
    /// </summary>
    public IDisposable SubscribeZeroAlloc(string sql, QueryDeltaHandler onChanged)
        => Engine.Reactive.SubscribeZeroAlloc(this, sql, onChanged);

    /// <summary>
    /// Drains buffered committed change sets through active subscriptions, invoking callbacks on the
    /// calling thread.
    /// </summary>
    /// <remarks>
    /// This is the only point at which reactive work runs — there are no background threads or timers,
    /// so the consumer fully controls timing (deterministic for game main-loops, non-blocking for the
    /// writer). Writes performed inside a callback are captured and surface on the <b>next</b> drain,
    /// never recursively within the current one, so callbacks cannot self-trigger an infinite loop.
    /// </remarks>
    public void DispatchPendingNotifications()
        => Engine.Reactive.Dispatch();

    /// <summary>
    /// Sets the maximum number of concurrently active reactive subscriptions; <see cref="Subscribe"/>
    /// throws once the cap is reached.
    /// </summary>
    /// <param name="max">The new subscription cap.</param>
    public void SetMaxReactiveSubscriptions(int max)
        => Engine.Reactive.MaxSubscriptions = max;

    /// <summary>
    /// Executes a SQL query using the current <see cref="SessionId"/>.
    /// </summary>
    /// <param name="query">The SQL text to parse and execute.</param>
    /// <returns>The sequence of query results produced by the parsed statement batch.</returns>
    public List<QueryResult> Execute(string query)
    {
        return Execute(query, SessionId);
    }

    /// <summary>
    /// Executes a SQL query using an explicit session identifier.
    /// </summary>
    /// <param name="query">The SQL text to parse and execute.</param>
    /// <param name="sessionId">The session whose database binding and transaction state should be used.</param>
    /// <returns>The sequence of query results produced by the parsed statement batch.</returns>
    public List<QueryResult> Execute(string query, Guid sessionId)
    {
        using IDisposable _ = Engine.EnterRuntimeReadScope();
        return new QueryEngine(query, sessionId, Engine).Parse();
    }

    /// <summary>
    /// Creates an in-memory snapshot of the current engine state for the current <see cref="SessionId"/>.
    /// </summary>
    public DataVoSnapshot CreateSnapshot()
    {
        return Engine.CreateSnapshot(SessionId);
    }

    /// <summary>
    /// Restores a previously captured engine snapshot for the current <see cref="SessionId"/>.
    /// </summary>
    public void RestoreSnapshot(DataVoSnapshot snapshot)
    {
        Engine.RestoreSnapshot(SessionId, snapshot);
    }

    /// <summary>
    /// Inserts multiple typed rows into a table using the current <see cref="SessionId"/> database binding.
    /// </summary>
    public IReadOnlyList<long> BulkInsert(
        string tableName,
        IEnumerable<IReadOnlyDictionary<string, object?>> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        using IDisposable runtimeScope = Engine.EnterRuntimeReadScope();
        List<IReadOnlyDictionary<string, object?>> materializedRows = rows
            .Select(row => row ?? throw new ArgumentNullException(nameof(rows), "Rows cannot contain null entries."))
            .Select(row => (IReadOnlyDictionary<string, object?>)new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase))
            .ToList();

        if (materializedRows.Count == 0)
        {
            return [];
        }

        RuntimeQueryStatsBuilder? diagnosticsBuilder = null;
        if (Engine.Diagnostics.Enabled)
        {
            diagnosticsBuilder = new RuntimeQueryStatsBuilder
            {
                QueryText = $"BULK INSERT {tableName}",
                StorageMode = Engine.Config.StorageMode,
                DatabaseName = TryGetCurrentDatabaseName()
            };
            diagnosticsBuilder.SetOperation("BULK INSERT");
            diagnosticsBuilder.AddTable(tableName);
        }

        using RuntimeQueryDiagnosticsScope? diagnosticsScope =
            RuntimeQueryDiagnosticsScope.Start(Engine.Diagnostics, diagnosticsBuilder);

        string databaseName;
        try
        {
            databaseName = ResolveCurrentDatabase();
        }
        catch (Exception ex)
        {
            diagnosticsBuilder?.RecordError(ex.Message);
            throw;
        }

        if (Engine.TransactionManager.HasActiveTransaction(SessionId))
        {
            const string transactionError = "BulkInsert cannot run while the current session has an active transaction.";
            diagnosticsBuilder?.RecordError(transactionError);
            throw new InvalidOperationException(transactionError);
        }

        var service = new InsertRowService(
            Engine,
            Engine.StorageContext,
            Engine.Catalog,
            Engine.IndexManager);

        ChangeRecorder? recorder = ChangeRecorder.TryCreate(Engine, databaseName);
        long statementTxId = MvccCoordinator.ResolveStatementTransactionId(Engine, null);
        Engine.LockManager.AcquireWriteLock(databaseName, tableName);

        try
        {
            InsertRowsResult result = service.InsertRows(databaseName, tableName, materializedRows, txContext: null, statementTxId, recorder);
            diagnosticsBuilder?.AddRowsAffected(result.AcceptedRowCount);
            recorder?.Publish();
            return result.RowIds;
        }
        catch (Exception ex)
        {
            diagnosticsBuilder?.RecordError(ex.Message);
            throw;
        }
        finally
        {
            Engine.LockManager.ReleaseWriteLock(databaseName, tableName);
        }
    }

    /// <summary>
    /// Authenticates the current <see cref="SessionId"/> against configured users.
    /// </summary>
    public bool Login(string username, string password)
    {
        return Login(username, password, SessionId);
    }

    /// <summary>
    /// Authenticates a specific session against configured users.
    /// </summary>
    public bool Login(string username, string password, Guid sessionId)
    {
        return Engine.AuthenticateSession(sessionId, username, password);
    }

    /// <summary>
    /// Clears the authenticated principal bound to the current <see cref="SessionId"/>.
    /// </summary>
    public void Logout()
    {
        Logout(SessionId);
    }

    /// <summary>
    /// Clears the authenticated principal bound to a specific session.
    /// </summary>
    public void Logout(Guid sessionId)
    {
        Engine.LogoutSession(sessionId);
    }

    /// <summary>
    /// Executes a nearest-neighbor vector search using an HNSW index in the current session database.
    /// </summary>
    /// <param name="tableName">The table containing the indexed vector column.</param>
    /// <param name="indexName">The vector index name.</param>
    /// <param name="queryVector">The query vector.</param>
    /// <param name="topK">The number of nearest rows to return.</param>
    /// <returns>The matching table rows in ranked order.</returns>
    public List<Dictionary<string, object?>> SearchNearest(string tableName, string indexName, float[] queryVector, int topK = 10)
    {
        using IDisposable runtimeScope = Engine.EnterRuntimeReadScope();
        RuntimeQueryStatsBuilder? diagnosticsBuilder = null;
        if (Engine.Diagnostics.Enabled)
        {
            diagnosticsBuilder = new RuntimeQueryStatsBuilder
            {
                QueryText = $"VECTOR SEARCH {tableName}.{indexName}",
                StorageMode = Engine.Config.StorageMode,
                DatabaseName = TryGetCurrentDatabaseName()
            };
            diagnosticsBuilder.SetOperation("VECTOR SEARCH");
            diagnosticsBuilder.AddTable(tableName);
        }

        using RuntimeQueryDiagnosticsScope? diagnosticsScope =
            RuntimeQueryDiagnosticsScope.Start(Engine.Diagnostics, diagnosticsBuilder);

        string databaseName;
        try
        {
            databaseName = ResolveCurrentDatabase();
        }
        catch (Exception ex)
        {
            diagnosticsBuilder?.RecordError(ex.Message);
            throw;
        }

        using var _ = DataVoEngine.PushCurrent(Engine);
        List<long> rowIds;
        try
        {
            rowIds = Engine.IndexManager.SearchVector(queryVector, topK, indexName, tableName, databaseName);
        }
        catch (Exception ex)
        {
            diagnosticsBuilder?.RecordError(ex.Message);
            throw;
        }

        if (rowIds.Count == 0)
        {
            return [];
        }

        try
        {
            Dictionary<long, Dictionary<string, object?>> rows = Engine.StorageContext.GetTableContents(rowIds, tableName, databaseName);
            List<Dictionary<string, object?>> results = rowIds
                .Where(rows.ContainsKey)
                .Select(id => rows[id])
                .ToList();
            diagnosticsBuilder?.AddRowsReturned(results.Count);
            return results;
        }
        catch (Exception ex)
        {
            diagnosticsBuilder?.RecordError(ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Executes a nearest-neighbor vector search using a vector literal formatted as <c>[x,y,z]</c>.
    /// </summary>
    public List<Dictionary<string, object?>> SearchNearest(string tableName, string indexName, string queryVector, int topK = 10)
    {
        if (VectorParser.TryParseVector(queryVector, out float[] parsedVector))
        {
            return SearchNearest(tableName, indexName, parsedVector, topK);
        }

        const string parseError = "Query vector must be in format [x,y,z].";
        RuntimeQueryStatsBuilder? diagnosticsBuilder = null;
        if (Engine.Diagnostics.Enabled)
        {
            diagnosticsBuilder = new RuntimeQueryStatsBuilder
            {
                QueryText = $"VECTOR SEARCH {tableName}.{indexName}",
                StorageMode = Engine.Config.StorageMode,
                DatabaseName = TryGetCurrentDatabaseName()
            };
            diagnosticsBuilder.SetOperation("VECTOR SEARCH");
            diagnosticsBuilder.AddTable(tableName);
        }

        using RuntimeQueryDiagnosticsScope? diagnosticsScope =
            RuntimeQueryDiagnosticsScope.Start(Engine.Diagnostics, diagnosticsBuilder);
        diagnosticsBuilder?.RecordError(parseError);
        throw new ArgumentException(parseError, nameof(queryVector));
    }

    private string? TryGetCurrentDatabaseName()
    {
        return Engine.Sessions.Get(SessionId);
    }

    private string ResolveCurrentDatabase()
    {
        string? databaseName = TryGetCurrentDatabaseName();
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new InvalidOperationException("No database selected for the current session. Execute USE <database> first.");
        }

        return databaseName;
    }

    /// <summary>
    /// Releases resources owned by the underlying engine.
    /// </summary>
    public void Dispose()
    {
        Engine.Dispose();
    }
}
