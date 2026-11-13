using DataVo.Core.StorageEngine;
﻿using System.Diagnostics.CodeAnalysis;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.Parser.DML;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Utils;
using DataVo.Core.ValueConversion;

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
    private InsertRowService? _typedInsertService;
    private int _disposed;
    private readonly DataVoValueConverterRegistry _valueConverters = new();
    private readonly DataVoModelBinding _modelBindings = new();
    private readonly Dictionary<(string DatabaseName, string TableName), string> _retainedWriteLockKeys = [];

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
    /// Registers external CLR value converters for this context. Registration is atomic: a conflicting
    /// converter leaves the existing set unchanged.
    /// </summary>
    public void RegisterValueConverters(params IDataVoValueConverter[] converters)
    {
        _valueConverters.RegisterRange(converters);
    }

    /// <summary>
    /// Reads one named materialized result value as <typeparamref name="T"/> using the converter registered
    /// for that target type when the value is not already compatible.
    /// </summary>
    public T Get<T>(IReadOnlyDictionary<string, object?> row, string columnName)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(row);
        if (string.IsNullOrWhiteSpace(columnName))
        {
            throw new ArgumentException("Result column name is required.", nameof(columnName));
        }

        if (!row.TryGetValue(columnName, out object? value))
        {
            throw new KeyNotFoundException($"Result row does not contain column '{columnName}'.");
        }

        return _valueConverters.ConvertMaterialized<T>(value);
    }

    /// <summary>
    /// Materializes a public parameterless model from a result row. Result fields without a matching public
    /// writable property are ignored; registered converters are selected from each property type.
    /// </summary>
    public T Map<
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        IReadOnlyDictionary<string, object?> row)
    {
        return _modelBindings.Map<T>(row, _valueConverters);
    }

    /// <summary>
    /// Inserts a public parameterless model by matching its public readable properties to the selected
    /// table's catalog columns. The binding is cached for the current catalog schema version.
    /// </summary>
    public RowKey Insert<
        [DynamicallyAccessedMembers(
            DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        string tableName,
        T entity)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name is required.", nameof(tableName));
        }

        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(entity);
        string databaseName = ResolveCurrentDatabase();
        tableName = StoredTableName(tableName);
        int schemaVersion = Engine.Catalog.GetTableSchemaVersion(tableName, databaseName);
        IReadOnlyList<Column> columns = Engine.Catalog.GetTableColumns(tableName, databaseName);
        DataVoModelBinding.WriteBinding binding = _modelBindings.GetWriteBinding(
            typeof(T), databaseName, tableName, schemaVersion, columns);
        CellValue[] cells = binding.CreateCells(entity!, _valueConverters);

        return InsertTyped(tableName, binding.Schema, cells);
    }

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
        using SnapshotLockScope _ = Engine.EnterRuntimeReadScope();
        return new QueryEngine(query, sessionId, Engine).Parse();
    }

    /// <summary>
    /// Runs one SELECT and returns its rows as they are read, without holding the whole result in memory.
    /// </summary>
    /// <param name="query">One SELECT statement, which may contain <c>@param</c> placeholders.</param>
    /// <param name="parameters">The values for the placeholders.</param>
    /// <returns>The rows. Each enumeration runs the scan again.</returns>
    /// <remarks>
    /// A plain scan of one table is read in small pages, and no lock is held between pages, so changes
    /// committed during enumeration may or may not appear. Queries that need the whole result first
    /// (joins, GROUP BY, ORDER BY, DISTINCT, index lookups) and reads inside a transaction are evaluated
    /// up front and then enumerated. Errors in the query are thrown by this call.
    /// </remarks>
    /// <exception cref="ArgumentException"><paramref name="query"/> is not exactly one SELECT.</exception>
    /// <exception cref="InvalidOperationException">The query failed.</exception>
    /// <example>
    /// <code>
    /// foreach (DataVoRow row in context.StreamQuery("SELECT Id, Hp FROM Players WHERE Hp &gt; @min", ("min", 10)))
    /// {
    ///     Process(row["Id"], row["Hp"]);
    /// }
    /// </code>
    /// </example>
    public IEnumerable<DataVoRow> StreamQuery(string query, params DataVoQueryParameter[] parameters) =>
        QueryStream.Open(Engine, query, parameters, SessionId);

    /// <summary>Runs one SELECT under an explicit session and returns its rows as they are read.</summary>
    /// <param name="query">One SELECT statement.</param>
    /// <param name="sessionId">The session whose database and transaction apply.</param>
    /// <returns>The rows. Each enumeration runs the scan again.</returns>
    /// <example>
    /// <code>
    /// IEnumerable&lt;DataVoRow&gt; rows = context.StreamQuery("SELECT Id FROM Players", sessionId);
    /// </code>
    /// </example>
    public IEnumerable<DataVoRow> StreamQuery(string query, Guid sessionId) =>
        QueryStream.Open(Engine, query, [], sessionId);

    /// <summary>
    /// Gets the typed, queryable table of a <c>[DataVoTable]</c> model.
    /// </summary>
    /// <typeparam name="T">A model marked <c>[DataVoTable]</c>, for which <c>DataVo.Generators</c> has emitted data access.</typeparam>
    /// <returns>The model's table. The same instance is returned until <see cref="SessionId"/> changes.</returns>
    /// <exception cref="InvalidOperationException">
    /// <typeparamref name="T"/> has no generated data access, or its generated code has not run yet. Models register
    /// themselves when the program starts; on a compiler without module initializers, such as Unity's, a model
    /// registers the first time one of its generated methods is called, so call its <c>Ensure…Table</c> method first.
    /// </exception>
    /// <example>
    /// <code>
    /// Player? player = context.Table&lt;Player&gt;().FirstOrDefault(id, static (p, key) => p.Id == key);
    /// foreach (Player p in context.Table&lt;Player&gt;().Where(minLevel, static (p, min) => p.Level >= min))
    /// {
    ///     Console.WriteLine(p.Name);
    /// }
    /// </code>
    /// </example>
    public DataVoTable<T> Table<T>() => DataVoTableInfo<T>.Registered.For(this);

    /// <summary>
    /// Streams a table of the current database as <see cref="RowRef"/> values read straight from storage. Once a
    /// thread has streamed this table, a full pass allocates nothing on the managed heap.
    /// </summary>
    /// <param name="tableName">The table to read.</param>
    /// <returns>A stream that reads the table each time it is enumerated.</returns>
    /// <exception cref="ArgumentException"><paramref name="tableName"/> is null or empty.</exception>
    /// <remarks>
    /// Column positions follow the table's column order; <see cref="RowRefStream.GetOrdinal"/> looks one up by name.
    /// The table and database are checked when the stream is enumerated. See <see cref="RowRefStream"/> for how a
    /// pass interacts with writers and transactions.
    /// </remarks>
    /// <example>
    /// <code>
    /// foreach (RowRef row in context.StreamRef("Players"))
    /// {
    ///     if (!row.IsNull(2))
    ///     {
    ///         Process(row.GetInt32(0), row.GetStringSpan(2));
    ///     }
    /// }
    /// </code>
    /// </example>
    public RowRefStream StreamRef(string tableName)
    {
        if (string.IsNullOrEmpty(tableName))
        {
            throw new ArgumentException("A table name is required.", nameof(tableName));
        }

        return new RowRefStream(Engine, SessionId, tableName);
    }

    /// <summary>
    /// Pre-parses and compiles a parameterized SQL statement for repeated execution under the current <see cref="SessionId"/>.
    /// </summary>
    /// <param name="sql">The SQL statement text, which may contain <c>@param</c> placeholders.</param>
    /// <returns>A thread-safe <see cref="DataVoPreparedStatement"/> that can be executed repeatedly with bound parameters.</returns>
    /// <example>
    /// <code>
    /// DataVoPreparedStatement lookup = context.Prepare("SELECT Id, Name FROM Users WHERE Id = @id");
    /// QueryResult result = lookup.Execute(("id", 5));
    /// </code>
    /// </example>
    public DataVoPreparedStatement Prepare(string sql)
    {
        return Prepare(sql, SessionId);
    }

    /// <summary>
    /// Pre-parses and compiles a parameterized SQL statement for repeated execution under an explicit session identifier.
    /// </summary>
    /// <param name="sql">The SQL statement text, which may contain <c>@param</c> placeholders.</param>
    /// <param name="sessionId">The default session identifier used when executing the prepared statement.</param>
    /// <returns>A thread-safe <see cref="DataVoPreparedStatement"/> that can be executed repeatedly with bound parameters.</returns>
    /// <example>
    /// <code>
    /// DataVoPreparedStatement lookup = context.Prepare("SELECT Id, Name FROM Users WHERE Id = @id", sessionId);
    /// QueryResult result = lookup.Execute(("id", 5));
    /// </code>
    /// </example>
    public DataVoPreparedStatement Prepare(string sql, Guid sessionId)
    {
        return new DataVoPreparedStatement(this, sql, sessionId);
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
    public IReadOnlyList<RowKey> BulkInsert(
        string tableName,
        IEnumerable<IReadOnlyDictionary<string, object?>> rows)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rows);
        tableName = StoredTableName(tableName);

        using SnapshotLockScope runtimeScope = Engine.EnterRuntimeReadScope();
        List<IReadOnlyDictionary<string, object?>> materializedRows = rows
            .Select(row => row ?? throw new ArgumentNullException(nameof(rows), "Rows cannot contain null entries."))
            .Select(MaterializeBoundWriteRow)
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
            // One atomic write unit, like a SQL statement: rows and their index entries share a commit.
            InsertRowsResult result;
            using (IWriteBatchScope batch = Engine.StorageContext.BeginWriteBatch())
            {
                result = service.InsertRows(databaseName, tableName, materializedRows, txContext: null, statementTxId, recorder);
                recorder?.Publish();
                batch.Complete();
            }

            diagnosticsBuilder?.AddRowsAffected(result.AcceptedRowCount);
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
    /// Inserts one typed row using a strict fast lane. The supplied span is consumed synchronously and
    /// never stored; callers must keep using <see cref="BulkInsert"/> when coercion or partial rows are needed.
    /// </summary>
    public RowKey InsertTyped(string tableName, ReactiveRowSchema columns, ReadOnlySpan<CellValue> row)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(columns);
        if (columns.ColumnCount != row.Length)
        {
            throw new ArgumentException(
                $"Typed row schema has {columns.ColumnCount} columns but row has {row.Length} cells.",
                nameof(row));
        }

        tableName = StoredTableName(tableName);
        using SnapshotLockScope runtimeScope = Engine.EnterRuntimeReadScope();

        RuntimeQueryStatsBuilder? diagnosticsBuilder = null;
        if (Engine.Diagnostics.Enabled)
        {
            diagnosticsBuilder = new RuntimeQueryStatsBuilder
            {
                QueryText = $"TYPED INSERT {tableName}",
                StorageMode = Engine.Config.StorageMode,
                DatabaseName = TryGetCurrentDatabaseName()
            };
            diagnosticsBuilder.SetOperation("TYPED INSERT");
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
            const string transactionError = "InsertTyped cannot run while the current session has an active transaction.";
            diagnosticsBuilder?.RecordError(transactionError);
            throw new InvalidOperationException(transactionError);
        }

        // InsertRowService holds only engine-scoped deps (no per-call state); reuse one per context
        // instead of allocating per insert (Slice 5 P2, lever 5).
        InsertRowService service = _typedInsertService ??= new InsertRowService(
            Engine,
            Engine.StorageContext,
            Engine.Catalog,
            Engine.IndexManager);

        ChangeRecorder? recorder = ChangeRecorder.TryCreate(Engine, databaseName);
        long statementTxId = MvccCoordinator.ResolveStatementTransactionId(Engine, null);
        string tableLockKey = GetRetainedWriteLockKey(databaseName, tableName);
        Engine.LockManager.AcquireRetainedWriteLock(tableLockKey);

        try
        {
            RowKey rowId;
            using (IWriteBatchScope batch = Engine.StorageContext.BeginWriteBatch())
            {
                rowId = service.InsertTypedRow(databaseName, tableName, columns, row, statementTxId, recorder);
                recorder?.Publish();
                batch.Complete();
            }

            diagnosticsBuilder?.AddRowsAffected(1);
            return rowId;
        }
        catch (Exception ex)
        {
            diagnosticsBuilder?.RecordError(ex.Message);
            throw;
        }
        finally
        {
            Engine.LockManager.ReleaseRetainedWriteLock(tableLockKey);
        }
    }

    /// <summary>
    /// Inserts multiple full typed rows under one runtime scope, table write lock, and statement transaction id.
    /// By default the supplied cell arrays are consumed as immutable row buffers and must not be mutated after
    /// this call. With <paramref name="callerOwnsRowBuffers"/> the caller keeps ownership and may reuse the
    /// arrays immediately after the call returns (e.g. pooled row buffers during bulk ingest); the engine
    /// copies a row only when something actually retains it, which only change capture does: every storage
    /// mode reads the cells during the call and keeps its own form of the row.
    /// </summary>
    public IReadOnlyList<RowKey> InsertTypedBatch(
        string tableName,
        ReactiveRowSchema columns,
        IReadOnlyList<CellValue[]> rows,
        bool callerOwnsRowBuffers = false)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(columns);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(rows);
        if (rows.Count == 0)
        {
            return [];
        }

        tableName = StoredTableName(tableName);
        using SnapshotLockScope runtimeScope = Engine.EnterRuntimeReadScope();

        RuntimeQueryStatsBuilder? diagnosticsBuilder = null;
        if (Engine.Diagnostics.Enabled)
        {
            diagnosticsBuilder = new RuntimeQueryStatsBuilder
            {
                QueryText = $"TYPED BATCH INSERT {tableName}",
                StorageMode = Engine.Config.StorageMode,
                DatabaseName = TryGetCurrentDatabaseName()
            };
            diagnosticsBuilder.SetOperation("TYPED BATCH INSERT");
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
            const string transactionError = "InsertTypedBatch cannot run while the current session has an active transaction.";
            diagnosticsBuilder?.RecordError(transactionError);
            throw new InvalidOperationException(transactionError);
        }

        InsertRowService service = _typedInsertService ??= new InsertRowService(
            Engine,
            Engine.StorageContext,
            Engine.Catalog,
            Engine.IndexManager);

        ChangeRecorder? recorder = ChangeRecorder.TryCreate(Engine, databaseName);
        long statementTxId = MvccCoordinator.ResolveStatementTransactionId(Engine, null);
        string tableLockKey = GetRetainedWriteLockKey(databaseName, tableName);
        Engine.LockManager.AcquireRetainedWriteLock(tableLockKey);

        try
        {
            IReadOnlyList<RowKey> rowIds;
            using (IWriteBatchScope batch = Engine.StorageContext.BeginWriteBatch())
            {
                rowIds = service.InsertTypedRows(databaseName, tableName, columns, rows, statementTxId, recorder, callerOwnsRowBuffers);
                recorder?.Publish();
                batch.Complete();
            }

            diagnosticsBuilder?.AddRowsAffected(rowIds.Count);
            return rowIds;
        }
        catch (Exception ex)
        {
            diagnosticsBuilder?.RecordError(ex.Message);
            throw;
        }
        finally
        {
            Engine.LockManager.ReleaseRetainedWriteLock(tableLockKey);
        }
    }

    private string StoredTableName(string tableName)
    {
        string? databaseName = Engine.Sessions.Get(SessionId);
        return databaseName is null ? tableName : Engine.Catalog.FindTableName(tableName, databaseName) ?? tableName;
    }

    private string GetRetainedWriteLockKey(string databaseName, string tableName)
    {
        (string DatabaseName, string TableName) lookupKey = (databaseName, tableName);
        if (_retainedWriteLockKeys.TryGetValue(lookupKey, out string? tableLockKey))
        {
            return tableLockKey;
        }

        tableLockKey = $"{databaseName}.{tableName}";
        _retainedWriteLockKeys[lookupKey] = tableLockKey;
        return tableLockKey;
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
    /// Executes a nearest-neighbor vector search in the current session database.
    /// </summary>
    /// <param name="tableName">The table containing the indexed vector column.</param>
    /// <param name="indexName">The vector index name.</param>
    /// <param name="queryVector">The query vector.</param>
    /// <param name="topK">The number of nearest rows to return.</param>
    /// <returns>The matching table rows in ranked order.</returns>
    public List<Dictionary<string, object?>> SearchNearest(string tableName, string indexName, float[] queryVector, int topK = 10)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(queryVector);
        return SearchNearestCore(tableName, indexName, queryVector.AsSpan(), topK);
    }

    /// <summary>
    /// Executes a nearest-neighbor vector search in the current session database.
    /// </summary>
    /// <param name="tableName">The table containing the indexed vector column.</param>
    /// <param name="indexName">The vector index name.</param>
    /// <param name="queryVector">The query vector.</param>
    /// <param name="topK">The number of nearest rows to return.</param>
    /// <returns>The matching table rows in ranked order.</returns>
    public List<Dictionary<string, object?>> SearchNearest(string tableName, string indexName, ReadOnlySpan<float> queryVector, int topK = 10) =>
        SearchNearestCore(tableName, indexName, queryVector, topK);

    private List<Dictionary<string, object?>> SearchNearestCore(string tableName, string indexName, ReadOnlySpan<float> queryVector, int topK)
    {
        tableName = StoredTableName(tableName);
        using SnapshotLockScope runtimeScope = Engine.EnterRuntimeReadScope();
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
        List<RowKey> rowIds;
        try
        {
            string indexKind = Engine.Catalog
                .GetTableIndexes(tableName, databaseName)
                .FirstOrDefault(index => index.IndexFileName.Equals(indexName, StringComparison.OrdinalIgnoreCase))
                ?.IndexKind ?? "HNSW";

            rowIds = Engine.IndexManager.SearchVector(queryVector, topK, indexName, tableName, databaseName, indexKind);
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
            Dictionary<RowKey, Dictionary<string, object?>> rows = Engine.StorageContext.GetTableContents(rowIds, tableName, databaseName);
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
            return SearchNearestCore(tableName, indexName, parsedVector, topK);
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

    internal object? BindValueForWrite(object? value) => _valueConverters.BindForLegacyWrite(value);

    private IReadOnlyDictionary<string, object?> MaterializeBoundWriteRow(IReadOnlyDictionary<string, object?> row)
    {
        var materialized = new Dictionary<string, object?>(row.Count, StringComparer.OrdinalIgnoreCase);
        foreach ((string columnName, object? value) in row)
        {
            materialized[columnName] = BindValueForWrite(value);
        }

        return materialized;
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
        // The engine may be shared with other contexts on the same directory, so each context must
        // release its reference exactly once.
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        Engine.Dispose();
    }
}
