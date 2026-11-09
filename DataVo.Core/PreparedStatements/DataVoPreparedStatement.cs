using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser;
using DataVo.Core.Parser.AST;
using DataVo.Core.PreparedStatements;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Diagnostics;
using DataVo.Core.StorageEngine;

namespace DataVo.Core;

/// <summary>
/// Represents a pre-parsed, reusable SQL statement or statement batch compiled against a <see cref="DataVoContext"/>.
/// </summary>
/// <remarks>
/// <para>
/// Preparing a SQL statement runs the lexer and parser once and builds reusable execution plans for
/// single-table point lookups, indexed lookups, range scans, and primary-key mutations.
/// </para>
/// <para>
/// Instances of <see cref="DataVoPreparedStatement"/> are thread-safe: parameter binding and result
/// materialization operate on per-invocation state or thread-local workspaces without mutating the shared
/// template AST.
/// </para>
/// <para>
/// <c>Execute</c> returns a result the caller owns. <c>ExecuteBorrowed</c> avoids that copy for hot loops: it
/// may return a reused result that is valid only until the next execution on the same thread.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// DataVoPreparedStatement lookup = context.Prepare("SELECT Id, Name FROM Users WHERE Id = @id");
/// QueryResult row = lookup.Execute(("id", 42));
/// </code>
/// </example>
public sealed partial class DataVoPreparedStatement
{
    private readonly DataVoEngine _engine;
    private readonly Guid _defaultSessionId;
    private readonly List<SqlStatement>? _statements;
    private readonly Exception? _parseException;
    private readonly string _inferredOperation;

    internal DataVoPreparedStatement(DataVoContext context, string sql, Guid sessionId)
        : this(context?.Engine ?? throw new ArgumentNullException(nameof(context)), sql, sessionId)
    {
    }

    /// <summary>
    /// Initializes a new pre-parsed <see cref="DataVoPreparedStatement"/> against the specified engine and default session.
    /// </summary>
    /// <param name="engine">The target database engine.</param>
    /// <param name="sql">The SQL statement or batch text to prepare.</param>
    /// <param name="sessionId">The default session identifier used when executing without an explicit session.</param>
    /// <example>
    /// <code>
    /// var prepared = new DataVoPreparedStatement(engine, "SELECT * FROM Users WHERE Id = @id", sessionId);
    /// QueryResult result = prepared.Execute(("id", 1));
    /// </code>
    /// </example>
    public DataVoPreparedStatement(DataVoEngine engine, string sql, Guid sessionId)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(engine);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(sql);

        _engine = engine;
        _defaultSessionId = sessionId;
        Sql = sql;
        _inferredOperation = InferOperation(sql);

        try
        {
            var lexer = new Lexer(sql);
            List<Token> tokens = lexer.Tokenize();
            var parser = new Parser.Parser(tokens);
            _statements = parser.Parse();
            InitializeFastPathCandidates();
        }
        catch (Exception ex)
        {
            _parseException = ex;
        }
    }

    /// <summary>
    /// Gets the SQL text that was used to prepare this statement.
    /// </summary>
    public string Sql { get; }

    /// <summary>
    /// Executes the prepared statement with no parameters and returns the final <see cref="QueryResult"/>.
    /// </summary>
    /// <returns>The execution result of the statement.</returns>
    public QueryResult Execute()
        => Execute(ReadOnlySpan<DataVoQueryParameter>.Empty, _defaultSessionId);

    /// <summary>
    /// Executes the prepared statement with one parameter and returns the final <see cref="QueryResult"/>.
    /// </summary>
    /// <param name="p0">The parameter binding.</param>
    /// <returns>The execution result of the statement.</returns>
    /// <example>
    /// <code>
    /// QueryResult result = prepared.Execute(("id", 5));
    /// </code>
    /// </example>
    public QueryResult Execute(DataVoQueryParameter p0)
    {
        return Execute(SpanOf(in p0), _defaultSessionId);
    }

    /// <summary>
    /// Executes the prepared statement with two parameters and returns the final <see cref="QueryResult"/>.
    /// </summary>
    /// <param name="p0">The first parameter binding.</param>
    /// <param name="p1">The second parameter binding.</param>
    /// <returns>The execution result of the statement.</returns>
    public QueryResult Execute(DataVoQueryParameter p0, DataVoQueryParameter p1)
    {
        TwoParams buffer = new(p0, p1);
        return Execute(buffer.AsSpan(), _defaultSessionId);
    }

    /// <summary>
    /// Executes the prepared statement with three parameters and returns the final <see cref="QueryResult"/>.
    /// </summary>
    /// <param name="p0">The first parameter binding.</param>
    /// <param name="p1">The second parameter binding.</param>
    /// <param name="p2">The third parameter binding.</param>
    /// <returns>The execution result of the statement.</returns>
    public QueryResult Execute(DataVoQueryParameter p0, DataVoQueryParameter p1, DataVoQueryParameter p2)
    {
        ThreeParams buffer = new(p0, p1, p2);
        return Execute(buffer.AsSpan(), _defaultSessionId);
    }

    /// <summary>
    /// Executes the prepared statement with four parameters and returns the final <see cref="QueryResult"/>.
    /// </summary>
    /// <param name="p0">The first parameter binding.</param>
    /// <param name="p1">The second parameter binding.</param>
    /// <param name="p2">The third parameter binding.</param>
    /// <param name="p3">The fourth parameter binding.</param>
    /// <returns>The execution result of the statement.</returns>
    public QueryResult Execute(DataVoQueryParameter p0, DataVoQueryParameter p1, DataVoQueryParameter p2, DataVoQueryParameter p3)
    {
        FourParams buffer = new(p0, p1, p2, p3);
        return Execute(buffer.AsSpan(), _defaultSessionId);
    }

    /// <summary>
    /// Executes the prepared statement with the supplied parameter array and returns the final <see cref="QueryResult"/>.
    /// </summary>
    /// <param name="parameters">The parameter bindings.</param>
    /// <returns>The execution result of the statement.</returns>
    public QueryResult Execute(params DataVoQueryParameter[] parameters)
        => Execute(parameters.AsSpan(), _defaultSessionId);

    /// <summary>
    /// Executes the prepared statement with a span of parameters and returns the final <see cref="QueryResult"/>.
    /// </summary>
    /// <param name="parameters">The parameter bindings.</param>
    /// <returns>The execution result of the statement.</returns>
    public QueryResult Execute(ReadOnlySpan<DataVoQueryParameter> parameters)
        => Execute(parameters, _defaultSessionId);

    /// <summary>
    /// Executes the prepared statement against an explicit session identifier using a span of parameters.
    /// </summary>
    /// <param name="parameters">The parameter bindings.</param>
    /// <param name="sessionId">The session whose database binding and transaction context should be used.</param>
    /// <returns>The execution result of the statement.</returns>
    public QueryResult Execute(ReadOnlySpan<DataVoQueryParameter> parameters, Guid sessionId)
    {
        using SnapshotLockScope _ = _engine.EnterRuntimeReadScope();

        return TryExecuteOnFastPath(parameters, sessionId, out QueryResult? fastResult)
            ? ToOwnedResult(fastResult)
            : ExecuteAllCore(parameters, sessionId)[^1];
    }

    /// <summary>
    /// Executes the prepared statement with one parameter without allocating a result when a fast path answers it.
    /// </summary>
    /// <param name="p0">The parameter binding.</param>
    /// <returns>A result to read immediately. See <see cref="ExecuteBorrowed(ReadOnlySpan{DataVoQueryParameter}, Guid)"/>.</returns>
    /// <example>
    /// <code>
    /// long qty = (int)prepared.ExecuteBorrowed(("id", 5)).Data[0]["Qty"]!;
    /// </code>
    /// </example>
    public QueryResult ExecuteBorrowed(DataVoQueryParameter p0)
        => ExecuteBorrowed(SpanOf(in p0), _defaultSessionId);

    /// <summary>
    /// Executes the prepared statement against an explicit session without allocating a result when a fast path answers it.
    /// </summary>
    /// <param name="parameters">The parameter bindings.</param>
    /// <param name="sessionId">The session whose database binding and transaction context should be used.</param>
    /// <returns>
    /// A result that may be shared and reused: treat it as read-only, on the calling thread, and do not use it, its
    /// rows or its boxed values after the next execution on this thread. Use an <c>Execute</c> overload to keep a result.
    /// </returns>
    /// <example>
    /// <code>
    /// QueryResult row = prepared.ExecuteBorrowed([("id", 5)], sessionId);
    /// Consume(row.Data[0]);
    /// </code>
    /// </example>
    public QueryResult ExecuteBorrowed(ReadOnlySpan<DataVoQueryParameter> parameters, Guid sessionId)
    {
        using SnapshotLockScope _ = _engine.EnterRuntimeReadScope();

        return TryExecuteOnFastPath(parameters, sessionId, out QueryResult? fastResult)
            ? fastResult
            : ExecuteAllCore(parameters, sessionId)[^1];
    }

    /// <summary>
    /// Executes all statements in the prepared batch and returns the full list of <see cref="QueryResult"/> objects.
    /// </summary>
    /// <param name="parameters">The parameter bindings for the batch.</param>
    /// <returns>The sequence of results produced by each statement in the batch.</returns>
    public List<QueryResult> ExecuteAll(ReadOnlySpan<DataVoQueryParameter> parameters)
        => ExecuteAll(parameters, _defaultSessionId);

    /// <summary>
    /// Executes all statements in the prepared batch against an explicit session identifier.
    /// </summary>
    /// <param name="parameters">The parameter bindings for the batch.</param>
    /// <param name="sessionId">The session whose database binding and transaction context should be used.</param>
    /// <returns>The sequence of results produced by each statement in the batch.</returns>
    public List<QueryResult> ExecuteAll(ReadOnlySpan<DataVoQueryParameter> parameters, Guid sessionId)
    {
        using SnapshotLockScope _ = _engine.EnterRuntimeReadScope();

        return TryExecuteOnFastPath(parameters, sessionId, out QueryResult? fastResult)
            ? [ToOwnedResult(fastResult)]
            : ExecuteAllCore(parameters, sessionId);
    }

    /// <summary>
    /// Runs a fast path when one applies. A failure on it is reported as an error result, as the general path does.
    /// </summary>
    private bool TryExecuteOnFastPath(
        ReadOnlySpan<DataVoQueryParameter> parameters,
        Guid sessionId,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out QueryResult? result)
    {
        result = null;
        if (_parseException is not null
            || _statements is not { Count: 1 }
            || _engine.Diagnostics.Enabled
            || _engine.TransactionManager.HasActiveTransaction(sessionId))
        {
            return false;
        }

        try
        {
            return TryExecuteFastPath(parameters, sessionId, out result) && result is not null;
        }
        catch (Exception ex)
        {
            result = QueryResult.FromException(ex);
            return true;
        }
    }

    /// <summary>
    /// Copies a fast-path result into objects only the caller holds. A single-row result can be a reused slot
    /// whose boxed numbers are overwritten in place, so those values are boxed afresh; multi-row results are
    /// always built per execution.
    /// </summary>
    private static QueryResult ToOwnedResult(QueryResult result)
    {
        var owned = new QueryResult
        {
            Fields = [.. result.Fields],
            Messages = [.. result.Messages],
            ExecutionTime = result.ExecutionTime,
            IsError = result.IsError,
            ErrorCode = result.ErrorCode,
            Data = result.Data,
        };

        if (result.Data.Count <= 1)
        {
            owned.Data = new List<Dictionary<string, object?>>(result.Data.Count);
            foreach (Dictionary<string, object?> row in result.Data)
            {
                var copy = new Dictionary<string, object?>(row.Count);
                foreach (KeyValuePair<string, object?> cell in row)
                {
                    copy[cell.Key] = cell.Value switch
                    {
                        int value => value,
                        float value => value,
                        Guid value => value,
                        var other => other,
                    };
                }

                owned.Data.Add(copy);
            }
        }

        return owned;
    }

    private static ReadOnlySpan<DataVoQueryParameter> SpanOf(in DataVoQueryParameter parameter)
    {
#if NET8_0_OR_GREATER
        return new ReadOnlySpan<DataVoQueryParameter>(in parameter);
#else
        return new[] { parameter };
#endif
    }

    private List<QueryResult> ExecuteAllCore(ReadOnlySpan<DataVoQueryParameter> parameters, Guid sessionId)
    {
        using var _ = DataVoEngine.PushCurrent(_engine);
        string? databaseName = _engine.Sessions.Get(sessionId);
        RuntimeQueryStatsBuilder? diagnosticsBuilder = null;
        if (_engine.Diagnostics.Enabled)
        {
            diagnosticsBuilder = new RuntimeQueryStatsBuilder
            {
                QueryText = Sql,
                StorageMode = _engine.Config.StorageMode,
                DatabaseName = databaseName,
            };
            diagnosticsBuilder.SetOperation(_inferredOperation);
        }

        using RuntimeQueryDiagnosticsScope? diagnosticsScope =
            RuntimeQueryDiagnosticsScope.Start(_engine.Diagnostics, diagnosticsBuilder);

        if (_parseException is not null)
        {
            diagnosticsBuilder?.RecordError(_parseException.Message);
            return [QueryResult.FromException(_parseException)];
        }

        if (_statements is null || _statements.Count == 0)
        {
            return
            [
                new QueryResult
                {
                    Messages = ["The query contains no statement."],
                    IsError = true,
                    ErrorCode = DataVoErrorCodes.EmptyStatement,
                }
            ];
        }

        List<Queue<IDbAction>> runnables;
        try
        {
            List<SqlStatement> boundStatements = SqlParameterBinder.BindStatements(_statements, parameters);
            var evaluator = new Evaluator(boundStatements, _engine);
            runnables = evaluator.ToRunnables();
        }
        catch (Exception ex)
        {
            diagnosticsBuilder?.RecordError(ex.Message);
            return [QueryResult.FromException(ex)];
        }

        List<QueryResult> response = new(_statements.Count);
        foreach (Queue<IDbAction> runnable in runnables)
        {
            ExecuteRunnableQueue(runnable, sessionId, response, diagnosticsBuilder);
        }

        return response;
    }

    private void ExecuteRunnableQueue(
        Queue<IDbAction> runnable,
        Guid sessionId,
        List<QueryResult> response,
        RuntimeQueryStatsBuilder? diagnosticsBuilder)
    {
        while (runnable.Count != 0)
        {
            try
            {
                QueryResult result;
                using (IWriteBatchScope batch = _engine.StorageContext.BeginWriteBatch())
                {
                    IDbAction action = runnable.Dequeue();
                    result = action.Perform(sessionId);
                    if (!result.IsError || IsSchemaAction(action))
                    {
                        batch.Complete();
                    }
                }

                response.Add(result);
                if (diagnosticsBuilder is not null)
                {
                    RecordResultMetrics(diagnosticsBuilder, result);
                }
            }
            catch (Exception ex)
            {
                diagnosticsBuilder?.RecordError(ex.Message);
                response.Add(QueryResult.FromException(ex));
                break;
            }
        }
    }

    private static bool IsSchemaAction(IDbAction action) =>
        action.GetType().Namespace?.EndsWith(".DDL", StringComparison.Ordinal) == true;

    private static void RecordResultMetrics(RuntimeQueryStatsBuilder builder, QueryResult result)
    {
        if (result.IsError)
        {
            builder.RecordError(string.Join(" | ", result.Messages));
        }

        if (result.Data.Count > 0)
        {
            builder.AddRowsReturned(result.Data.Count);
        }

        foreach (string message in result.Messages)
        {
            if (TryReadMessageCount(message, "Rows affected:", out int affected))
            {
                builder.AddRowsAffected(affected);
                continue;
            }

            if (result.Data.Count == 0 && TryReadMessageCount(message, "Rows selected:", out int selected))
            {
                builder.AddRowsReturned(selected);
            }
        }
    }

    private static bool TryReadMessageCount(string message, string prefix, out int count)
    {
        count = 0;
        return message.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            && int.TryParse(message[prefix.Length..].Trim(), out count);
    }

    private static string InferOperation(string sql)
    {
        string trimmed = sql.TrimStart();
        if (trimmed.Length == 0)
        {
            return "UNKNOWN";
        }

        int end = 0;
        while (end < trimmed.Length && !char.IsWhiteSpace(trimmed[end]) && trimmed[end] != ';')
        {
            end++;
        }

        return trimmed[..end].ToUpperInvariant();
    }

#if NET8_0_OR_GREATER
    [System.Runtime.CompilerServices.InlineArray(2)]
    private struct TwoParams
    {
        private DataVoQueryParameter _element0;

        public TwoParams(DataVoQueryParameter p0, DataVoQueryParameter p1)
        {
            this[0] = p0;
            this[1] = p1;
        }

        public ReadOnlySpan<DataVoQueryParameter> AsSpan()
            => System.Runtime.InteropServices.MemoryMarshal.CreateReadOnlySpan(ref _element0, 2);
    }

    [System.Runtime.CompilerServices.InlineArray(3)]
    private struct ThreeParams
    {
        private DataVoQueryParameter _element0;

        public ThreeParams(DataVoQueryParameter p0, DataVoQueryParameter p1, DataVoQueryParameter p2)
        {
            this[0] = p0;
            this[1] = p1;
            this[2] = p2;
        }

        public ReadOnlySpan<DataVoQueryParameter> AsSpan()
            => System.Runtime.InteropServices.MemoryMarshal.CreateReadOnlySpan(ref _element0, 3);
    }

    [System.Runtime.CompilerServices.InlineArray(4)]
    private struct FourParams
    {
        private DataVoQueryParameter _element0;

        public FourParams(DataVoQueryParameter p0, DataVoQueryParameter p1, DataVoQueryParameter p2, DataVoQueryParameter p3)
        {
            this[0] = p0;
            this[1] = p1;
            this[2] = p2;
            this[3] = p3;
        }

        public ReadOnlySpan<DataVoQueryParameter> AsSpan()
            => System.Runtime.InteropServices.MemoryMarshal.CreateReadOnlySpan(ref _element0, 4);
    }
#else
    private readonly struct TwoParams(DataVoQueryParameter p0, DataVoQueryParameter p1)
    {
        private readonly DataVoQueryParameter[] _items = [p0, p1];

        public ReadOnlySpan<DataVoQueryParameter> AsSpan() => _items;
    }

    private readonly struct ThreeParams(DataVoQueryParameter p0, DataVoQueryParameter p1, DataVoQueryParameter p2)
    {
        private readonly DataVoQueryParameter[] _items = [p0, p1, p2];

        public ReadOnlySpan<DataVoQueryParameter> AsSpan() => _items;
    }

    private readonly struct FourParams(DataVoQueryParameter p0, DataVoQueryParameter p1, DataVoQueryParameter p2, DataVoQueryParameter p3)
    {
        private readonly DataVoQueryParameter[] _items = [p0, p1, p2, p3];

        public ReadOnlySpan<DataVoQueryParameter> AsSpan() => _items;
    }
#endif
}
