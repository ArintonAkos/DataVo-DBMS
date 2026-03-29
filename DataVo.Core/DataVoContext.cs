using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
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
    /// Gets or sets the default session identifier used by <see cref="Execute(string)"/>.
    /// </summary>
    public Guid SessionId { get; set; }

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
        return new QueryEngine(query, sessionId, Engine).Parse();
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
        string databaseName = ResolveCurrentDatabase();
        using var _ = DataVoEngine.PushCurrent(Engine);

        List<long> rowIds;
        rowIds = Engine.IndexManager.SearchVector(queryVector, topK, indexName, tableName, databaseName);
        if (rowIds.Count == 0)
        {
            return [];
        }

        Dictionary<long, Dictionary<string, object?>> rows = Engine.StorageContext.GetTableContents(rowIds, tableName, databaseName);
        return rowIds
            .Where(rows.ContainsKey)
            .Select(id => rows[id])
            .ToList();
    }

    /// <summary>
    /// Executes a nearest-neighbor vector search using a vector literal formatted as <c>[x,y,z]</c>.
    /// </summary>
    public List<Dictionary<string, object?>> SearchNearest(string tableName, string indexName, string queryVector, int topK = 10)
    {
        if (!VectorParser.TryParseVector(queryVector, out float[] parsedVector))
        {
            throw new ArgumentException("Query vector must be in format [x,y,z].", nameof(queryVector));
        }

        return SearchNearest(tableName, indexName, parsedVector, topK);
    }

    private string ResolveCurrentDatabase()
    {
        string? databaseName = Engine.Sessions.Get(SessionId);
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
