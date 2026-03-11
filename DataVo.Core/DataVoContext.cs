using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;

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
    /// Releases resources owned by the underlying engine.
    /// </summary>
    public void Dispose()
    {
        Engine.Dispose();
    }
}
