using System.Collections.Concurrent;

namespace DataVo.Core.Runtime;

/// <summary>
/// Stores the currently selected database for each logical session within an engine instance.
/// </summary>
public sealed class SessionDatabaseStore
{
    private readonly ConcurrentDictionary<Guid, string> _selectedDatabases = new();

    /// <summary>
    /// Gets the selected database for the provided session.
    /// </summary>
    /// <param name="session">The logical session identifier.</param>
    /// <returns>The selected database name, or <see langword="null"/> if none is bound.</returns>
    public string? Get(Guid session)
    {
        return _selectedDatabases.GetValueOrDefault(session);
    }

    /// <summary>
    /// Sets the selected database for the provided session.
    /// </summary>
    /// <param name="session">The logical session identifier.</param>
    /// <param name="databaseName">The database name to bind.</param>
    public void Set(Guid session, string databaseName)
    {
        _selectedDatabases[session] = databaseName;
    }

    /// <summary>
    /// Removes all tracked session-to-database bindings.
    /// </summary>
    public void Clear()
    {
        _selectedDatabases.Clear();
    }
}