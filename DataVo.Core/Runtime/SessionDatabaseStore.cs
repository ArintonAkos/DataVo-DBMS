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
    public string? Get(Guid session)
    {
        return _selectedDatabases.GetValueOrDefault(session);
    }

    /// <summary>
    /// Sets the selected database for the provided session.
    /// </summary>
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