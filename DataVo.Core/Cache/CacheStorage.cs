namespace DataVo.Core.Cache;

/// <summary>
/// Stores process-local session state used by legacy execution flows.
/// </summary>
/// <remarks>
/// The cache currently maps a session identifier to its active database name.
/// Newer engine-scoped flows prefer <c>SessionDatabaseStore</c>, but this type remains in use by
/// compatibility paths and helper utilities.
/// </remarks>
internal static class CacheStorage
{
    /// <summary>
    /// Holds the active database name for each known session.
    /// </summary>
    private static readonly Dictionary<Guid, string> Cache = [];

    /// <summary>
    /// Gets the active database bound to a session.
    /// </summary>
    /// <param name="key">The session identifier.</param>
    /// <returns>The active database name, or <see langword="null"/> when the session is unbound.</returns>
    public static string? Get(Guid key)
    {
        return Cache.TryGetValue(key, out string? session)
            ? session
            : null;
    }

    /// <summary>
    /// Binds a session to an active database name.
    /// </summary>
    /// <param name="key">The session identifier.</param>
    /// <param name="value">The database name to associate with the session.</param>
    public static void Set(Guid key, string value)
    {
        Cache[key] = value;
    }

    /// <summary>
    /// Removes all cached session bindings.
    /// </summary>
    public static void Clear()
    {
        Cache.Clear();
    }
}