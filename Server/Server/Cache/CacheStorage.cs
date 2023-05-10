namespace Server.Server.Cache;

internal class CacheStorage
{
    //                            <SessionKey, DatabaseName>
    private static readonly Dictionary<Guid, string> _cache = new();

    public static string Get(Guid key)
    {
        return _cache[key];
    }

    public static void Set(Guid key, string value)
    {
        _cache[key] = value;
    }

    public static void Clear()
    {
        _cache.Clear();
    }
}