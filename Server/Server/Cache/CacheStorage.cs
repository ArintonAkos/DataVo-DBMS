namespace Server.Server.Cache;

internal static class CacheStorage
{
    //                          <SessionKey, DatabaseName>
    private static readonly Dictionary<Guid, string> Cache = new();

    public static string Get(Guid key) => Cache[key];

    public static void Set(Guid key, string value)
    {
        Cache[key] = value;
    }

    public static void Clear()
    {
        Cache.Clear();
    }
}