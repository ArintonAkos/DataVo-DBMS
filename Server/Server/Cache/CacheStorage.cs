using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Cache
{
    internal class CacheStorage
    {
        //                            <SessionKey, DatabaseName>
        private static readonly Dictionary<string, string> _cache = new();

        public static string Get(string key)
        {
            return _cache[key];
        }

        public static void Set(string key, string value)
        {
            _cache[key] = value;
        }

        public static void Clear()
        {
            _cache.Clear();
        }
    }
}
