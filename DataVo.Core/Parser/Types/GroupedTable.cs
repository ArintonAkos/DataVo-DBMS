using System.Collections;

namespace DataVo.Core.Parser.Types
{
    /// <summary>
    /// Represents grouped rows keyed by a group hash.
    /// </summary>
    public class GroupedTable : IEnumerable<KeyValuePair<string, ListedTable>>
    {
        private readonly Dictionary<string, ListedTable> _tables = [];

        /// <summary>
        /// Returns a typed enumerator for grouped entries.
        /// </summary>
        /// <returns>An enumerator of group hash and grouped table pairs.</returns>
        public IEnumerator<KeyValuePair<string, ListedTable>> GetEnumerator()
        {
            return _tables.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Adds a grouped table under the provided hash key.
        /// </summary>
        /// <param name="hash">The group hash key.</param>
        /// <param name="table">The grouped rows.</param>
        public void Add(string hash, ListedTable table)
        {
            _tables.Add(hash, table);
        }

        /// <summary>
        /// Determines whether a group hash is present.
        /// </summary>
        /// <param name="hash">The group hash key.</param>
        /// <returns><see langword="true"/> if the hash exists; otherwise <see langword="false"/>.</returns>
        public bool ContainsKey(string hash)
        {
            return _tables.ContainsKey(hash);
        }

        /// <summary>
        /// Gets or sets grouped rows by hash key.
        /// </summary>
        /// <param name="hash">The group hash key.</param>
        /// <returns>The grouped rows for the hash.</returns>
        public ListedTable this[string hash]
        {
            get { return _tables[hash]; }
            set { _tables[hash] = value; }
        }
    }
}
