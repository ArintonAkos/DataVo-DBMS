using System.Collections;

namespace DataVo.Core.Parser.Types
{
    /// <summary>
    /// Represents joined rows indexed by composite joined-row identifiers.
    /// </summary>
    public class HashedTable : IEnumerable<KeyValuePair<JoinedRowId, JoinedRow>>
    {
        private readonly Dictionary<JoinedRowId, JoinedRow> _rows;

        /// <summary>
        /// Initializes an empty hashed table.
        /// </summary>
        public HashedTable()
        {
            _rows = [];
        }

        /// <summary>
        /// Initializes a hashed table from existing keyed rows.
        /// </summary>
        /// <param name="rows">Rows keyed by joined-row identifier.</param>
        public HashedTable(Dictionary<JoinedRowId, JoinedRow> rows)
        {
            _rows = rows;
        }

        /// <summary>
        /// Adds a keyed row.
        /// </summary>
        /// <param name="hash">The composite row identifier.</param>
        /// <param name="row">The joined row payload.</param>
        public void Add(JoinedRowId hash, JoinedRow row)
        {
            _rows.Add(hash, row);
        }

        /// <summary>
        /// Determines whether a row key exists.
        /// </summary>
        /// <param name="hash">The composite row identifier.</param>
        /// <returns><see langword="true"/> when the key exists; otherwise <see langword="false"/>.</returns>
        public bool ContainsKey(JoinedRowId hash)
        {
            return _rows.ContainsKey(hash);
        }

        /// <summary>
        /// Gets all row keys in the table.
        /// </summary>
        public IEnumerable<JoinedRowId> Keys
        {
            get { return _rows.Keys; }
        }

        /// <summary>
        /// Gets or sets a joined row by composite row key.
        /// </summary>
        /// <param name="hash">The composite row identifier.</param>
        /// <returns>The row mapped to the key.</returns>
        public JoinedRow this[JoinedRowId hash]
        {
            get { return _rows[hash]; }
            set { _rows[hash] = value; }
        }

        /// <summary>
        /// Gets the number of keyed rows.
        /// </summary>
        public int Count
        {
            get { return _rows.Count; }
        }

        /// <summary>
        /// Returns the first keyed row.
        /// </summary>
        /// <returns>The first key/value pair in enumeration order.</returns>
        public KeyValuePair<JoinedRowId, JoinedRow> First()
        {
            return _rows.First();
        }

        /// <summary>
        /// Gets a row by key.
        /// </summary>
        /// <param name="hash">The composite row identifier.</param>
        /// <returns>The row mapped to the key.</returns>
        public JoinedRow Get(JoinedRowId hash)
        {
            return _rows[hash];
        }

        /// <summary>
        /// Materializes the keyed rows as a listed table.
        /// </summary>
        /// <returns>A listed table containing all joined rows.</returns>
        public ListedTable ToListedTable()
        {
            return new ListedTable(_rows.Select(row => row.Value).ToList());
        }

        /// <summary>
        /// Returns a typed enumerator for keyed rows.
        /// </summary>
        /// <returns>An enumerator of joined row key/value pairs.</returns>
        public IEnumerator<KeyValuePair<JoinedRowId, JoinedRow>> GetEnumerator()
        {
            return _rows.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
