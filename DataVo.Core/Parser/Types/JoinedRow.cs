using System.Collections;

namespace DataVo.Core.Parser.Types
{
    /// <summary>
    /// Represents a logical row composed of per-table row segments.
    /// </summary>
    public class JoinedRow : IEnumerable<KeyValuePair<string, Row>>
    {
        private readonly Dictionary<string, Row> _row = [];

        /// <summary>
        /// Initializes an empty joined row.
        /// </summary>
        public JoinedRow()
        {
            _row = [];
        }

        /// <summary>
        /// Initializes a joined row from an existing table-to-row map.
        /// </summary>
        /// <param name="rows">Per-table row segments.</param>
        public JoinedRow(Dictionary<string, Row> rows)
        {
            _row = rows;
        }

        /// <summary>
        /// Initializes a joined row containing a single table segment.
        /// </summary>
        /// <param name="tableName">The table name key.</param>
        /// <param name="row">The row segment for the table.</param>
        public JoinedRow(string tableName, Row row)
        {
            _row = new()
            {
                { tableName, row }
            };
        }

        /// <summary>
        /// Gets the number of table segments in the joined row.
        /// </summary>
        public int Count
        {
            get { return _row.Count; }
        }

        /// <summary>
        /// Gets the table keys currently present in the joined row.
        /// </summary>
        public IEnumerable<string> Keys
        {
            get { return _row.Keys; }
        }

        /// <summary>
        /// Returns a typed enumerator for table-row pairs.
        /// </summary>
        /// <returns>An enumerator over table and row pairs.</returns>
        public IEnumerator<KeyValuePair<string, Row>> GetEnumerator()
        {
            return _row.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Determines whether a table segment exists in the joined row.
        /// </summary>
        /// <param name="tableName">The table key.</param>
        /// <returns><see langword="true"/> when present; otherwise <see langword="false"/>.</returns>
        public bool ContainsKey(string tableName)
        {
            return _row.ContainsKey(tableName);
        }

        /// <summary>
        /// Gets or sets a table segment by key.
        /// </summary>
        /// <param name="tableName">The table key.</param>
        /// <returns>The table segment row.</returns>
        public Row this[string tableName]
        {
            get { return _row[tableName]; }
            set { _row[tableName] = value; }
        }

        /// <summary>
        /// Adds a table segment.
        /// </summary>
        /// <param name="tableName">The table key.</param>
        /// <param name="row">The row segment.</param>
        public void Add(string tableName, Row row)
        {
            _row.Add(tableName, row);
        }

        /// <summary>
        /// Retrieves a table segment by key.
        /// </summary>
        /// <param name="tableName">The table key.</param>
        /// <returns>The row segment for the table.</returns>
        public Row Get(string tableName)
        {
            return _row[tableName];
        }
    }
}