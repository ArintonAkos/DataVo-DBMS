namespace DataVo.Core.Parser.Types
{
    /// <summary>
    /// Represents a single logical row as a dictionary of column/value pairs.
    /// </summary>
    public class Row
    {
        private readonly Dictionary<string, object?> _cells = [];

        /// <summary>
        /// Initializes an empty row.
        /// </summary>
        public Row()
        {
            _cells = [];
        }

        /// <summary>
        /// Initializes a row with existing cell values.
        /// </summary>
        /// <param name="cells">Initial cells mapped by column name.</param>
        public Row(Dictionary<string, object?> cells)
        {
            _cells = cells;
        }

        /// <summary>
        /// Determines whether the row contains the specified column.
        /// </summary>
        /// <param name="key">The column name.</param>
        /// <returns><see langword="true"/> when the column exists; otherwise <see langword="false"/>.</returns>
        public bool ContainsKey(string key)
        {
            return _cells.ContainsKey(key);
        }

        /// <summary>
        /// Gets all column names present in the row.
        /// </summary>
        public IEnumerable<string> Keys
        {
            get { return _cells.Keys; }
        }

        /// <summary>
        /// Gets or sets a value by column name.
        /// </summary>
        /// <param name="key">The column name.</param>
        /// <returns>The stored value.</returns>
        public object? this[string key]
        {
            get { return _cells[key]; }
            set { _cells[key] = value; }
        }

        /// <summary>
        /// Gets a value by column name.
        /// </summary>
        /// <param name="key">The column name.</param>
        /// <returns>The stored value.</returns>
        public object? Get(string key)
        {
            return _cells[key];
        }

        /// <summary>
        /// Adds a new column/value entry to the row.
        /// </summary>
        /// <param name="cellName">The column name.</param>
        /// <param name="value">The value to store.</param>
        public void Add(string cellName, object? value)
        {
            _cells.Add(cellName, value);
        }
    }
}
