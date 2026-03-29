using DataVo.Core.Parser.Statements;
using System.Collections;

namespace DataVo.Core.Parser.Types
{
    /// <summary>
    /// Represents a materialized list of joined rows.
    /// </summary>
    public class ListedTable : IEnumerable<JoinedRow>
    {
        private readonly List<JoinedRow> _tables = [];

        /// <summary>
        /// Initializes an empty listed table.
        /// </summary>
        public ListedTable()
        {
            _tables = [];
        }

        /// <summary>
        /// Initializes a listed table from an existing row list.
        /// </summary>
        /// <param name="tables">Rows to include.</param>
        public ListedTable(List<JoinedRow> tables)
        {
            _tables = tables;
        }

        /// <summary>
        /// Gets or sets the row at a given index.
        /// </summary>
        /// <param name="index">Zero-based row index.</param>
        /// <returns>The joined row at the provided index.</returns>
        public JoinedRow this[int index]
        {
            get { return _tables[index]; }
            set { _tables[index] = value; }
        }

        /// <summary>
        /// Gets the number of rows in the table.
        /// </summary>
        public int Count
        {
            get { return _tables.Count; }
        }

        /// <summary>
        /// Returns the row at the specified index.
        /// </summary>
        /// <param name="index">Zero-based row index.</param>
        /// <returns>The joined row at the provided index.</returns>
        public JoinedRow Get(int index)
        {
            return _tables[index];
        }

        /// <summary>
        /// Appends a row to the table.
        /// </summary>
        /// <param name="row">The row to append.</param>
        public void Add(JoinedRow row)
        {
            _tables.Add(row);
        }

        /// <summary>
        /// Returns a generic row enumerator.
        /// </summary>
        /// <returns>An enumerator over joined rows.</returns>
        public IEnumerator<JoinedRow> GetEnumerator()
        {
            return _tables.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Converts this listed table to a single-bucket grouped table.
        /// </summary>
        /// <returns>A grouped table keyed by the hash-group placeholder.</returns>
        public GroupedTable ToGroupedTable()
        {
            GroupedTable groupedTable = new()
            {
                { GroupBy.HASH_VALUE, new() }
            };

            foreach (var row in _tables)
            {
                groupedTable[GroupBy.HASH_VALUE].Add(row);
            }

            return groupedTable;
        }
    }
}
