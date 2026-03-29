using DataVo.Core.Parser.Types;

namespace DataVo.Core.Utils
{
    /// <summary>
    /// Extension helpers for converting joined-row collections into listed tables.
    /// </summary>
    public static class ListExtensions
    {
        /// <summary>
        /// Converts an enumerable sequence of joined rows into a listed table.
        /// </summary>
        /// <param name="list">The source sequence.</param>
        /// <returns>A listed table containing the sequence items.</returns>
        public static ListedTable ToListedTable(this IEnumerable<JoinedRow> list)
        {
            return new ListedTable(list.ToList());
        }

        /// <summary>
        /// Converts a list of joined rows into a listed table.
        /// </summary>
        /// <param name="list">The source list.</param>
        /// <returns>A listed table containing the list items.</returns>
        public static ListedTable ToListedTable(this List<JoinedRow> list)
        {
            return new ListedTable(list);
        }
    }
}
