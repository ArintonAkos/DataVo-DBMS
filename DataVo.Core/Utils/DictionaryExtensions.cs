using DataVo.Core.Parser.Types;

namespace DataVo.Core.Utils
{
    /// <summary>
    /// Extension helpers for converting dictionary-like structures into parser row containers.
    /// </summary>
    public static class DictionaryExtensions
    {
        /// <summary>
        /// Converts a statement record into a parser row.
        /// </summary>
        /// <param name="record">The source statement record.</param>
        /// <returns>A new row containing the record values.</returns>
        public static Row ToRow(this Models.Statement.Utils.Record record)
        {
            return new Row(record.Values);
        }

        /// <summary>
        /// Converts a dictionary of column values into a parser row.
        /// </summary>
        /// <param name="dictionary">The source value map.</param>
        /// <returns>A new row containing the dictionary entries.</returns>
        public static Row ToRow(this Dictionary<string, object?> dictionary)
        {
            return new Row(dictionary);
        }

        /// <summary>
        /// Converts a keyed joined-row dictionary into a hashed table.
        /// </summary>
        /// <param name="dictionary">The keyed joined-row map.</param>
        /// <returns>A hashed table over the provided dictionary.</returns>
        public static HashedTable ToHashedTable(this Dictionary<JoinedRowId, JoinedRow> dictionary)
        {
            return new HashedTable(dictionary);
        }
    }
}
