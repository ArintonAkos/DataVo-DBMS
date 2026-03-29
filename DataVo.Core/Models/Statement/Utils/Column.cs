using DataVo.Core.Contracts;
using DataVo.Core.Runtime;

namespace DataVo.Core.Models.Statement.Utils
{
    /// <summary>
    /// Represents a resolved column reference used in statement models.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="columnName">The column name.</param>
    public class Column(string databaseName, string tableName, string columnName) : IColumn
    {
        /// <summary>
        /// Gets or sets the database name.
        /// </summary>
        public string DatabaseName { get; set; } = databaseName;

        /// <summary>
        /// Gets or sets the table name.
        /// </summary>
        public string TableName { get; set; } = tableName;

        /// <summary>
        /// Gets or sets the column name.
        /// </summary>
        public string ColumnName { get; set; } = columnName;

        /// <summary>
        /// Resolves the raw catalog type string for this column.
        /// </summary>
        /// <returns>The raw column type.</returns>
        public string RawType()
        {
            return DataVoEngine.Current().Catalog.GetTableColumnType(TableName, DatabaseName, ColumnName);
        }
    }
}
