using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Services
{
    /// <summary>
    /// Resolves table metadata for aliases, names, and column bindings within a query scope.
    /// </summary>
    public class TableService
    {
        /// <summary>
        /// Gets the database name associated with this service instance.
        /// </summary>
        public string DatabaseName { get; }

        /// <summary>
        /// Gets the known table details by table name or alias in use.
        /// </summary>
        public Dictionary<string, TableDetail> TableDetails { get; private set; } = [];

        /// <summary>
        /// Initializes a new table service bound to a database.
        /// </summary>
        /// <param name="databaseName">The logical database name.</param>
        public TableService(string databaseName)
        {
            DatabaseName = databaseName;
        }

        /// <summary>
        /// Resolves a table by alias or real table name.
        /// </summary>
        /// <param name="aliasOrName">The alias or table name in use.</param>
        /// <returns>The resolved table detail.</returns>
        public TableDetail GetTableDetailByAliasOrName(string aliasOrName)
        {
            foreach (var tableDetail in TableDetails.Values)
            {
                if (tableDetail.TableName == aliasOrName || tableDetail.TableAlias == aliasOrName)
                {
                    return tableDetail;
                }
            }

            throw new BindingException("Table name or alias not found");
        }

        /// <summary>
        /// Resolves the table that owns a given column reference.
        /// </summary>
        /// <param name="column">Column reference, optionally qualified with <c>table.column</c>.</param>
        /// <returns>The resolved table detail.</returns>
        public TableDetail GetTableDetailByColumn(string column)
        {
            string? tableName = null;

            if (column.Contains(value: "."))
            {
                var splitColumn = column.Split('.');

                tableName = splitColumn[0];
            }

            if (tableName != null)
            {
                if (!TableDetails.ContainsKey(tableName))
                {
                    throw new BindingException("Invalid table name");
                }

                return TableDetails[tableName];
            }

            List<string> tablesWithThisColumnName = [];

            foreach (var table in TableDetails)
            {
                if (table.Value.Columns!.Contains(column))
                {
                    tablesWithThisColumnName.Add(table.Key);
                }
            }

            if (tablesWithThisColumnName.Count > 1)
            {
                throw new BindingException($"Ambiguous column name: {column}");
            }

            if (tablesWithThisColumnName.Count == 0)
            {
                throw new BindingException($"Invalid column name: {column}");
            }

            tableName = tablesWithThisColumnName[0];

            return TableDetails[tableName];
        }

        /// <summary>
        /// Creates a null-filled row for a table's columns, useful for outer join padding.
        /// </summary>
        /// <param name="aliasOrName">The alias or table name in use.</param>
        /// <returns>A row containing all table columns with null values.</returns>
        public Row GetNullRowForTable(string aliasOrName)
        {
            var tableDetail = GetTableDetailByAliasOrName(aliasOrName);
            Row nullRow = new Row();

            if (tableDetail.Columns != null)
            {
                foreach (var col in tableDetail.Columns)
                {
                    nullRow.Add(col, null!);
                }
            }

            return nullRow;
        }

        /// <summary>
        /// Resolves the canonical table name from an alias-or-name reference.
        /// </summary>
        /// <param name="aliasOrName">The alias or table name in use.</param>
        /// <returns>The real table name.</returns>
        public string GetRealTableName(string aliasOrName)
        {
            return GetTableDetailByAliasOrName(aliasOrName).TableName;
        }

        /// <summary>
        /// Adds a table detail to the binding scope.
        /// </summary>
        /// <param name="tableDetail">The table metadata to add.</param>
        public void AddTableDetail(TableDetail tableDetail)
        {
            if (TableDetails.ContainsKey(tableDetail.TableName))
            {
                throw new BindingException("Duplicate table name found");
            }

            if (tableDetail.TableAlias != null && TableDetails.ContainsKey(tableDetail.TableAlias))
            {
                throw new BindingException("Duplicate table alias found");
            }

            tableDetail.DatabaseName = DatabaseName;

            TableDetails[tableDetail.GetTableNameInUse()] = tableDetail;
        }

        /// <summary>
        /// Parses a column reference and resolves both the owning table detail and unqualified column name.
        /// </summary>
        /// <param name="columnName">Column reference, optionally qualified with <c>table.column</c>.</param>
        /// <returns>A tuple of resolved table detail and column name.</returns>
        public Tuple<TableDetail, string> ParseAndFindTableDetailByColumn(string columnName)
        {
            string column = columnName;
            TableDetail? table;

            if (columnName.Contains('.'))
            {
                string[] splitColumnName = columnName.Split('.');

                if (splitColumnName.Length != 2)
                {
                    throw new BindingException("Column names can only contain one '.' character!");
                }

                table = TableDetails[splitColumnName[0]];
                column = splitColumnName[1];
            }
            else
            {
                table = GetTableDetailByColumn(columnName);
            }

            return Tuple.Create(table!, column);
        }

        /// <summary>
        /// Parses a column reference and resolves both the real table name and column name.
        /// </summary>
        /// <param name="columnName">Column reference, optionally qualified with <c>table.column</c>.</param>
        /// <returns>A tuple of table name and column name.</returns>
        public Tuple<string, string> ParseAndFindTableNameByColumn(string columnName)
        {
            Tuple<TableDetail, string> parseResult = ParseAndFindTableDetailByColumn(columnName);

            return Tuple.Create(parseResult.Item1.TableName, parseResult.Item2);
        }
    }
}
