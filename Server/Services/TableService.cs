using Server.Models.Statement.Utils;

namespace Server.Services
{
    public class TableService
    {
        private readonly string _databaseName;
        public Dictionary<string, TableDetail> TableDetails { get; private set; } = new();

        public TableService(string databaseName)
        {
            _databaseName = databaseName;
        }

        public TableDetail GetTableDetailByAliasOrName(string aliasOrName)
        {
            foreach (var tableDetail in TableDetails.Values)
            {
                if (tableDetail.TableName == aliasOrName || tableDetail.TableAlias == aliasOrName)
                {
                    return tableDetail;
                }
            }

            throw new Exception("Table name or alias not found");
        }

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
                    throw new Exception("Invalid table name");
                }

                return TableDetails[tableName];
            }

            List<string> tablesWithThisColumnName = new();

            foreach (var table in TableDetails)
            {
                if (table.Value.Columns!.Contains(column))
                {
                    tablesWithThisColumnName.Add(table.Key);
                }
            }

            if (tablesWithThisColumnName.Count > 1)
            {
                throw new Exception($"Ambiguous column name: {column}");
            }

            if (tablesWithThisColumnName.Count == 0)
            {
                throw new Exception($"Invalid column name: {column}");
            }

            tableName = tablesWithThisColumnName[0];

            return TableDetails[tableName];
        }

        public string GetRealTableName(string aliasOrName)
        {
            return GetTableDetailByAliasOrName(aliasOrName).TableName;
        }

        public void AddTableDetail(TableDetail tableDetail)
        {
            if (TableDetails.ContainsKey(tableDetail.TableName))
            {
                throw new Exception("Duplicate table name found");
            }

            if (tableDetail.TableAlias != null && TableDetails.ContainsKey(tableDetail.TableAlias))
            {
                throw new Exception("Duplicate table alias found");
            }

            tableDetail.DatabaseName = _databaseName;

            TableDetails[tableDetail.GetTableNameInUse()] = tableDetail;
        }

        public Tuple<TableDetail, string> ParseAndFindTableDetailByColumn(string columnName)
        {
            string column = columnName;
            TableDetail? table;

            if (columnName.Contains('.'))
            {
                string[] splitColumnName = columnName.Split('.');

                if (splitColumnName.Length != 2)
                {
                    throw new Exception("Column names can only contain one '.' character!");
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

        public Tuple<string, string> ParseAndFindTableNameByColumn(string columnName)
        {
            Tuple<TableDetail, string> parseResult = ParseAndFindTableDetailByColumn(columnName);

            return Tuple.Create(parseResult.Item1.TableName, parseResult.Item2);
        }
    }
}
