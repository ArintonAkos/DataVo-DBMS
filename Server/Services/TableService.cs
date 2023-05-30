using Microsoft.Extensions.Primitives;
using Server.Models.Catalog;
using Server.Models.Statement.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Services
{
    public class TableService
    {
        public Dictionary<string, TableDetail> TableDetails { get; private set; } = new();

        public TableDetail GetTableDetailByAliasOrName(string aliasOrName)
        {
            foreach (var tableDetail in TableDetails.Values)
            {
                if (tableDetail.GetTableNameInUse() == aliasOrName)
                {
                    return tableDetail;
                }
            }

            throw new Exception("Table name or alias not found");
        }

        public string GetTableNameByColumn(string column)
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

                return TableDetails[tableName].GetTableNameInUse();
            }

            List<string> tablesWithThisColumnName = new();

            foreach (var table in TableDetails)
            {
                if (table.Value.Columns.Contains(column))
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
            return TableDetails[tableName].GetTableNameInUse();
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

            TableDetails[tableDetail.GetTableNameInUse()] = tableDetail;
        }

        public void AddTableColumnsFromCatalog(string aliasOrName, string database)
        {
            var selectedTable = TableDetails[aliasOrName];

            if (selectedTable == null)
            {
                throw new Exception($"Table {aliasOrName} not found");
            }

            selectedTable.FetchTableColumns(database);
        }
    }
}
