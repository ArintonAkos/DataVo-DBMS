using DataVo.Core.Contracts;
using Database = DataVo.Core.Models.Catalog;

namespace DataVo.Core.Models.Statement.Utils
{
    public class Column : IColumn
    {
        public string DatabaseName { get; set; }
        public string TableName { get; set; }
        public string ColumnName { get; set; }

        public Column(string databaseName, string tableName, string columnName)
        {
            DatabaseName = databaseName;
            TableName = tableName;
            ColumnName = columnName;
        }

        public string RawType()
        {
            return Database.Catalog.GetTableColumnType(TableName, DatabaseName, ColumnName);
        }
    }
}
