using DataVo.Core.Contracts;
using DataVo.Core.Runtime;

namespace DataVo.Core.Models.Statement.Utils
{
    public class Column(string databaseName, string tableName, string columnName) : IColumn
    {
        public string DatabaseName { get; set; } = databaseName;
        public string TableName { get; set; } = tableName;
        public string ColumnName { get; set; } = columnName;

        public string RawType()
        {
            return DataVoEngine.Current().Catalog.GetTableColumnType(TableName, DatabaseName, ColumnName);
        }
    }
}
