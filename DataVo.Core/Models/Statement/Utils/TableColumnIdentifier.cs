namespace DataVo.Core.Models.Statement.Utils
{
    public class TableColumnIdentifier(string tableName, string columnName)
    {
        public string TableName { get; set; } = tableName;
        public string ColumnName { get; set; } = columnName;

        public string GetFullyQualifiedName()
        {
            return $"{TableName}.{ColumnName}";
        }
    }
}
