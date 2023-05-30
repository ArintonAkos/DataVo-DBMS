using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models.Statement.Utils
{
    public class TableColumnIdentifier
    {
        public string TableName { get; set; }
        public string ColumnName { get; set; }

        public TableColumnIdentifier(string tableName, string columnName)
        {
            TableName = tableName;
            ColumnName = columnName;
        }

        public string GetFullyQualifiedName()
        {
            return $"{TableName}.{ColumnName}";
        }
    }
}
