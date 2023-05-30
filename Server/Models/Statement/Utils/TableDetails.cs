using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models.Statement.Utils
{
    public class TableDetail
    {
        public string TableName { get; set; }
        public string? TableAlias { get; set; }
        public List<string> Columns { get; set; } = new();

        public TableDetail(string tableName, string? tableAlias, List<string>? columns = null)
        {
            TableName = tableName;
            TableAlias = tableAlias;
            Columns = columns ?? new();
        }

        public void FetchTableColumns(string database)
        {
            Columns = Catalog.Catalog.GetTableColumns(TableName, database)
                .Select(c => c.Name)
                .ToList();
        }

        public string GetTableNameInUse()
        {
            if (TableAlias is not null)
            {
                return TableAlias;
            }

            return TableName;
        }
    }
}
