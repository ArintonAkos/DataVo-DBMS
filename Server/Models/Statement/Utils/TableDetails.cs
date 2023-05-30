
using Server.Server.MongoDB;

namespace Server.Models.Statement.Utils
{
    public class TableDetail
    {
        public string? DatabaseName { get; set; }
        public string TableName { get; set; }
        public string? TableAlias { get; set; }
        public List<string>? Columns { get; set; }

        private List<string>? _primaryKeys { get; set; }
        public List<string>? PrimaryKeys
        {
            get
            {
                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                _primaryKeys ??= Catalog.Catalog.GetTablePrimaryKeys(TableName, DatabaseName);
                return _primaryKeys;
            }
        }

        private Dictionary<string, string>? _indexedColumns { get; set; }
        public Dictionary<string, string>? IndexedColumns
        {
            get
            {
                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                _indexedColumns ??= Catalog.Catalog.GetTableIndexedColumns(TableName, DatabaseName);
                return _indexedColumns;
            }
        }

        private Dictionary<string, Dictionary<string, dynamic>>? tableContent { get; set; }
        public Dictionary<string, Dictionary<string, dynamic>>? TableContent
        {
            get
            {
                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                tableContent ??= DbContext.Instance.GetTableContents(TableName, DatabaseName);
                return tableContent;
            }
        }

        public TableDetail(string tableName, string? tableAlias)
        {
            TableName = tableName;
            TableAlias = tableAlias;
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
