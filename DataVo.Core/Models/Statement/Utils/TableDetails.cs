using DataVo.Core.StorageEngine;

namespace DataVo.Core.Models.Statement.Utils
{
    public class TableDetail(string tableName, string? tableAlias)
    {
        public string? DatabaseName { get; set; }
        public string TableName { get; set; } = tableName;
        public string? TableAlias { get; set; } = tableAlias;

        private List<string>? _columnsCache;
        public List<string>? Columns
        {
            get
            {
                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                _columnsCache ??= Catalog.Catalog.GetTableColumns(TableName, DatabaseName)
                    .Select(c => c.Name)
                    .ToList();
                return _columnsCache;
            }
        }

        private List<string>? _primaryKeysCache;
        public List<string>? PrimaryKeys
        {
            get
            {
                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                _primaryKeysCache ??= Catalog.Catalog.GetTablePrimaryKeys(TableName, DatabaseName);
                return _primaryKeysCache;
            }
        }

        private Dictionary<string, string>? _indexedColumnsCache;
        public Dictionary<string, string>? IndexedColumns
        {
            get
            {
                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                _indexedColumnsCache ??= Catalog.Catalog.GetTableIndexedColumns(TableName, DatabaseName);
                return _indexedColumnsCache;
            }
        }


        // Stores the table content in the <RowId, Record> format
        private TableData? _tableContentCache;
        public TableData? TableContent
        {
            get
            {
                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                if (_tableContentCache == null)
                {
                    _tableContentCache = [];
                    var internalRows = StorageContext.Instance.GetTableContents(TableName, DatabaseName);
                    foreach (var row in internalRows)
                    {
                        _tableContentCache[row.Key] = new Record(row.Key, row.Value);
                    }
                }

                return _tableContentCache;
            }
        }

        private List<Record>? _tableContentValuesCache;
        public List<Record>? TableContentValues
        {
            get
            {
                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                _tableContentValuesCache ??= TableContent!.Select(row => row.Value).ToList();
                return _tableContentValuesCache;
            }
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
