using DataVo.Core.StorageEngine;
using DataVo.Core.Runtime;

namespace DataVo.Core.Models.Statement.Utils
{
    public class TableDetail
    {
        public TableDetail(string tableName, string? tableAlias)
        {
            TableName = tableName;
            TableAlias = tableAlias;
        }

        public TableDetail(string tableName, string? tableAlias, List<string> inMemoryColumns, List<Record> inMemoryRows)
        {
            TableName = tableName;
            TableAlias = tableAlias;
            _columnsCache = [.. inMemoryColumns];
            _tableContentCache = [];
            foreach (var row in inMemoryRows)
            {
                _tableContentCache[row.RowId] = row;
            }

            _tableContentValuesCache = [.. inMemoryRows];
        }

        public string? DatabaseName { get; set; }
        public string TableName { get; set; }
        public string? TableAlias { get; set; }

        private List<string>? _columnsCache;
        public List<string>? Columns
        {
            get
            {
                if (_columnsCache != null)
                {
                    return _columnsCache;
                }

                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                _columnsCache ??= DataVoEngine.Current().Catalog.GetTableColumns(TableName, DatabaseName)
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
                if (_columnsCache != null)
                {
                    _primaryKeysCache ??= [];
                    return _primaryKeysCache;
                }

                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                _primaryKeysCache ??= DataVoEngine.Current().Catalog.GetTablePrimaryKeys(TableName, DatabaseName);
                return _primaryKeysCache;
            }
        }

        private Dictionary<string, string>? _indexedColumnsCache;
        public Dictionary<string, string>? IndexedColumns
        {
            get
            {
                if (_columnsCache != null)
                {
                    _indexedColumnsCache ??= [];
                    return _indexedColumnsCache;
                }

                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                _indexedColumnsCache ??= DataVoEngine.Current().Catalog.GetTableIndexedColumns(TableName, DatabaseName);
                return _indexedColumnsCache;
            }
        }


        // Stores the table content in the <RowId, Record> format
        private TableData? _tableContentCache;
        public TableData? TableContent
        {
            get
            {
                if (_tableContentCache != null)
                {
                    return _tableContentCache;
                }

                if (DatabaseName is null)
                {
                    throw new Exception("Database not selected!");
                }

                if (_tableContentCache == null)
                {
                    _tableContentCache = [];
                    var internalRows = DataVoEngine.Current().StorageContext.GetTableContents(TableName, DatabaseName);
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
                if (_tableContentValuesCache != null)
                {
                    return _tableContentValuesCache;
                }

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
