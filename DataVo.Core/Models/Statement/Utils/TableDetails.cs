using DataVo.Core.StorageEngine;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime;

namespace DataVo.Core.Models.Statement.Utils
{
    /// <summary>
    /// Describes a query-visible table, including schema and cached row materialization.
    /// </summary>
    public class TableDetail
    {
        /// <summary>
        /// Initializes a table detail bound to a catalog table and optional alias.
        /// </summary>
        /// <param name="tableName">The catalog table name.</param>
        /// <param name="tableAlias">The query alias, when present.</param>
        public TableDetail(string tableName, string? tableAlias)
        {
            TableName = tableName;
            TableAlias = tableAlias;
        }

        /// <summary>
        /// Initializes a table detail backed by in-memory schema and rows.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <param name="tableAlias">The query alias, when present.</param>
        /// <param name="inMemoryColumns">In-memory column names.</param>
        /// <param name="inMemoryRows">In-memory row records.</param>
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

        /// <summary>
        /// Gets or sets the bound database name.
        /// </summary>
        public string? DatabaseName { get; set; }

        /// <summary>
        /// Gets or sets the catalog table name.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets the query alias for the table.
        /// </summary>
        public string? TableAlias { get; set; }

        private List<string>? _columnsCache;

        /// <summary>
        /// Gets table column names, loading from catalog when not already cached.
        /// </summary>
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
                    throw new BindingException("Database not selected!");
                }

                _columnsCache ??= DataVoEngine.Current().Catalog.GetTableColumns(TableName, DatabaseName)
                    .Select(c => c.Name)
                    .ToList();
                return _columnsCache;
            }
        }

        private List<string>? _primaryKeysCache;

        /// <summary>
        /// Gets primary key column names, loading from catalog when not already cached.
        /// </summary>
        public List<string>? PrimaryKeys
        {
            get
            {
                if (_columnsCache != null && DatabaseName is null)
                {
                    _primaryKeysCache ??= [];
                    return _primaryKeysCache;
                }

                if (DatabaseName is null)
                {
                    throw new BindingException("Database not selected!");
                }

                _primaryKeysCache ??= DataVoEngine.Current().Catalog.GetTablePrimaryKeys(TableName, DatabaseName);
                return _primaryKeysCache;
            }
        }

        private Dictionary<string, string>? _indexedColumnsCache;

        /// <summary>
        /// Gets indexed columns mapped to index names, loading from catalog when needed.
        /// </summary>
        public Dictionary<string, string>? IndexedColumns
        {
            get
            {
                if (_columnsCache != null && DatabaseName is null)
                {
                    _indexedColumnsCache ??= [];
                    return _indexedColumnsCache;
                }

                if (DatabaseName is null)
                {
                    throw new BindingException("Database not selected!");
                }

                _indexedColumnsCache ??= DataVoEngine.Current().Catalog.GetTableIndexedColumns(TableName, DatabaseName);
                return _indexedColumnsCache;
            }
        }


        // Stores the table content in the <RowId, Record> format
        private TableData? _tableContentCache;

        /// <summary>
        /// Gets table content keyed by row identifier, loading from storage context when needed.
        /// </summary>
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
                    throw new BindingException("Database not selected!");
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

        /// <summary>
        /// Gets table content as a row-value list, materialized from <see cref="TableContent"/> when needed.
        /// </summary>
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
                    throw new BindingException("Database not selected!");
                }

                _tableContentValuesCache ??= TableContent!.Select(row => row.Value).ToList();
                return _tableContentValuesCache;
            }
        }

        /// <summary>
        /// Returns the effective table identifier used in query text (alias when present, otherwise table name).
        /// </summary>
        /// <returns>The alias or table name.</returns>
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
