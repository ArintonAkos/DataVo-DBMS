using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Serialization;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core.Indexing.Columnar;

/// <summary>
/// Finds the columnar index of a table for a query, building it when the table's definition calls for one that
/// storage does not hold yet: after <c>CREATE INDEX ... USING COLUMNAR</c>, after the table's columns changed,
/// or after its rows were replaced as a whole.
/// </summary>
/// <remarks>
/// The catalog holds the definition (which columns); the storage engine holds the index and writes it as rows
/// change. All columnar indexes declared on a table are kept as one index over the union of their columns.
/// </remarks>
internal sealed class ColumnarIndexManager(DataVoEngine engine)
{
    private readonly object _buildGate = new();

    /// <summary>
    /// The table's columnar index, current for its schema, or null when the table has none, the storage mode
    /// keeps none, or the table holds rows the index cannot represent.
    /// </summary>
    public ColumnarIndex? Find(string databaseName, string tableName)
    {
        IColumnarRowStore? store = engine.StorageContext.ColumnarStore;
        if (store is null)
        {
            return null;
        }

        ColumnarIndex? index = store.Columnar(databaseName, tableName);
        int version = engine.Catalog.GetTableSchemaVersion(tableName, databaseName);
        if (index is null || index.SchemaVersion != version || index.WantsRebuild)
        {
            index = Refresh(store, index, databaseName, tableName, version);
        }

        return index is { Usable: true } ? index : null;
    }

    /// <summary>Checks that every column can be in a columnar index.</summary>
    /// <exception cref="Exceptions.BindingException">A column has a type the index cannot hold.</exception>
    public void Validate(string databaseName, string tableName, IReadOnlyList<string> columnNames)
    {
        foreach (string name in columnNames)
        {
            string type = engine.Catalog.GetTableColumnType(tableName, databaseName, name);
            if (!ColumnarIndex.Supports(RowSerializer.ClassifyColumnType(type)))
            {
                throw new Exceptions.BindingException(
                    $"Column {name} is of type {type}, which a columnar index cannot hold. Use INT, BIGINT, FLOAT, DOUBLE, BIT or DATE columns.");
            }
        }
    }

    private ColumnarIndex? Refresh(IColumnarRowStore store, ColumnarIndex? stale, string databaseName, string tableName, int version)
    {
        IReadOnlyList<IndexFile> definitions = engine.Catalog.GetColumnarIndexesReadOnly(tableName, databaseName);
        if (definitions.Count == 0)
        {
            if (stale is not null)
            {
                store.DropColumnar(databaseName, tableName);
            }

            return null;
        }

        // A clustered table keeps its key outside the row. Only an engine that finds rows by key can hand
        // the index that key with each row.
        int keyOrdinal = engine.StorageContext.ResolveClusteredKey(databaseName, tableName)?.Ordinal ?? -1;
        if (keyOrdinal >= 0 && !store.KeyedRows)
        {
            return null;
        }

        IReadOnlyList<Models.Catalog.Column> columns = engine.Catalog.GetTableColumnsReadOnly(tableName, databaseName);
        var kinds = new StorageKind[columns.Count];
        for (int i = 0; i < kinds.Length; i++)
        {
            kinds[i] = RowSerializer.ClassifyColumnType(columns[i].Type);
        }

        var ordinals = new List<int>();
        foreach (IndexFile definition in definitions)
        {
            foreach (string name in definition.AttributeNames)
            {
                for (int i = 0; i < columns.Count; i++)
                {
                    if (string.Equals(columns[i].Name, name, StringComparison.OrdinalIgnoreCase)
                        && ColumnarIndex.Supports(kinds[i])
                        && !ordinals.Contains(i))
                    {
                        ordinals.Add(i);
                    }
                }
            }
        }

        if (ordinals.Count == 0)
        {
            store.DropColumnar(databaseName, tableName);
            return null;
        }

        lock (_buildGate)
        {
            // Another query may have built it while this one waited.
            ColumnarIndex? current = store.Columnar(databaseName, tableName);
            return current is not null && current.SchemaVersion == version && !current.WantsRebuild
                ? current
                : store.BuildColumnar(databaseName, tableName, kinds, [.. ordinals], version, keyOrdinal);
        }
    }
}
