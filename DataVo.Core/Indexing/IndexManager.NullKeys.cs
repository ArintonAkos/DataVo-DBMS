using DataVo.Core.BTree;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Lsm.V2;

namespace DataVo.Core.Indexing;

/// <summary>
/// NULL index keys. A persisted (LSM) scalar index holds every row of its table: a row whose key has a NULL part is
/// entered under <see cref="LsmIndexKeyCodec.NullKey"/>, which no lookup can produce, so the index can count the
/// NULL group without reading the table. In-memory indexes do not hold NULL keys; every method here is a no-op
/// for them. Every write path (INSERT, UPDATE, DELETE, COMMIT and index rebuilds) goes through these methods for
/// the rows it does not give a key, so both kinds of index stay consistent with their rule.
/// </summary>
public partial class IndexManager
{
    /// <summary>Whether this manager's scalar indexes hold rows whose key is NULL.</summary>
    internal bool IndexesNullKeys => _lsmStore is not null;

    /// <summary>Enters a row whose key has a NULL part into the index (persisted indexes only).</summary>
    internal void InsertNullKeyRow(RowKey rowId, string indexName, string tableName, string databaseName) =>
        _lsmStore?.IndexPut(databaseName, tableName, indexName, LsmIndexKeyCodec.NullKey, rowId);

    /// <summary>Removes a row whose key had a NULL part from the index (persisted indexes only).</summary>
    internal void DeleteNullKeyRow(RowKey rowId, string indexName, string tableName, string databaseName) =>
        _lsmStore?.IndexDelete(databaseName, tableName, indexName, LsmIndexKeyCodec.NullKey, rowId);

    /// <summary>Enters every row of <paramref name="rows"/> whose key has a NULL part (persisted indexes only).</summary>
    internal void InsertNullKeyRows(IReadOnlyList<Dictionary<string, object?>> rows, IReadOnlyList<RowKey> rowIds, IReadOnlyList<string> attributes, string indexName, string tableName, string databaseName)
    {
        if (_lsmStore is null)
        {
            return;
        }

        for (int i = 0; i < rows.Count; i++)
        {
            if (!IndexKeyEncoder.HasCompleteKey(rows[i], attributes))
            {
                InsertNullKeyRow(rowIds[i], indexName, tableName, databaseName);
            }
        }
    }

    /// <summary>Enters every typed row of <paramref name="rows"/> whose key has a NULL part (persisted indexes only).</summary>
    internal void InsertNullKeyRows(ReactiveRowSchema schema, IReadOnlyList<CellValue[]> rows, IReadOnlyList<RowKey> rowIds, IReadOnlyList<string> attributes, string indexName, string tableName, string databaseName)
    {
        if (_lsmStore is null)
        {
            return;
        }

        for (int i = 0; i < rows.Count; i++)
        {
            if (!IndexKeyEncoder.HasCompleteKey(schema, rows[i], attributes))
            {
                InsertNullKeyRow(rowIds[i], indexName, tableName, databaseName);
            }
        }
    }

    /// <summary>
    /// Keeps the NULL-key entries of an index in step with an UPDATE that turns rows <paramref name="oldRows"/>
    /// (at <paramref name="oldRowIds"/>) into <paramref name="newRows"/> (at <paramref name="newRowIds"/>): a row
    /// that becomes NULL-keyed is entered before one that stops being NULL-keyed is removed, and a row that stays
    /// NULL-keyed in place is left alone (persisted indexes only).
    /// </summary>
    internal void UpdateNullKeyRows(
        IReadOnlyList<RowKey> oldRowIds,
        IReadOnlyList<Dictionary<string, object?>> oldRows,
        IReadOnlyList<RowKey> newRowIds,
        IReadOnlyList<Dictionary<string, object?>> newRows,
        IReadOnlyList<string> attributes,
        string indexName,
        string tableName,
        string databaseName)
    {
        if (_lsmStore is null)
        {
            return;
        }

        for (int i = 0; i < newRows.Count; i++)
        {
            bool wasNull = !IndexKeyEncoder.HasCompleteKey(oldRows[i], attributes);
            bool isNull = !IndexKeyEncoder.HasCompleteKey(newRows[i], attributes);
            if (wasNull && isNull && oldRowIds[i] == newRowIds[i])
            {
                continue;
            }

            if (isNull)
            {
                InsertNullKeyRow(newRowIds[i], indexName, tableName, databaseName);
            }

            if (wasNull)
            {
                DeleteNullKeyRow(oldRowIds[i], indexName, tableName, databaseName);
            }
        }
    }
}

/// <summary>
/// The contents of a scalar index gathered from a table's rows when the index is created or rebuilt: the row ids
/// of each key, and the rows whose key has a NULL part (indexed under the NULL key where indexes hold them).
/// </summary>
internal sealed class ScalarIndexContents
{
    /// <summary>Row ids per index key string (see <see cref="IndexKeyEncoder.BuildKeyString(Dictionary{string, object?}, IEnumerable{string})"/>).</summary>
    public Dictionary<string, List<RowKey>> Keys { get; } = new(StringComparer.Ordinal);

    /// <summary>Rows whose key has a NULL part.</summary>
    public List<RowKey> NullKeyRows { get; } = [];

    /// <summary>Adds a row under its key, or to <see cref="NullKeyRows"/> when the key has a NULL part.</summary>
    public void Add(Dictionary<string, object?> row, RowKey rowId, IReadOnlyList<string> attributes)
    {
        if (IndexKeyEncoder.HasCompleteKey(row, attributes))
        {
            AddKey(IndexKeyEncoder.BuildKeyString(row, attributes), rowId);
        }
        else
        {
            NullKeyRows.Add(rowId);
        }
    }

    /// <summary>Adds a row under an already built key string.</summary>
    public void AddKey(string key, RowKey rowId)
    {
        if (!Keys.TryGetValue(key, out List<RowKey>? rowIds))
        {
            rowIds = [];
            Keys[key] = rowIds;
        }

        rowIds.Add(rowId);
    }

    /// <summary>Gathers the contents of an index over <paramref name="rows"/> stored at <paramref name="rowIds"/>.</summary>
    public static ScalarIndexContents From(IReadOnlyList<Dictionary<string, object?>> rows, IReadOnlyList<RowKey> rowIds, IReadOnlyList<string> attributes)
    {
        var contents = new ScalarIndexContents();
        for (int i = 0; i < rows.Count; i++)
        {
            contents.Add(rows[i], rowIds[i], attributes);
        }

        return contents;
    }
}
