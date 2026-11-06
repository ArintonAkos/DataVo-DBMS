using System.Globalization;
using DataVo.Core.BTree;
using DataVo.Core.Exceptions;
using DataVo.Core.Indexing;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.Parser.DML;

/// <summary>
/// The key checks of an UPDATE, shared by the SQL and compiled update paths. The primary key is one
/// constraint over all its columns, so a composite key only clashes when every column matches another
/// row; each UNIQUE column is its own constraint. Keys are probed in the key indexes with the encoding
/// INSERT uses, and the table is scanned only when an index cannot be read.
/// </summary>
internal sealed class UpdateKeyValidator(IndexManager indexes, StorageContext storage, string tableName, string databaseName)
{
    private const string PrimaryKeySlot = "\0primary-key";
    private readonly Dictionary<string, HashSet<string>> _assigned = new(StringComparer.OrdinalIgnoreCase);

    public void Validate(
        IReadOnlyDictionary<string, object?> newRow,
        IReadOnlyDictionary<string, object?> oldRow,
        IReadOnlyList<string> primaryKeys,
        IReadOnlyList<string> uniqueKeys,
        int rowNumber)
    {
        if (primaryKeys.Count > 0 && primaryKeys.Any(col => newRow.ContainsKey(col) && !SameValue(newRow, oldRow, col)))
        {
            foreach (string col in primaryKeys)
            {
                if (newRow.TryGetValue(col, out object? keyPart) && keyPart is null)
                {
                    throw new EvaluationException($"Constraint violation: Primary key column {col} cannot be null in row {rowNumber}.");
                }
            }

            var merged = Merge(newRow, oldRow, primaryKeys);
            string key = IndexKeyEncoder.BuildKeyString(merged, primaryKeys);
            string shown = string.Join(", ", primaryKeys.Select(col => Convert.ToString(merged[col], CultureInfo.InvariantCulture)));
            if (!Slot(PrimaryKeySlot).Add(key))
            {
                throw new EvaluationException($"Primary key violation: the UPDATE gives two rows the key ({shown}) in table {tableName}.");
            }

            if (KeyExists(key, $"_PK_{tableName}", primaryKeys))
            {
                throw new EvaluationException($"Primary key violation: a row with key ({shown}) already exists in table {tableName} (row {rowNumber} of the UPDATE).");
            }
        }

        foreach (string col in uniqueKeys)
        {
            if (!newRow.TryGetValue(col, out object? value) || value is null || SameValue(newRow, oldRow, col))
            {
                continue; // NULL never clashes; an unchanged value is this row's own
            }

            string key = IndexKeyEncoder.BuildKeyString(new Dictionary<string, object?> { [col] = value }, [col]);
            string shown = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
            if (!Slot(col).Add(key))
            {
                throw new EvaluationException($"Update conflict: Duplicate value '{shown}' generated within the same batch for unique column {col}.");
            }

            if (KeyExists(key, $"_UK_{col}", [col]))
            {
                throw new EvaluationException($"Constraint violation: Duplicate value '{shown}' for unique column {col} in row {rowNumber}.");
            }
        }
    }

    private HashSet<string> Slot(string name)
    {
        if (!_assigned.TryGetValue(name, out HashSet<string>? set))
        {
            set = new HashSet<string>(StringComparer.Ordinal);
            _assigned[name] = set;
        }

        return set;
    }

    /// <summary>The key columns after the update: assigned values over the old row's.</summary>
    private static Dictionary<string, object?> Merge(IReadOnlyDictionary<string, object?> newRow, IReadOnlyDictionary<string, object?> oldRow, IReadOnlyList<string> columns)
    {
        var merged = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (string col in columns)
        {
            merged[col] = newRow.TryGetValue(col, out object? value) ? value : oldRow.TryGetValue(col, out object? old) ? old : null;
        }

        return merged;
    }

    private static bool SameValue(IReadOnlyDictionary<string, object?> newRow, IReadOnlyDictionary<string, object?> oldRow, string col)
    {
        oldRow.TryGetValue(col, out object? before);
        newRow.TryGetValue(col, out object? after);
        return (before is null) == (after is null)
            && string.Equals(
                Convert.ToString(before, CultureInfo.InvariantCulture),
                Convert.ToString(after, CultureInfo.InvariantCulture),
                StringComparison.Ordinal);
    }

    private bool KeyExists(string key, string indexName, IReadOnlyList<string> columns)
    {
        try
        {
            return indexes.IndexContainsKey(key, indexName, tableName, databaseName);
        }
        catch (IndexException)
        {
        }

        return storage.GetTableContents(tableName, databaseName).Values
            .Any(row => columns.All(col => row.TryGetValue(col, out object? v) && v is not null)
                && string.Equals(IndexKeyEncoder.BuildKeyString(row, columns), key, StringComparison.Ordinal));
    }
}
