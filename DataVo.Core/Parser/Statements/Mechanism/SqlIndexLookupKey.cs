using System.Globalization;
using DataVo.Core.Compat;
using DataVo.Core.Models.Catalog;
using Column = DataVo.Core.Models.Catalog.Column;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.Statements.Mechanism;

/// <summary>
/// Builds the index key for a SQL <c>column = literal</c> predicate exactly as the index writers build
/// it (<see cref="BTree.IndexKeyEncoder"/>), and decides whether the index answer is authoritative.
/// </summary>
/// <remarks>
/// The SQL path used to look indexes up with the raw literal text ("101"), while writers store
/// numbers as bracketed keys ("[101]"). Every secondary-index lookup missed and fell back to a full
/// table scan, so indexed SQL queries cost O(rows). The full-scan fallback stays for cases where an
/// exact index match could differ from the scan's comparison rules; for the cases below the two are
/// equivalent, so a miss means no row matches.
/// </remarks>
internal static class SqlIndexLookupKey
{
    /// <summary>
    /// Returns true when <paramref name="key"/> is the canonical key of <paramref name="literal"/> for
    /// <paramref name="columnName"/> and an index miss proves that no row matches (no scan needed).
    /// </summary>
    /// <param name="table">The table being filtered.</param>
    /// <param name="columnName">The single column compared with the literal.</param>
    /// <param name="indexName">The index that would answer the lookup.</param>
    /// <param name="literal">The literal as parsed: int, long, double, or a quoted string.</param>
    /// <param name="key">The canonical index key.</param>
    public static bool TryBuildAuthoritative(TableDetail table, string columnName, string indexName, object? literal, out string key)
    {
        key = string.Empty;
        if (literal is null || table.DatabaseName is null)
        {
            return false;
        }

        DataVoEngine engine = DataVoEngine.Current();

        // Disk mode keeps index files written by older versions, whose key format may differ; only
        // modes that rebuild their indexes from rows on open are guaranteed to use the current format.
        if (engine.Config.StorageMode is not (StorageMode.InMemory or StorageMode.Lsm))
        {
            return false;
        }

        if (!IsSingleColumnBTree(engine, table, columnName, indexName))
        {
            return false;
        }

        Column? column = FindColumn(engine, table, columnName);
        if (column is null)
        {
            return false;
        }

        if (column.Type.Equals("INT", StringComparison.OrdinalIgnoreCase))
        {
            return TryBuildIntegerKey(literal, out key);
        }

        if (column.Type.Equals("FLOAT", StringComparison.OrdinalIgnoreCase))
        {
            return TryBuildFloatKey(literal, out key);
        }

        if (column.Type.Equals("VARCHAR", StringComparison.OrdinalIgnoreCase))
        {
            return TryBuildStringKey(literal, out key);
        }

        if (column.Type.Equals("GUID", StringComparison.OrdinalIgnoreCase))
        {
            return TryBuildGuidKey(literal, out key);
        }

        // DATE, BIT and VECTOR keep the scan fallback.
        return false;
    }

    /// <summary>
    /// A GUID column equals a literal that parses as the same GUID, in any case or format (as in SQL
    /// Server). Without an authoritative key, every lookup of a GUID that does not exist fell back to a
    /// full table scan to confirm the miss: about a second per query on a million rows.
    /// </summary>
    private static bool TryBuildGuidKey(object literal, out string key)
    {
        Guid value;
        switch (literal)
        {
            case Guid guid:
                value = guid;
                break;
            case string text when Guid.TryParse(text.Trim('\''), out Guid parsed):
                value = parsed;
                break;
            default:
                key = string.Empty; // not a GUID: no GUID value can equal it, and the empty key finds nothing
                return true;
        }

        key = value.ToString("D");
        return true;
    }

    private static bool TryBuildIntegerKey(object literal, out string key)
    {
        switch (literal)
        {
            case int intValue:
                key = ScalarKeyFormatter.FormatBracketed(intValue, CultureInfo.InvariantCulture);
                return true;
            case long longValue:
                // Outside the INT range no row can be equal; the bracketed long simply finds nothing.
                key = ScalarKeyFormatter.FormatBracketed(longValue, CultureInfo.InvariantCulture);
                return true;
            case double doubleValue when doubleValue == Math.Floor(doubleValue)
                && doubleValue >= int.MinValue && doubleValue <= int.MaxValue:
                // 101.0 equals the INT 101.
                key = ScalarKeyFormatter.FormatBracketed((int)doubleValue, CultureInfo.InvariantCulture);
                return true;
            case double doubleValue when !double.IsNaN(doubleValue):
                // A fractional value equals no INT; this key finds nothing, which is the right answer.
                key = ScalarKeyFormatter.FormatBracketed(doubleValue, CultureInfo.InvariantCulture);
                return true;
            default:
                key = string.Empty;
                return false; // a string literal: let the scan apply its coercion rules
        }
    }

    private static bool TryBuildFloatKey(object literal, out string key)
    {
        double value;
        switch (literal)
        {
            case int intValue:
                value = intValue;
                break;
            case long longValue:
                value = longValue;
                break;
            case double doubleValue when !double.IsNaN(doubleValue):
                value = doubleValue;
                break;
            default:
                key = string.Empty;
                return false;
        }

        key = ScalarKeyFormatter.FormatBracketed(value, CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryBuildStringKey(object literal, out string key)
    {
        key = string.Empty;
        if (literal is not string text || text.Length < 2 || text[0] != '\'' || text[^1] != '\'')
        {
            return false;
        }

        string inner = text[1..^1];

        // Keep the scan for values whose comparison is not plain ordinal equality: the scan also
        // compares numeric-looking text as numbers ('7' = '007'), writers store vector-looking text as
        // a vector key, and quotes/escapes at the edges are trimmed by the scan.
        if (inner.Length == 0
            || inner.Contains('\'')
            || char.IsWhiteSpace(inner[0])
            || char.IsWhiteSpace(inner[^1])
            || double.TryParse(inner, NumberStyles.Float, CultureInfo.InvariantCulture, out _)
            || double.TryParse(inner, NumberStyles.Float, CultureInfo.CurrentCulture, out _)
            || VectorParser.TryParseVector(inner, out _))
        {
            return false;
        }

        key = inner;
        return true;
    }

    private static bool IsSingleColumnBTree(DataVoEngine engine, TableDetail table, string columnName, string indexName)
    {
        if (indexName.Equals($"_PK_{table.TableName}", StringComparison.OrdinalIgnoreCase))
        {
            return table.PrimaryKeys is { Count: 1 } primaryKeys
                && primaryKeys[0].Equals(columnName, StringComparison.OrdinalIgnoreCase);
        }

        foreach (IndexFile index in engine.Catalog.GetTableIndexesReadOnly(table.TableName, table.DatabaseName!))
        {
            if (!index.IndexFileName.Equals(indexName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            return index.AttributeNames.Count == 1
                && index.AttributeNames[0].Equals(columnName, StringComparison.OrdinalIgnoreCase)
                && index.IndexKind.Equals("BTREE", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static Column? FindColumn(DataVoEngine engine, TableDetail table, string columnName)
    {
        foreach (Column column in engine.Catalog.GetTableColumnsReadOnly(table.TableName, table.DatabaseName!))
        {
            if (column.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase))
            {
                return column;
            }
        }

        return null;
    }
}
