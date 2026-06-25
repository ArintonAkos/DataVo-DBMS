using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVo.Generators.Sql;

/// <summary>
/// Immutable, value-equatable compile-time view of the table schema relevant to access-path resolution:
/// which single columns are primary keys and which single columns are covered by a named secondary index.
/// Value equality (over a canonical signature) lets the Roslyn incremental catalog node cache between builds.
/// </summary>
internal sealed class CompileTimeCatalog : IEquatable<CompileTimeCatalog>
{
    public static readonly CompileTimeCatalog Empty = new(
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
        new HashSet<string>(StringComparer.OrdinalIgnoreCase));

    // Keyed "table|column" (case-insensitive); '|' cannot appear in a SQL identifier. Value is the declared index name (original casing).
    private readonly Dictionary<string, string> _columnIndexes;
    private readonly HashSet<string> _primaryKeys;
    private readonly string _signature;

    public CompileTimeCatalog(Dictionary<string, string> columnIndexes, HashSet<string> primaryKeys)
    {
        _columnIndexes = columnIndexes;
        _primaryKeys = primaryKeys;
        _signature = BuildSignature(columnIndexes, primaryKeys);
    }

    public static string Key(string table, string column) => table + "|" + column;

    public bool TryResolveSingleColumnIndex(string table, string column, out string indexName)
        => _columnIndexes.TryGetValue(Key(table, column), out indexName!);

    public bool IsPrimaryKey(string table, string column)
        => _primaryKeys.Contains(Key(table, column));

    public bool Equals(CompileTimeCatalog? other) => other is not null && _signature == other._signature;

    public override bool Equals(object? obj) => Equals(obj as CompileTimeCatalog);

    public override int GetHashCode() => _signature.GetHashCode();

    private static string BuildSignature(Dictionary<string, string> columnIndexes, HashSet<string> primaryKeys)
    {
        var sb = new StringBuilder();
        foreach (KeyValuePair<string, string> pair in columnIndexes.OrderBy(p => p.Key, StringComparer.OrdinalIgnoreCase))
        {
            sb.Append("I:").Append(pair.Key.ToLowerInvariant()).Append('=').Append(pair.Value.ToLowerInvariant()).Append(';');
        }

        foreach (string pk in primaryKeys.OrderBy(p => p, StringComparer.OrdinalIgnoreCase))
        {
            sb.Append("P:").Append(pk.ToLowerInvariant()).Append(';');
        }

        return sb.ToString();
    }
}
