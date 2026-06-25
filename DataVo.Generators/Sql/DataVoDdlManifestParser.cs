using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text.RegularExpressions;

namespace DataVo.Generators.Sql;

/// <summary>
/// Minimal regex parser for the DDL schema manifest, mirroring <see cref="DataVoQueryShapeParser"/>. Recognizes
/// single-column <c>CREATE TABLE … PRIMARY KEY</c> (inline or table-constraint) and single-column
/// <c>CREATE [UNIQUE] INDEX … ON t (col)</c>. Composite indexes/keys and unrecognized statements are ignored
/// (they degrade safely to RuntimeResolve at emit time).
/// </summary>
internal static class DataVoDdlManifestParser
{
    private static readonly Regex CreateTableRegex = new(
        @"CREATE\s+TABLE\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)\s*\((?<body>.*)\)",
        RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);

    private static readonly Regex CreateIndexRegex = new(
        @"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s+ON\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(?<col>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex TableConstraintPrimaryKeyRegex = new(
        @"PRIMARY\s+KEY\s*\(\s*(?<col>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex InlinePrimaryKeyRegex = new(
        @"(?<col>[A-Za-z_][A-Za-z0-9_]*)\s+[^,]*?PRIMARY\s+KEY",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static CompileTimeCatalog Parse(ImmutableArray<string> manifestTexts)
    {
        var columnIndexes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var primaryKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (string text in manifestTexts)
        {
            if (string.IsNullOrWhiteSpace(text))
            {
                continue;
            }

            foreach (string raw in text.Split(';'))
            {
                string statement = raw.Trim();
                if (statement.Length == 0)
                {
                    continue;
                }

                Match index = CreateIndexRegex.Match(statement);
                if (index.Success)
                {
                    columnIndexes[CompileTimeCatalog.Key(index.Groups["table"].Value, index.Groups["col"].Value)] =
                        index.Groups["name"].Value;
                    continue;
                }

                Match table = CreateTableRegex.Match(statement);
                if (table.Success)
                {
                    string? pk = ResolvePrimaryKeyColumn(table.Groups["body"].Value);
                    if (pk is not null)
                    {
                        primaryKeys.Add(CompileTimeCatalog.Key(table.Groups["table"].Value, pk));
                    }
                }
            }
        }

        return columnIndexes.Count == 0 && primaryKeys.Count == 0
            ? CompileTimeCatalog.Empty
            : new CompileTimeCatalog(columnIndexes, primaryKeys);
    }

    private static string? ResolvePrimaryKeyColumn(string tableBody)
    {
        Match constraint = TableConstraintPrimaryKeyRegex.Match(tableBody);
        if (constraint.Success)
        {
            return constraint.Groups["col"].Value;
        }

        Match inline = InlinePrimaryKeyRegex.Match(tableBody);
        return inline.Success ? inline.Groups["col"].Value : null;
    }
}
