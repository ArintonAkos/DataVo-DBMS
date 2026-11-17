using System.Text;
using DataVo.Core.Enums;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// Text conditions of the typed scan tested on the stored UTF-8 bytes of a row. A scan over serialized rows
/// (the LSM engine) used to build a <see cref="string"/> for every text column a condition named, on every
/// row, only to compare it and drop it: 36 to 46 MB per query over a million rows. Equality, <c>LIKE</c> and
/// NULL tests need only the bytes, so the text is compared where it lies and decoded only for the rare case
/// the bytes cannot decide (a <c>_</c> wildcard against non-ASCII text).
/// </summary>
internal partial class Select
{
    /// <summary>Which text columns of a scan are compared as bytes, and where their bytes lie in the current row.</summary>
    private sealed class RawTextColumns
    {
        /// <summary>The text columns compared in place; they are not decoded before the conditions are tested.</summary>
        public required bool[] Mask { get; init; }

        /// <summary>The other filter columns, decoded into cells as usual.</summary>
        public required bool[] DecodeMask { get; init; }

        /// <summary>The last column either mask names: the row is walked no further.</summary>
        public required int Last { get; init; }

        /// <summary>Per column, the offset of its text in the current row.</summary>
        public required int[] Offsets { get; init; }

        /// <summary>Per column, the byte length of its text in the current row; -1 for NULL.</summary>
        public required int[] Lengths { get; init; }
    }

    /// <summary>
    /// Plans which text filter columns can be compared as bytes: those every condition tests with <c>=</c>,
    /// <c>&lt;&gt;</c>, <c>LIKE</c>, <c>NOT LIKE</c>, <c>IS NULL</c> or <c>IS NOT NULL</c> against text that has
    /// an exact UTF-8 form. Returns null when there is none.
    /// </summary>
    private static RawTextColumns? TryPlanRawText(IReadOnlyList<Column> columns, IReadOnlyList<TypedPredicate> predicates, bool[] filterMask)
    {
        var raw = new bool[columns.Count];
        bool any = false;
        for (int i = 0; i < raw.Length; i++)
        {
            raw[i] = filterMask[i] && IsTextColumn(columns[i]);
            any |= raw[i];
        }

        if (!any)
        {
            return null;
        }

        ExcludeColumnsNeedingStrings(predicates, raw);
        if (Array.IndexOf(raw, true) < 0)
        {
            return null;
        }

        var decode = new bool[columns.Count];
        for (int i = 0; i < decode.Length; i++)
        {
            decode[i] = filterMask[i] && !raw[i];
        }

        return new RawTextColumns
        {
            Mask = raw,
            DecodeMask = decode,
            Last = Array.LastIndexOf(filterMask, true),
            Offsets = new int[columns.Count],
            Lengths = new int[columns.Count],
        };
    }

    /// <summary>Clears from <paramref name="raw"/> every column some condition can only test on a decoded string.</summary>
    private static void ExcludeColumnsNeedingStrings(IEnumerable<TypedPredicate> predicates, bool[] raw)
    {
        foreach (TypedPredicate predicate in predicates)
        {
            if (predicate.Branches is { } branches)
            {
                foreach (TypedPredicate[] branch in branches)
                {
                    ExcludeColumnsNeedingStrings(branch, raw);
                }

                continue;
            }

            bool onBytes = predicate.Kind switch
            {
                TypedPredicateKind.IsNull or TypedPredicateKind.IsNotNull => true,
                TypedPredicateKind.Like => true, // the pattern itself says when it needs the string
                TypedPredicateKind.Text or TypedPredicateKind.ExactText => predicate.LiteralUtf8 is not null,
                _ => false,
            };
            if (!onBytes)
            {
                raw[predicate.Ordinal] = false;
            }
        }
    }

    /// <summary>
    /// Loads the filter columns of a serialized row for <see cref="MatchesAllRaw"/>: the text columns compared
    /// as bytes are located, the others decoded.
    /// </summary>
    private static void LoadFilterColumnsRaw(TypedScanState state, RawTextColumns raw, RowKey rowId, ReadOnlySpan<byte> rowBytes)
    {
        RowSerializer.DecodeCellsLocatingText(rowBytes, state.Columns, raw.DecodeMask, raw.Mask, raw.Last, state.Cells, raw.Offsets, raw.Lengths);
        RestoreClusteredKey(state, rowId, raw.DecodeMask);
    }

    /// <summary>Whether the current row meets every predicate, reading the located text columns from <paramref name="rowBytes"/>.</summary>
    private static bool MatchesAllRaw(TypedScanState state, RawTextColumns raw, IReadOnlyList<TypedPredicate> predicates, ReadOnlySpan<byte> rowBytes)
    {
        for (int i = 0; i < predicates.Count; i++)
        {
            if (!MatchesRaw(state, raw, predicates[i], rowBytes))
            {
                return false;
            }
        }

        return true;
    }

    private static bool MatchesRaw(TypedScanState state, RawTextColumns raw, TypedPredicate predicate, ReadOnlySpan<byte> rowBytes)
    {
        if (predicate.Branches is { } branches)
        {
            foreach (TypedPredicate[] branch in branches)
            {
                if (MatchesAllRaw(state, raw, branch, rowBytes))
                {
                    return true;
                }
            }

            return false;
        }

        int ordinal = predicate.Ordinal;
        if (!raw.Mask[ordinal])
        {
            return Matches(predicate, state.Cells[ordinal], state.Columns[ordinal]);
        }

        int length = raw.Lengths[ordinal];
        switch (predicate.Kind)
        {
            case TypedPredicateKind.IsNull:
                return length < 0;
            case TypedPredicateKind.IsNotNull:
                return length >= 0;
        }

        // A comparison with NULL is unknown: the row is not kept.
        return length >= 0 && MatchesText(predicate, state.Columns[ordinal], rowBytes.Slice(raw.Offsets[ordinal], length));
    }

    /// <summary>Tests an equality or LIKE condition on the UTF-8 bytes of a stored text value.</summary>
    private static bool MatchesText(TypedPredicate predicate, Column column, ReadOnlySpan<byte> utf8)
    {
        switch (predicate.Kind)
        {
            case TypedPredicateKind.Text:
                // Valid UTF-8 is equal byte for byte exactly when the strings are equal ordinally.
                return Utf8Text.UnquoteSingleQuoted(utf8).SequenceEqual(predicate.LiteralUtf8) == (predicate.Operator == Operators.EQUALS);
            case TypedPredicateKind.ExactText:
                return utf8.SequenceEqual(predicate.LiteralUtf8) == (predicate.Operator == Operators.EQUALS);
            case TypedPredicateKind.Like when predicate.Pattern!.TryIsMatchUtf8(Utf8Text.UnquoteSingleQuoted(utf8), out bool like):
                return like == (predicate.Operator == Operators.LIKE);
            default:
                // The bytes cannot decide (a '_' against non-ASCII text): compare the decoded string as before.
                return Matches(predicate, CellValue.From(Encoding.UTF8.GetString(utf8)), column);
        }
    }

    /// <summary>
    /// Adds to <paramref name="restMask"/> the text columns compared as bytes that the rest of the query reads:
    /// they were not decoded for the filter, so they are decoded with the other columns of a matching row.
    /// </summary>
    private static void DecodeRawTextAfterMatch(RawTextColumns? raw, bool[] readAfterFilter, bool[] restMask)
    {
        if (raw is null)
        {
            return;
        }

        for (int i = 0; i < restMask.Length; i++)
        {
            restMask[i] |= raw.Mask[i] && readAfterFilter[i];
        }
    }
}
