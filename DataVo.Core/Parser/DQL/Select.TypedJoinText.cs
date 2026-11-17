using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Utils;
using CatalogColumn = DataVo.Core.Models.Catalog.Column;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// How the typed join reads the serialized rows of a hashed table. The filter columns are read first, with text
/// conditions compared on the stored UTF-8 bytes; the columns the join keeps are decoded only for rows that pass,
/// and kept text is shared per distinct value. Hashing 100,000 customers for <c>WHERE c.Region = @r</c> used to
/// decode every customer's region and name to strings before testing the region (4.1 to 11.5 MB per query).
/// </summary>
internal partial class Select
{
    /// <summary>The column plan of <see cref="LoadSerializedBuildRow"/> for one hashed table.</summary>
    private sealed class JoinBuildLoad
    {
        /// <summary>The conditions' columns and the join key, decoded before the conditions are tested.</summary>
        public required bool[] FilterMask { get; init; }

        /// <summary>The text condition columns compared in place; null when none can be.</summary>
        public RawTextColumns? RawText { get; init; }

        /// <summary>Kept columns not read by the filter step, decoded for rows that pass.</summary>
        public required bool[] RestDecode { get; init; }

        /// <summary>Kept text columns not read by the filter step, located and then shared through <see cref="Interners"/>.</summary>
        public required bool[] RestLocate { get; init; }

        /// <summary>The last column of the rest step, or -1 when it reads nothing.</summary>
        public required int RestLast { get; init; }

        public required int[] Offsets { get; init; }

        public required int[] Lengths { get; init; }

        /// <summary>Per column in <see cref="RestLocate"/>, the strings handed out so far.</summary>
        public required Utf8StringInterner?[] Interners { get; init; }
    }

    /// <summary>
    /// Plans the two steps for a hashed table: <paramref name="filter"/> says whether its conditions are tested while
    /// it is hashed; <paramref name="keep"/> names the columns its joined rows read.
    /// </summary>
    private static JoinBuildLoad PlanJoinBuildLoad(JoinSide side, int keyOrdinal, bool filter, bool[] keep)
    {
        int count = side.Columns.Count;
        var filterMask = new bool[count];
        if (filter)
        {
            MarkFilterColumns(side.Predicates, filterMask);
        }

        RawTextColumns? raw = filter ? TryPlanRawText(side.Columns, side.Predicates, filterMask) : null;
        filterMask[keyOrdinal] = true;
        if (raw is not null)
        {
            raw.Mask[keyOrdinal] = false; // the key is always decoded: it is hashed
            raw.DecodeMask[keyOrdinal] = true;
        }

        bool[] loaded = raw?.DecodeMask ?? filterMask;
        var restDecode = new bool[count];
        var restLocate = new bool[count];
        var interners = new Utf8StringInterner?[count];
        int restLast = -1;
        for (int i = 0; i < count; i++)
        {
            if (!keep[i] || loaded[i])
            {
                continue;
            }

            restLast = i;
            if (IsTextColumn(side.Columns[i]))
            {
                restLocate[i] = true;
                interners[i] = new Utf8StringInterner();
            }
            else
            {
                restDecode[i] = true;
            }
        }

        return new JoinBuildLoad
        {
            FilterMask = filterMask,
            RawText = raw is null ? null : new RawTextColumns
            {
                Mask = raw.Mask,
                DecodeMask = raw.DecodeMask,
                Last = Math.Max(raw.Last, keyOrdinal),
                Offsets = raw.Offsets,
                Lengths = raw.Lengths,
            },
            RestDecode = restDecode,
            RestLocate = restLocate,
            RestLast = restLast,
            Offsets = new int[count],
            Lengths = new int[count],
            Interners = interners,
        };
    }

    /// <summary>
    /// Loads a serialized row of a hashed table into <paramref name="cells"/>: the filter step, then, when the row
    /// passes, the kept columns. Returns false when the row fails the table's conditions.
    /// </summary>
    private static bool LoadSerializedBuildRow(JoinSide side, JoinBuildLoad load, bool filter, RowKey rowId, ReadOnlySpan<byte> rowBytes, CellValue[] cells)
    {
        IReadOnlyList<CatalogColumn> columns = side.Columns;
        if (load.RawText is { } raw)
        {
            RowSerializer.DecodeCellsLocatingText(rowBytes, columns, raw.DecodeMask, raw.Mask, raw.Last, cells, raw.Offsets, raw.Lengths);
            RestoreJoinKey(side, rowId, raw.DecodeMask, cells);
            if (!JoinMatchesAllRaw(cells, columns, raw, side.Predicates, rowBytes))
            {
                return false;
            }
        }
        else
        {
            RowSerializer.DecodeCellsByOrdinal(rowBytes, columns, load.FilterMask, cells);
            RestoreJoinKey(side, rowId, load.FilterMask, cells);
            if (filter && !MatchesAll(side.Predicates, cells, columns))
            {
                return false;
            }
        }

        if (load.RestLast >= 0)
        {
            LoadKeptBuildColumns(side, load, rowId, rowBytes, cells);
        }

        return true;
    }

    /// <summary>Decodes the kept columns the filter step did not read; kept text is shared per distinct value.</summary>
    private static void LoadKeptBuildColumns(JoinSide side, JoinBuildLoad load, RowKey rowId, ReadOnlySpan<byte> rowBytes, CellValue[] cells)
    {
        RowSerializer.DecodeCellsLocatingText(rowBytes, side.Columns, load.RestDecode, load.RestLocate, load.RestLast, cells, load.Offsets, load.Lengths);
        for (int i = 0; i <= load.RestLast; i++)
        {
            if (load.RestLocate[i])
            {
                int length = load.Lengths[i];
                cells[i] = length < 0 ? CellValue.Null : CellValue.From(load.Interners[i]!.Intern(rowBytes.Slice(load.Offsets[i], length)));
            }
        }

        RestoreJoinKey(side, rowId, load.RestDecode, cells);
    }

    /// <summary>
    /// Whether a hashed row meets every predicate: text columns in <paramref name="raw"/> are compared where they lie
    /// in <paramref name="rowBytes"/>, the others read from <paramref name="cells"/>.
    /// </summary>
    /// <remarks>
    /// The single-table scan has its own copy (<see cref="MatchesAllRaw"/>): sharing one method let the runtime
    /// optimize it for whichever path ran first, and a scan run after a join was 25% slower.
    /// </remarks>
    private static bool JoinMatchesAllRaw(CellValue[] cells, IReadOnlyList<CatalogColumn> columns, RawTextColumns raw, IReadOnlyList<TypedPredicate> predicates, ReadOnlySpan<byte> rowBytes)
    {
        for (int i = 0; i < predicates.Count; i++)
        {
            if (!JoinMatchesRaw(cells, columns, raw, predicates[i], rowBytes))
            {
                return false;
            }
        }

        return true;
    }

    private static bool JoinMatchesRaw(CellValue[] cells, IReadOnlyList<CatalogColumn> columns, RawTextColumns raw, TypedPredicate predicate, ReadOnlySpan<byte> rowBytes)
    {
        if (predicate.Branches is { } branches)
        {
            foreach (TypedPredicate[] branch in branches)
            {
                if (JoinMatchesAllRaw(cells, columns, raw, branch, rowBytes))
                {
                    return true;
                }
            }

            return false;
        }

        int ordinal = predicate.Ordinal;
        if (!raw.Mask[ordinal])
        {
            return Matches(predicate, cells[ordinal], columns[ordinal]);
        }

        int length = raw.Lengths[ordinal];
        return predicate.Kind switch
        {
            TypedPredicateKind.IsNull => length < 0,
            TypedPredicateKind.IsNotNull => length >= 0,

            // A comparison with NULL is unknown: the row is not kept.
            _ => length >= 0 && MatchesText(predicate, columns[ordinal], rowBytes.Slice(raw.Offsets[ordinal], length)),
        };
    }
}
