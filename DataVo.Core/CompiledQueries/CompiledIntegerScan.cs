using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.CompiledQueries;

internal static partial class CompiledIntegerScan
{
    public static List<T> ScanMany<T>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        CompiledRowMapper<T> mapper)
    {
        var results = new List<T>();
        ScanRows(rows, whereColumn, expected, (row, state) =>
        {
            state.Add(mapper(new CompiledRowReader(row.AsView())));
            return true;
        }, results);

        return results;
    }

    public static bool TryScanMany<T>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        CompiledRowMapper<T> mapper,
        out List<T> results)
    {
        results = [];
        if (!IsInt32ScanEligible(rows, whereColumn))
        {
            return false;
        }

        results = ScanMany(rows, whereColumn, expected, mapper);
        return true;
    }

    public static T? ScanSingle<T>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        CompiledRowMapper<T> mapper)
    {
        var state = new SingleScanState<T>();
        ScanRows(rows, whereColumn, expected, (row, state) =>
        {
            state.Result = mapper(new CompiledRowReader(row.AsView()));
            return false;
        }, state);

        return state.Result;
    }

    public static bool TryScanSingle<T>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        CompiledRowMapper<T> mapper,
        out T? result)
    {
        result = default;
        if (!IsInt32ScanEligible(rows, whereColumn))
        {
            return false;
        }

        result = ScanSingle(rows, whereColumn, expected, mapper);
        return true;
    }

    private static bool TryReadInt32(StoredRow row, string column, out int value)
    {
        value = default;
        if (!row.Schema.TryGetOrdinal(column, out int ordinal))
        {
            return false;
        }

        CellValue cell = row[ordinal];
        if (cell.Type != CellType.Int32)
        {
            return false;
        }

        value = cell.AsInt32();
        return true;
    }

    private static bool IsInt32ScanEligible(Dictionary<long, StoredRow> rows, string whereColumn)
    {
        foreach ((_, StoredRow row) in rows)
        {
            if (!row.Schema.TryGetOrdinal(whereColumn, out int ordinal))
            {
                continue;
            }

            CellType type = row[ordinal].Type;
            if (type is not CellType.Int32 and not CellType.Null)
            {
                return false;
            }
        }

        return true;
    }

    private sealed class SingleScanState<T>
    {
        public T? Result;
    }
}
