using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Statements.Mechanism;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Diagnostics;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// Index-driven probing for the typed join: when an INNER join to a small hashed table can find the probe rows
/// through a secondary index on the probe's join column, only those rows are read instead of the whole table.
/// </summary>
/// <remarks>
/// <para>
/// Every join being INNER, a probe row whose key is not in the hashed table produces no joined row, so reading
/// only the rows the index lists under the hashed keys gives the same rows. They are read in row-key order,
/// which is the order a scan visits them in, so groups, ties and unordered results come out as from the scan.
/// </para>
/// <para>
/// The index is used when the hashed table has few keys and the rows they name are a small part of the probe
/// table (<see cref="IndexDrivenShare"/>): a lookup and a read by key cost several times more per row than a
/// scan, so a filter that keeps more rows is faster as a scan. SQLite's planner makes the same choice for J7
/// (<c>SCAN p | SEARCH o USING INDEX ix_orders_product</c>).
/// </para>
/// </remarks>
internal partial class Select
{
    /// <summary>The index drives the probe when it names at most 1/<c>IndexDrivenShare</c> of the probe's rows.</summary>
    private const int IndexDrivenShare = 8;

    /// <summary>How many hashed keys are looked up to estimate the rows an index names; their rows are kept.</summary>
    private const int IndexEstimateSample = 32;

    /// <summary>
    /// The probe rows to read through an index, in row-key order; null when the probe is cheaper to scan or no
    /// join step allows it (a LEFT JOIN, which keeps every probe row, or no single-column index on the key).
    /// </summary>
    private static List<RowKey>? TryIndexDrivenProbeRows(DataVoEngine engine, JoinSide[] sides, JoinStep[] steps, int probeIndex, string databaseName)
    {
        JoinSide probe = sides[probeIndex];
        long probeRows = EstimateRows(engine, probe, databaseName);
        if (steps.Any(step => step.Outer) || probeRows is <= 0 or long.MaxValue || !engine.StorageContext.CanStreamRows(probe.Detail.TableName, databaseName))
        {
            return null;
        }

        // Sample each candidate's keys, then finish collecting the rows of the best one only.
        long budget = probeRows / IndexDrivenShare;
        List<RowKey>? best = null;
        List<RowKey>? spare = null;
        JoinStep? bestStep = null;
        string? bestIndex = null;
        long bestEstimate = budget;
        int bestSampled = 0;
        foreach (JoinStep step in steps)
        {
            if (step.SourceSide != probeIndex
                || step.Table.KeyCount > budget
                || IntegerIndexOn(engine, probe, probe.Columns[step.SourceOrdinal].Name, databaseName) is not { } indexName)
            {
                continue;
            }

            List<RowKey> sample = spare ?? RentRowKeyList();
            spare = null;
            sample.Clear();
            long estimate = SampleIndexedRows(engine, probe, step.Table, indexName, databaseName, bestEstimate, sample, out int sampled);
            if (estimate <= bestEstimate)
            {
                spare = best;
                (best, bestStep, bestIndex, bestEstimate, bestSampled) = (sample, step, indexName, estimate, sampled);
            }
            else
            {
                spare = sample;
            }
        }

        if (spare is not null)
        {
            ReleaseRowKeyList(spare);
        }

        if (best is null || !CollectIndexedRows(engine, probe, bestStep!.Table, bestIndex!, databaseName, budget, bestSampled, best))
        {
            if (best is not null)
            {
                ReleaseRowKeyList(best);
            }

            return null;
        }

        best.Sort();
        RuntimeQueryDiagnosticsScope.RecordIndexUse(bestIndex!);
        return best;
    }

    /// <summary>The largest row-key list kept per thread for the next index-driven join.</summary>
    private const int RetainedRowKeys = 256 * 1024;

    [ThreadStatic]
    private static List<RowKey>? _rowKeyLists;

    /// <summary>An empty row-key list: the one this thread kept from an earlier join, or a new one.</summary>
    private static List<RowKey> RentRowKeyList()
    {
        List<RowKey> rows = _rowKeyLists ?? [];
        _rowKeyLists = null;
        rows.Clear();
        return rows;
    }

    /// <summary>Keeps <paramref name="rows"/> for the next join on this thread, unless it has grown past reason.</summary>
    private static void ReleaseRowKeyList(List<RowKey> rows)
    {
        if (rows.Capacity <= RetainedRowKeys)
        {
            rows.Clear();
            _rowKeyLists = rows;
        }
    }

    /// <summary>
    /// Appends to <paramref name="rows"/> the rows the index lists under the first <see cref="IndexEstimateSample"/>
    /// keys of <paramref name="table"/> (all of them when there are fewer) and returns the rows all its keys are
    /// estimated to name. Stops early, returning <see cref="long.MaxValue"/>, as soon as the sample alone is past
    /// its share of <paramref name="limit"/>.
    /// </summary>
    private static long SampleIndexedRows(DataVoEngine engine, JoinSide probe, JoinHashTable table, string indexName, string databaseName, long limit, List<RowKey> rows, out int sampled)
    {
        sampled = 0;
        int keyCount = table.KeyCount;
        long sampleLimit = limit * Math.Min(IndexEstimateSample, keyCount) / Math.Max(1, keyCount);
        foreach (long key in table.Keys)
        {
            engine.IndexManager.AppendIntegerIndexRows(key, indexName, probe.Detail.TableName, databaseName, rows);
            if (rows.Count > sampleLimit)
            {
                return long.MaxValue;
            }

            if (++sampled == IndexEstimateSample)
            {
                break;
            }
        }

        return sampled == 0 ? 0 : (long)rows.Count * keyCount / sampled;
    }

    /// <summary>
    /// Appends the rows of the keys after the first <paramref name="skip"/> (already sampled) to
    /// <paramref name="rows"/>; false once there are more than <paramref name="limit"/>.
    /// </summary>
    private static bool CollectIndexedRows(DataVoEngine engine, JoinSide probe, JoinHashTable table, string indexName, string databaseName, long limit, int skip, List<RowKey> rows)
    {
        int index = 0;
        foreach (long key in table.Keys)
        {
            if (index++ < skip)
            {
                continue;
            }

            engine.IndexManager.AppendIntegerIndexRows(key, indexName, probe.Detail.TableName, databaseName, rows);
            if (rows.Count > limit)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>The name of a single-column B-tree index on <paramref name="column"/> whose lookups are exact, else null.</summary>
    private static string? IntegerIndexOn(DataVoEngine engine, JoinSide side, string column, string databaseName)
    {
        foreach (IndexFile index in engine.Catalog.GetTableIndexesReadOnly(side.Detail.TableName, databaseName))
        {
            if (index.AttributeNames.Count == 1
                && index.AttributeNames[0].Equals(column, StringComparison.OrdinalIgnoreCase)
                && SqlIndexLookupKey.IsExactIntegerIndex(engine, side.Detail.TableName, databaseName, column, index.IndexFileName))
            {
                return index.IndexFileName;
            }
        }

        return null;
    }
}
