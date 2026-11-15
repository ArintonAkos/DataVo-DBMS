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

    /// <summary>How many hashed keys are looked up to estimate the rows an index names.</summary>
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

        // Estimate each candidate from a sample of its keys, then collect the rows of the best one only.
        long budget = probeRows / IndexDrivenShare;
        JoinStep? bestStep = null;
        string? bestIndex = null;
        long bestEstimate = budget;
        foreach (JoinStep step in steps)
        {
            if (step.SourceSide != probeIndex
                || step.Table.KeyCount > budget
                || IntegerIndexOn(engine, probe, probe.Columns[step.SourceOrdinal].Name, databaseName) is not { } indexName)
            {
                continue;
            }

            long estimate = EstimateIndexedRows(engine, probe, step.Table, indexName, databaseName);
            if (estimate <= bestEstimate)
            {
                (bestStep, bestIndex, bestEstimate) = (step, indexName, estimate);
            }
        }

        if (bestStep is null || CollectIndexedRows(engine, probe, bestStep.Table, bestIndex!, databaseName, budget, (int)bestEstimate) is not { } rows)
        {
            return null;
        }

        rows.Sort();
        RuntimeQueryDiagnosticsScope.RecordIndexUse(bestIndex!);
        return rows;
    }

    /// <summary>
    /// How many probe rows the index lists under the keys of <paramref name="table"/>, extrapolated from the
    /// first <see cref="IndexEstimateSample"/> keys (all of them when there are fewer).
    /// </summary>
    private static long EstimateIndexedRows(DataVoEngine engine, JoinSide probe, JoinHashTable table, string indexName, string databaseName)
    {
        long sampled = 0;
        int keys = 0;
        foreach (long key in table.Keys)
        {
            sampled += engine.IndexManager.PeekIntegerIndex(key, indexName, probe.Detail.TableName, databaseName).Count;
            if (++keys == IndexEstimateSample)
            {
                break;
            }
        }

        return keys == 0 ? 0 : sampled * table.KeyCount / keys;
    }

    /// <summary>The row keys the index lists under every key of <paramref name="table"/>; null once there are more than <paramref name="limit"/>.</summary>
    private static List<RowKey>? CollectIndexedRows(DataVoEngine engine, JoinSide probe, JoinHashTable table, string indexName, string databaseName, long limit, int expected)
    {
        var rows = new List<RowKey>(expected);
        foreach (long key in table.Keys)
        {
            IReadOnlyList<RowKey> matches = engine.IndexManager.PeekIntegerIndex(key, indexName, probe.Detail.TableName, databaseName);
            if (rows.Count + matches.Count > limit)
            {
                return null;
            }

            for (int i = 0; i < matches.Count; i++)
            {
                rows.Add(matches[i]);
            }
        }

        return rows;
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
