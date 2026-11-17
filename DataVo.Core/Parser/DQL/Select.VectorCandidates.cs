using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// Filters the candidates of a vector-index search by the WHERE predicates on their table before any row is
/// materialized. A filtered nearest-neighbour query asks the index for about <c>LIMIT / selectivity</c>
/// candidates; testing them used to load every candidate as a dictionary, stored vector included, twice.
/// Here the predicates read only the columns they name from the stored row, and only the surviving rows
/// are loaded afterwards.
/// </summary>
internal partial class Select
{
    /// <summary>The typed predicates of one table and the scratch state to test stored rows against them.</summary>
    private sealed class VectorCandidateFilter
    {
        public required TypedScanState Scan { get; init; }

        /// <summary>The candidates that passed, in the order they were visited.</summary>
        public List<RowKey> Survivors { get; } = [];
    }

    /// <summary>
    /// Builds a candidate filter for <paramref name="predicate"/>, a condition on the FROM table alone.
    /// Returns null when the condition has a shape the typed predicates do not cover (the caller then
    /// tests materialized rows, as before) or when the table cannot be read in stored form.
    /// </summary>
    private VectorCandidateFilter? TryBuildCandidateFilter(ExpressionNode? predicate, string tableName)
    {
        if (predicate is null
            || IsAllTrueExpression(predicate)
            || _activeSnapshot is not null
            || _model.Database is null
            || _model.FromTable is null
            || _model.FromTable.IsMaterialized
            || _model.CteTables.ContainsKey(_model.FromTable.TableName)
            || !tableName.Equals(ResolveRealTableName(_model.FromTable.TableName), StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        DataVoEngine engine = DataVoEngine.Current();
        IReadOnlyList<Column> columns = engine.Catalog.GetTableColumns(tableName, _model.Database);
        var predicates = new List<TypedPredicate>();
        if (columns.Count == 0 || !TryCollectTypedPredicates(predicate, _model.FromTable.TableName, columns, predicates))
        {
            return null;
        }

        var filterMask = new bool[columns.Count];
        MarkFilterColumns(predicates, filterMask);
        ClusteredKeyLayout? clustered = engine.StorageContext.ResolveClusteredKey(_model.Database, tableName);
        int clusteredOrdinal = clustered?.Ordinal ?? -1;
        return new VectorCandidateFilter
        {
            Scan = new TypedScanState
            {
                Columns = columns,
                Predicates = predicates,
                FilterMask = filterMask,
                RestMask = new bool[columns.Count],
                Cells = new CellValue[columns.Count],
                ClusteredOrdinal = clusteredOrdinal,
                ClusteredIsGuid = clustered is not null
                    && string.Equals(columns[clusteredOrdinal].Type, "GUID", StringComparison.OrdinalIgnoreCase),
                TableName = tableName,
            },
        };
    }

    /// <summary>
    /// Returns the rows of <paramref name="candidates"/> that pass <paramref name="filter"/>, in candidate
    /// order; null when the rows cannot be visited in stored form here, so the caller keeps every candidate.
    /// </summary>
    private List<RowKey>? TryFilterCandidates(VectorCandidateFilter filter, List<RowKey> candidates, string tableName)
    {
        filter.Survivors.Clear();
        if (candidates.Count == 0)
        {
            return [];
        }

        VectorCandidateFilter state = filter;
        return DataVoEngine.Current().StorageContext.VisitRowsByKey(tableName, _model.Database!, candidates, ref state, VisitCandidateRow)
            ? [.. filter.Survivors]
            : null;
    }

    /// <summary>
    /// The exact answer by filtering first: scans the table for the rows passing <paramref name="filter"/> and
    /// ranks only those by the distance of the vectors the index stores, returning the nearest
    /// <paramref name="requiredRows"/>. Used when widening the index search still found too few passing rows,
    /// which happens when few rows pass or the passing rows lie far from the query. Returns null when the
    /// table cannot be scanned in stored form or the index cannot rank rows.
    /// </summary>
    private List<RowKey>? TryFilterThenRank(VectorCandidateFilter filter, float[] queryVector, string indexName, string tableName, string indexKind, int requiredRows)
    {
        filter.Survivors.Clear();
        VectorCandidateFilter state = filter;
        DataVoEngine engine = DataVoEngine.Current();
        if (!engine.StorageContext.ScanTableRows(tableName, _model.Database!, null, null, ref state, VisitCandidateRow))
        {
            return null;
        }

        IncrementHybridRoutingCounter("hybrid.orderby.filter_then_rank");
        return engine.IndexManager.TryRankRowsByVectorDistance(queryVector, filter.Survivors, requiredRows, indexName, tableName, _model.Database!, indexKind);
    }

    /// <summary>Keeps a candidate when the filter columns of its stored row meet every predicate.</summary>
    private static bool VisitCandidateRow(ref VectorCandidateFilter filter, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        TypedScanState scan = filter.Scan;
        Load(scan, rowId, rowBytes, typedCells, scan.FilterMask);
        if (MatchesAll(scan.Predicates, scan.Cells, scan.Columns))
        {
            filter.Survivors.Add(rowId);
        }

        return true;
    }
}
