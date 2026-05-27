using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Runtime.Diagnostics;

internal sealed class RuntimeQueryStatsBuilder
{
    private readonly HashSet<string> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _indexesUsed = new(StringComparer.OrdinalIgnoreCase);

    public required string QueryText { get; init; }
    public string Operation { get; private set; } = "UNKNOWN";
    public required StorageMode StorageMode { get; init; }
    public string? DatabaseName { get; init; }
    public long RowsRead { get; private set; }
    public long RowsScanned { get; private set; }
    public int RowsReturned { get; private set; }
    public int RowsAffected { get; private set; }
    public bool FullTableScan { get; private set; }
    public bool VectorIndexUsed { get; private set; }
    public int VectorTopK { get; private set; }
    public int VectorExpansionPasses { get; private set; }
    public bool IsError { get; private set; }
    public string? ErrorMessage { get; private set; }

    public void SetOperation(string operation)
    {
        if (!string.IsNullOrWhiteSpace(operation))
        {
            Operation = operation.ToUpperInvariant();
        }
    }

    public void AddTable(string tableName)
    {
        if (!string.IsNullOrWhiteSpace(tableName))
        {
            _tables.Add(tableName);
        }
    }

    public void AddIndex(string indexName)
    {
        if (!string.IsNullOrWhiteSpace(indexName))
        {
            _indexesUsed.Add(indexName);
        }
    }

    public void AddRowsRead(long count) => RowsRead += Math.Max(0, count);
    public void AddRowsScanned(long count) => RowsScanned += Math.Max(0, count);
    public void MarkFullTableScan() => FullTableScan = true;
    public void AddRowsReturned(int count) => RowsReturned += Math.Max(0, count);
    public void AddRowsAffected(int count) => RowsAffected += Math.Max(0, count);

    public void RecordVectorSearch(string indexName, int topK, int expansionPasses)
    {
        VectorIndexUsed = true;
        VectorTopK = Math.Max(VectorTopK, topK);
        VectorExpansionPasses += Math.Max(0, expansionPasses);
        AddIndex(indexName);
    }

    public void RecordError(string message)
    {
        IsError = true;
        ErrorMessage = message;
    }

    public RuntimeQueryStats Build(TimeSpan elapsed)
    {
        return new RuntimeQueryStats(
            QueryText,
            Operation,
            StorageMode,
            DatabaseName,
            _tables.OrderBy(static item => item, StringComparer.OrdinalIgnoreCase).ToArray(),
            _indexesUsed.OrderBy(static item => item, StringComparer.OrdinalIgnoreCase).ToArray(),
            elapsed,
            RowsRead,
            RowsScanned,
            RowsReturned,
            RowsAffected,
            FullTableScan,
            VectorIndexUsed,
            VectorTopK,
            VectorExpansionPasses,
            IsError,
            ErrorMessage);
    }
}
