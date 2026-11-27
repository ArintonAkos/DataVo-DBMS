using DataVo.Core.Contracts.Results;
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

    public int JoinExecutions { get; private set; }

    public void RecordJoinExecution() => JoinExecutions++;

    public void RecordError(string message)
    {
        IsError = true;
        ErrorMessage = message;
    }

    /// <summary>
    /// Infers the uppercase leading SQL operation keyword from a raw statement string.
    /// </summary>
    /// <param name="sql">The raw SQL query text.</param>
    /// <returns>The leading SQL verb in uppercase, or <c>"UNKNOWN"</c> when the input is blank.</returns>
    public static string InferOperation(string sql)
    {
        string trimmed = sql.TrimStart();
        if (trimmed.Length == 0)
        {
            return "UNKNOWN";
        }

        int end = 0;
        while (end < trimmed.Length && !char.IsWhiteSpace(trimmed[end]) && trimmed[end] != ';')
        {
            end++;
        }

        return trimmed[..end].ToUpperInvariant();
    }

    /// <summary>
    /// Aggregates row counts and error details from a completed <see cref="QueryResult"/> into this builder.
    /// </summary>
    /// <param name="result">The executed statement result.</param>
    public void RecordResultMetrics(QueryResult result)
    {
        if (result.IsError)
        {
            RecordError(string.Join(" | ", result.Messages));
        }

        if (result.Data.Count > 0)
        {
            AddRowsReturned(result.Data.Count);
        }

        if (result.RowsAffected > 0)
        {
            AddRowsAffected(result.RowsAffected);
        }

        foreach (string message in result.Messages)
        {
            if (result.Data.Count == 0 && TryReadMessageCount(message, "Rows selected:", out int selected))
            {
                AddRowsReturned(selected);
            }
        }
    }

    private static bool TryReadMessageCount(string message, string prefix, out int count)
    {
        count = 0;
        return message.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            && int.TryParse(message[prefix.Length..].Trim(), out count);
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
            ErrorMessage)
        {
            JoinExecutions = JoinExecutions,
        };
    }
}
