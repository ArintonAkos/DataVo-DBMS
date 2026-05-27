using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Runtime.Diagnostics;

/// <summary>
/// Immutable runtime diagnostics captured for a single query or direct context operation.
/// </summary>
public sealed class RuntimeQueryStats
{
    internal RuntimeQueryStats(
        string queryText,
        string operation,
        StorageMode storageMode,
        string? databaseName,
        IReadOnlyList<string> tables,
        IReadOnlyList<string> indexesUsed,
        TimeSpan elapsed,
        long rowsRead,
        long rowsScanned,
        int rowsReturned,
        int rowsAffected,
        bool fullTableScan,
        bool vectorIndexUsed,
        int vectorTopK,
        int vectorExpansionPasses,
        bool isError,
        string? errorMessage)
    {
        QueryText = queryText;
        Operation = operation;
        StorageMode = storageMode;
        DatabaseName = databaseName;
        Tables = tables;
        IndexesUsed = indexesUsed;
        Elapsed = elapsed;
        RowsRead = rowsRead;
        RowsScanned = rowsScanned;
        RowsReturned = rowsReturned;
        RowsAffected = rowsAffected;
        FullTableScan = fullTableScan;
        VectorIndexUsed = vectorIndexUsed;
        VectorTopK = vectorTopK;
        VectorExpansionPasses = vectorExpansionPasses;
        IsError = isError;
        ErrorMessage = errorMessage;
    }

    /// <summary>Gets the submitted query text or context operation descriptor.</summary>
    public string QueryText { get; }
    /// <summary>Gets the inferred high-level operation name.</summary>
    public string Operation { get; }
    /// <summary>Gets the storage mode active for the query.</summary>
    public StorageMode StorageMode { get; }
    /// <summary>Gets the active database name when one was selected.</summary>
    public string? DatabaseName { get; }
    /// <summary>Gets the distinct table names referenced by the query.</summary>
    public IReadOnlyList<string> Tables { get; }
    /// <summary>Gets the distinct indexes observed during execution.</summary>
    public IReadOnlyList<string> IndexesUsed { get; }
    /// <summary>Gets the end-to-end elapsed query time.</summary>
    public TimeSpan Elapsed { get; }
    /// <summary>Gets the number of rows loaded by direct row-id reads.</summary>
    public long RowsRead { get; }
    /// <summary>Gets the number of rows scanned through full-table access.</summary>
    public long RowsScanned { get; }
    /// <summary>Gets the number of rows returned to the caller.</summary>
    public int RowsReturned { get; }
    /// <summary>Gets the number of rows modified by the operation.</summary>
    public int RowsAffected { get; }
    /// <summary>Gets whether execution observed a full-table scan.</summary>
    public bool FullTableScan { get; }
    /// <summary>Gets whether a vector index participated in execution.</summary>
    public bool VectorIndexUsed { get; }
    /// <summary>Gets the largest vector top-k requested during execution.</summary>
    public int VectorTopK { get; }
    /// <summary>Gets the accumulated vector fast-path expansion pass count.</summary>
    public int VectorExpansionPasses { get; }
    /// <summary>Gets whether execution ended in an error.</summary>
    public bool IsError { get; }
    /// <summary>Gets the last recorded error message, if any.</summary>
    public string? ErrorMessage { get; }
}
