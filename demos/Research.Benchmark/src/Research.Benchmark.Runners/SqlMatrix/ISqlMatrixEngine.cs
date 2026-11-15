namespace Research.Benchmark.Runners.SqlMatrix;

/// <summary>
/// One embedded database driven through the SQL capability matrix. Every query is prepared once per engine
/// and executed with bound parameters.
/// </summary>
public interface ISqlMatrixEngine : IDisposable
{
    string Name { get; }

    /// <summary>Storage, durability and API path, printed under the results.</summary>
    string Notes { get; }

    /// <summary>Whether <see cref="CreateColumnarIndex"/> is available (DataVo only).</summary>
    bool SupportsColumnarIndex { get; }

    /// <summary>
    /// How many queries the columnar index has answered so far, or <see langword="null"/> without one. The
    /// runner reads it around a query to report whether that query was pushed down.
    /// </summary>
    long? ColumnarScans { get; }

    void Create(string directory);

    void Load(SqlMatrixData data);

    /// <summary>Closes and reopens a file-backed database so queries read from its files; a no-op in memory.</summary>
    void Reopen();

    /// <summary>Runs the query and returns its rows as CLR values in select-list order (for the answer check).</summary>
    List<object?[]> Query(SqlMatrixQuery query, SqlMatrixParameter[] parameters);

    /// <summary>Runs the query and reads every cell of every row, as an application would. Returns the row count.</summary>
    long Execute(SqlMatrixQuery query, SqlMatrixParameter[] parameters);

    void CreateColumnarIndex();

    long SizeOnDiskBytes();
}
