using System.Diagnostics;

namespace DataVo.Core.Runtime.Diagnostics;

internal sealed class RuntimeQueryDiagnosticsScope : IDisposable
{
    private static readonly AsyncLocal<RuntimeQueryDiagnosticsScope?> CurrentScope = new();
    private readonly DataVoDiagnostics _owner;
    private readonly RuntimeQueryDiagnosticsScope? _previous;
    private readonly Stopwatch _stopwatch;
    private bool _disposed;

    private RuntimeQueryDiagnosticsScope(DataVoDiagnostics owner, RuntimeQueryStatsBuilder builder)
    {
        _owner = owner;
        Builder = builder;
        _previous = CurrentScope.Value;
        CurrentScope.Value = this;
        _stopwatch = Stopwatch.StartNew();
    }

    public RuntimeQueryStatsBuilder Builder { get; }

    public static RuntimeQueryDiagnosticsScope? Start(DataVoDiagnostics owner, RuntimeQueryStatsBuilder? builder)
    {
        return owner.Enabled && builder is not null ? new RuntimeQueryDiagnosticsScope(owner, builder) : null;
    }

    public static void RecordTableRead(string tableName, long rowsRead)
    {
        RuntimeQueryDiagnosticsScope? scope = CurrentScope.Value;
        if (scope is null)
        {
            return;
        }

        scope.Builder.AddTable(tableName);
        scope.Builder.AddRowsRead(rowsRead);
    }

    public static void RecordTableScan(string tableName, long rowsScanned)
    {
        RuntimeQueryDiagnosticsScope? scope = CurrentScope.Value;
        if (scope is null)
        {
            return;
        }

        scope.Builder.AddTable(tableName);
        scope.Builder.AddRowsScanned(rowsScanned);
        scope.Builder.MarkFullTableScan();
    }

    public static void RecordIndexUse(string indexName)
    {
        CurrentScope.Value?.Builder.AddIndex(indexName);
    }

    public static void RecordVectorSearch(string indexName, int topK, int expansionPasses)
    {
        CurrentScope.Value?.Builder.RecordVectorSearch(indexName, topK, expansionPasses);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _stopwatch.Stop();
        CurrentScope.Value = _previous;
        _owner.Record(Builder.Build(_stopwatch.Elapsed));
    }
}
