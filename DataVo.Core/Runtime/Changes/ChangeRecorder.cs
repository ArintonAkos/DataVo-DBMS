using DataVo.Core.Runtime;

namespace DataVo.Core.Runtime.Changes;

/// <summary>
/// Accumulates <see cref="RowChange"/> records for a single DML apply scope and publishes a
/// <see cref="ChangeSet"/> when the scope completes successfully.
/// </summary>
/// <remarks>
/// Used by the auto-commit DML executors and the transaction commit flush. When change capture is
/// disabled, the recorder short-circuits: <see cref="IsEnabled"/> is <c>false</c> and callers skip
/// building row images entirely (no added allocation). Only successful applies should call
/// <see cref="Publish"/>; abandoning the recorder (e.g. on error/rollback) emits nothing.
/// </remarks>
internal sealed class ChangeRecorder
{
    private readonly ChangeCapture _capture;
    private readonly string _databaseName;
    private List<RowChange>? _changes;

    private ChangeRecorder(ChangeCapture capture, string databaseName)
    {
        _capture = capture;
        _databaseName = databaseName;
    }

    /// <summary>
    /// Creates a recorder bound to the engine's capture service, or <c>null</c> when capture is disabled
    /// so callers can cheaply skip all change-tracking work.
    /// </summary>
    public static ChangeRecorder? TryCreate(DataVoEngine engine, string databaseName)
    {
        ChangeCapture capture = engine.Changes;
        return capture.Enabled ? new ChangeRecorder(capture, databaseName) : null;
    }

    /// <summary>Gets a value indicating whether the bound capture service is enabled.</summary>
    public bool IsEnabled => _capture.Enabled;

    /// <summary>Records an inserted row image.</summary>
    public void RecordInsert(string table, long rowId, IReadOnlyDictionary<string, object?> after)
        => Add(new RowChange(table, rowId, ChangeKind.Insert, before: null, after: Clone(after)));

    /// <summary>
    /// Records an inserted row image whose dictionary is already a fresh owned normalized row.
    /// Used by the typed insert fast lane to avoid a redundant after-image clone.
    /// </summary>
    public void RecordTypedInsert(
        string table,
        long rowId,
        IReadOnlyDictionary<string, object?> ownedAfter,
        TypedRow typedAfter)
        => Add(new RowChange(table, rowId, ChangeKind.Insert, before: null, after: ownedAfter, typedAfter));

    /// <summary>Records a deleted row image.</summary>
    public void RecordDelete(string table, long rowId, IReadOnlyDictionary<string, object?> before)
        => Add(new RowChange(table, rowId, ChangeKind.Delete, before: Clone(before), after: null));

    /// <summary>Records an updated row's before and after images.</summary>
    public void RecordUpdate(string table, long rowId,
        IReadOnlyDictionary<string, object?> before,
        IReadOnlyDictionary<string, object?> after)
        => Add(new RowChange(table, rowId, ChangeKind.Update, before: Clone(before), after: Clone(after)));

    private void Add(RowChange change)
    {
        _changes ??= [];
        _changes.Add(change);
    }

    /// <summary>
    /// Publishes the accumulated changes as a single <see cref="ChangeSet"/> when any were recorded.
    /// </summary>
    public void Publish()
    {
        if (_changes is null || _changes.Count == 0)
        {
            return;
        }

        _capture.Publish(new ChangeSet(_capture.NextSequenceId(), _databaseName, _changes));
    }

    private static IReadOnlyDictionary<string, object?> Clone(IReadOnlyDictionary<string, object?> row)
    {
        // Snapshot the row so later mutation of the underlying storage dictionary cannot leak into the
        // captured before/after images. Single allocation (no intermediate ToDictionary copy).
        var clone = new Dictionary<string, object?>(row.Count, StringComparer.OrdinalIgnoreCase);
        foreach (KeyValuePair<string, object?> pair in row)
        {
            clone[pair.Key] = pair.Value;
        }

        return clone;
    }
}
