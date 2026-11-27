using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.ValueConversion;

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
    private readonly string? _origin;
    private List<RowChange>? _changes;
    private Dictionary<string, ReactiveRowSchema>? _schemas;

    private ChangeRecorder(ChangeCapture capture, string databaseName, string? origin)
    {
        _capture = capture;
        _databaseName = databaseName;
        // Captured now: publication may be deferred to a commit callback that runs outside the origin scope.
        _origin = origin;
    }

    /// <summary>
    /// Creates a recorder bound to the engine's capture service, or <c>null</c> when capture is disabled
    /// or no subscriber is interested in the current write origin so callers can skip all change-tracking work.
    /// </summary>
    public static ChangeRecorder? TryCreate(DataVoEngine engine, string databaseName)
    {
        ChangeCapture capture = engine.Changes;
        if (!capture.Enabled)
        {
            return null;
        }

        string? origin = ChangeCapture.CurrentOrigin;
        return capture.HasInterestedSubscribers(origin) ? new ChangeRecorder(capture, databaseName, origin) : null;
    }

    /// <summary>Gets a value indicating whether the bound capture service is enabled.</summary>
    public bool IsEnabled => _capture.Enabled;

    /// <summary>Gets whether a subscriber wants changes announced before they are written.</summary>
    public bool WantsAnnouncements => _capture.HasInterestedRecordingSubscribers(_origin);

    /// <summary>Announces an insert before it is written. Call ahead of any storage or log write.</summary>
    public void AnnounceInsert(string table, IReadOnlyDictionary<string, object?> after)
    {
        if (WantsAnnouncements)
        {
            Announce(Capture(table, default, ChangeKind.Insert, before: null, after));
        }
    }

    /// <summary>Announces a typed insert before it is written. Call ahead of any storage or log write.</summary>
    public void AnnounceTypedInsert(string table, TypedRow typedAfter)
    {
        if (WantsAnnouncements)
        {
            Announce(new RowChange(table, default, typedAfter));
        }
    }

    /// <summary>Announces an update before it is written. Call ahead of any storage or log write.</summary>
    public void AnnounceUpdate(string table, RowKey rowId, IReadOnlyDictionary<string, object?> before, IReadOnlyDictionary<string, object?> after)
    {
        if (WantsAnnouncements)
        {
            Announce(Capture(table, rowId, ChangeKind.Update, before, after));
        }
    }

    /// <summary>Announces a delete before it is written. Call ahead of any storage or log write.</summary>
    public void AnnounceDelete(string table, RowKey rowId, IReadOnlyDictionary<string, object?> before)
    {
        if (WantsAnnouncements)
        {
            Announce(Capture(table, rowId, ChangeKind.Delete, before, after: null));
        }
    }

    private void Announce(RowChange change) => _capture.PublishRecording(_databaseName, change, _origin);

    /// <summary>Records an inserted row image.</summary>
    public void RecordInsert(string table, RowKey rowId, IReadOnlyDictionary<string, object?> after)
        => Add(Capture(table, rowId, ChangeKind.Insert, before: null, after));

    /// <summary>
    /// Records a typed insert carrying only the typed after-image. The owned dictionary after-image is
    /// materialized lazily (see <see cref="RowChange.After"/>), so borrowed-only subscriptions that read
    /// the typed lane never allocate it.
    /// </summary>
    public void RecordTypedInsert(string table, RowKey rowId, TypedRow typedAfter)
        => Add(new RowChange(table, rowId, typedAfter));

    /// <summary>Records a deleted row image.</summary>
    public void RecordDelete(string table, RowKey rowId, IReadOnlyDictionary<string, object?> before)
        => Add(Capture(table, rowId, ChangeKind.Delete, before, after: null));

    /// <summary>Records an updated row's before and after images.</summary>
    public void RecordUpdate(string table, RowKey rowId,
        IReadOnlyDictionary<string, object?> before,
        IReadOnlyDictionary<string, object?> after)
        => Add(Capture(table, rowId, ChangeKind.Update, before, after));

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

        // Subscribers are notified only once the statement's commit record is durable.
        if (DeferredPublication.TryDefer(Publish))
        {
            return;
        }

        _capture.Publish(new ChangeSet(_capture.NextSequenceId(), _databaseName, _changes, _origin));
    }

    /// <summary>
    /// Snapshots a change's images as typed rows, so typed readers (reactive operators) never touch dictionaries
    /// and owned readers get dictionaries built on demand. Falls back to dictionary snapshots when a value has no
    /// typed cell form.
    /// </summary>
    private RowChange Capture(
        string table,
        RowKey rowId,
        ChangeKind kind,
        IReadOnlyDictionary<string, object?>? before,
        IReadOnlyDictionary<string, object?>? after)
    {
        try
        {
            return RowChange.FromTyped(table, rowId, kind, CaptureTyped(table, before), CaptureTyped(table, after));
        }
        catch (NotSupportedException)
        {
            return new RowChange(table, rowId, kind, CloneOrNull(before), CloneOrNull(after));
        }
    }

    /// <summary>
    /// Copies a dictionary image into a typed row. Rows of one table share a schema (cached per table and rebuilt
    /// only when an image carries a different column set), so steady state allocates just the cell array.
    /// </summary>
    private TypedRow? CaptureTyped(string table, IReadOnlyDictionary<string, object?>? image)
    {
        if (image is null)
        {
            return null;
        }

        _schemas ??= new Dictionary<string, ReactiveRowSchema>(StringComparer.OrdinalIgnoreCase);
        if (!_schemas.TryGetValue(table, out ReactiveRowSchema? schema) || schema.ColumnCount != image.Count)
        {
            schema = SchemaOf(image);
            _schemas[table] = schema;
        }

        var cells = new CellValue[schema.ColumnCount];
        if (!TryFill(schema, image, cells))
        {
            schema = SchemaOf(image);
            _schemas[table] = schema;
            TryFill(schema, image, cells);
        }

        return TypedRow.FromOwnedCells(schema, cells);
    }

    private static bool TryFill(ReactiveRowSchema schema, IReadOnlyDictionary<string, object?> image, CellValue[] cells)
    {
        for (int i = 0; i < cells.Length; i++)
        {
            if (!image.TryGetValue(schema.ColumnAt(i), out object? value))
            {
                return false;
            }

            cells[i] = CellValue.FromBorrowedObject(value is DataVoBoundValue bound ? bound.ToMaterializedValue() : value);
        }

        return true;
    }

    private static ReactiveRowSchema SchemaOf(IReadOnlyDictionary<string, object?> image)
    {
        var columns = new string[image.Count];
        int i = 0;
        foreach (string column in image.Keys)
        {
            columns[i++] = column;
        }

        return new ReactiveRowSchema(columns);
    }

    private static IReadOnlyDictionary<string, object?>? CloneOrNull(IReadOnlyDictionary<string, object?>? row)
    {
        if (row is null)
        {
            return null;
        }

        // Snapshot the row so later mutation of the underlying storage dictionary cannot leak into the
        // captured before/after images. Single allocation (no intermediate ToDictionary copy).
        var clone = new Dictionary<string, object?>(row.Count, StringComparer.OrdinalIgnoreCase);
        foreach (KeyValuePair<string, object?> pair in row)
        {
            clone[pair.Key] = pair.Value is DataVoBoundValue boundValue
                ? boundValue.ToMaterializedValue()
                : pair.Value;
        }

        return clone;
    }
}
