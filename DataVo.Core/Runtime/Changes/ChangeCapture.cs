namespace DataVo.Core.Runtime.Changes;

/// <summary>
/// Engine-owned service that captures committed <see cref="ChangeSet"/>s and forwards them to subscribers.
/// </summary>
/// <remarks>
/// Capture is opt-in: it is disabled by default and auto-enabled when the first reactive subscription
/// registers. When disabled, the commit path performs no extra work or allocation. All publishing runs
/// synchronously on the committing thread; the reactive registry buffers change sets for later pull-drain.
/// </remarks>
public sealed class ChangeCapture
{
    private long _sequence;

    /// <summary>
    /// Gets or sets a value indicating whether change capture is active.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Raised for each published <see cref="ChangeSet"/> when capture is enabled.
    /// </summary>
    public event Action<ChangeSet>? Captured;

    /// <summary>
    /// Raised for each row change before any of it reaches storage or a log, in every storage mode, so a
    /// subscriber can log the change first. The statement may still fail, so a subscriber must treat this as
    /// intent only; <see cref="Captured"/> reports what actually committed. An insert has no storage key yet,
    /// so its <see cref="RowChange.RowId"/> is the default value. Runs synchronously on the writing thread.
    /// </summary>
    /// <example>
    /// <code>
    /// engine.Changes.Enabled = true;
    /// engine.Changes.Recording += (database, change) => log.Append(database, change);
    /// </code>
    /// </example>
    public event Action<string, RowChange>? Recording;

    /// <summary>Gets whether anything is listening to <see cref="Recording"/>, so callers can skip building row images.</summary>
    internal bool HasRecordingSubscribers => Recording is not null;

    /// <summary>Reports a row change that is about to be written.</summary>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="change">The recorded change.</param>
    internal void PublishRecording(string databaseName, RowChange change) => Recording?.Invoke(databaseName, change);

    /// <summary>
    /// Returns the next monotonically increasing sequence identifier.
    /// </summary>
    public long NextSequenceId() => System.Threading.Interlocked.Increment(ref _sequence);

    private static readonly System.Threading.AsyncLocal<string?> OriginSlot = new();

    /// <summary>
    /// Gets the write origin of the current flow, or <c>null</c> outside any <see cref="BeginOrigin"/> scope.
    /// </summary>
    /// <remarks>
    /// <see cref="Recording"/> handlers run on the writing thread, so they can read this to see where a change
    /// comes from. Committed change sets carry the same value as <see cref="ChangeSet.Origin"/>.
    /// </remarks>
    public static string? CurrentOrigin => OriginSlot.Value;

    /// <summary>
    /// Marks the writes of the current flow (the current thread and the asynchronous calls it makes) as coming
    /// from <paramref name="origin"/> until the returned scope is disposed.
    /// </summary>
    /// <remarks>
    /// The tag only labels changes: every subscriber (reactive queries included) still receives them, and
    /// SQL and storage behave exactly as without it. The origin in effect when a statement, batch or COMMIT
    /// runs is the one its change set carries. Scopes nest; disposing one restores the previous origin.
    /// </remarks>
    /// <param name="origin">A caller-chosen label, for example <c>"import"</c> or <c>"replica:primary"</c>.</param>
    /// <returns>A scope that ends the origin when disposed.</returns>
    /// <example>
    /// <code>
    /// using (ChangeCapture.BeginOrigin("import"))
    /// {
    ///     context.Execute("INSERT INTO Players VALUES (1, 'Ann')");
    /// }
    /// // Subscribers see ChangeSet.Origin == "import" for that insert.
    /// </code>
    /// </example>
    public static OriginScope BeginOrigin(string origin)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(origin);
        string? previous = OriginSlot.Value;
        OriginSlot.Value = origin;
        return new OriginScope(previous);
    }

    /// <summary>Ends a write origin started by <see cref="BeginOrigin"/>.</summary>
    public readonly struct OriginScope : IDisposable
    {
        private readonly string? _previous;

        internal OriginScope(string? previous) => _previous = previous;

        /// <summary>Restores the origin that was in effect before the scope began.</summary>
        public void Dispose() => OriginSlot.Value = _previous;
    }

    /// <summary>
    /// Publishes a committed change set to subscribers when capture is enabled; otherwise does nothing.
    /// </summary>
    /// <param name="set">The change set produced by a successful commit.</param>
    public void Publish(ChangeSet set)
    {
        if (!Enabled) return;
        Captured?.Invoke(set);
    }
}
