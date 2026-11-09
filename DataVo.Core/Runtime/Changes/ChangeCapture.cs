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
    /// Raised as each row change is recorded, before its commit is durable, so a subscriber can log the change
    /// first. The statement may still fail or roll back, so a subscriber must treat this as intent only;
    /// <see cref="Captured"/> reports what actually committed. Runs synchronously on the writing thread.
    /// </summary>
    /// <example>
    /// <code>
    /// engine.Changes.Enabled = true;
    /// engine.Changes.Recording += (database, change) => log.Append(database, change);
    /// </code>
    /// </example>
    public event Action<string, RowChange>? Recording;

    /// <summary>Reports a row change that is about to be committed.</summary>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="change">The recorded change.</param>
    internal void PublishRecording(string databaseName, RowChange change) => Recording?.Invoke(databaseName, change);

    /// <summary>
    /// Returns the next monotonically increasing sequence identifier.
    /// </summary>
    public long NextSequenceId() => System.Threading.Interlocked.Increment(ref _sequence);

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
