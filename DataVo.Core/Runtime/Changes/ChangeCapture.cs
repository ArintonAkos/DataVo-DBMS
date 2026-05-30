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
