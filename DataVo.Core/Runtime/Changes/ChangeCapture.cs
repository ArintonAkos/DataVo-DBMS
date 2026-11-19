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
    private readonly object _subscriptionGate = new();
    private volatile FilteredSubscriber<Action<ChangeSet>>[] _capturedSubscribers = [];
    private volatile FilteredSubscriber<Action<string, RowChange>>[] _recordingSubscribers = [];
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

    /// <summary>
    /// Subscribes <paramref name="handler"/> to committed change sets, optionally skipping sets whose
    /// <see cref="ChangeSet.Origin"/> matches <paramref name="ignoreOrigin"/>.
    /// </summary>
    /// <remarks>
    /// When every active subscriber ignores the current <see cref="CurrentOrigin"/>, write paths skip building
    /// row change images altogether.
    /// </remarks>
    /// <param name="handler">The callback invoked for matching change sets.</param>
    /// <param name="ignoreOrigin">An optional write origin to skip, compared ordinally.</param>
    /// <returns>A handle that unsubscribes <paramref name="handler"/> when disposed.</returns>
    /// <example>
    /// <code>
    /// using IDisposable sub = engine.Changes.Subscribe(OnChange, ignoreOrigin: "import");
    /// </code>
    /// </example>
    public IDisposable Subscribe(Action<ChangeSet> handler, string? ignoreOrigin = null)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(handler);
        var entry = new FilteredSubscriber<Action<ChangeSet>>(handler, ignoreOrigin);
        lock (_subscriptionGate)
        {
            _capturedSubscribers = [.. _capturedSubscribers, entry];
        }

        return new SubscriptionHandle(() => RemoveCapturedSubscriber(entry));
    }

    /// <summary>
    /// Subscribes <paramref name="handler"/> to pre-commit recording notifications, optionally skipping writes
    /// whose <see cref="CurrentOrigin"/> matches <paramref name="ignoreOrigin"/>.
    /// </summary>
    /// <param name="handler">The callback invoked for matching row changes.</param>
    /// <param name="ignoreOrigin">An optional write origin to skip, compared ordinally.</param>
    /// <returns>A handle that unsubscribes <paramref name="handler"/> when disposed.</returns>
    public IDisposable SubscribeRecording(Action<string, RowChange> handler, string? ignoreOrigin = null)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(handler);
        var entry = new FilteredSubscriber<Action<string, RowChange>>(handler, ignoreOrigin);
        lock (_subscriptionGate)
        {
            _recordingSubscribers = [.. _recordingSubscribers, entry];
        }

        return new SubscriptionHandle(() => RemoveRecordingSubscriber(entry));
    }

    /// <summary>Gets whether anything is listening to <see cref="Recording"/> for <paramref name="origin"/>.</summary>
    internal bool HasInterestedRecordingSubscribers(string? origin)
    {
        if (Recording is not null)
        {
            return true;
        }

        foreach (FilteredSubscriber<Action<string, RowChange>> subscriber in _recordingSubscribers)
        {
            if (subscriber.Accepts(origin))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Gets whether any captured or recording subscriber is interested in writes from <paramref name="origin"/>.</summary>
    internal bool HasInterestedSubscribers(string? origin)
    {
        if (Captured is not null || Recording is not null)
        {
            return true;
        }

        foreach (FilteredSubscriber<Action<ChangeSet>> subscriber in _capturedSubscribers)
        {
            if (subscriber.Accepts(origin))
            {
                return true;
            }
        }

        foreach (FilteredSubscriber<Action<string, RowChange>> subscriber in _recordingSubscribers)
        {
            if (subscriber.Accepts(origin))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Reports a row change that is about to be written.</summary>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="change">The recorded change.</param>
    /// <param name="origin">The write origin of the current flow.</param>
    internal void PublishRecording(string databaseName, RowChange change, string? origin)
    {
        Recording?.Invoke(databaseName, change);
        foreach (FilteredSubscriber<Action<string, RowChange>> subscriber in _recordingSubscribers)
        {
            if (subscriber.Accepts(origin))
            {
                subscriber.Handler(databaseName, change);
            }
        }
    }

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
        foreach (FilteredSubscriber<Action<ChangeSet>> subscriber in _capturedSubscribers)
        {
            if (subscriber.Accepts(set.Origin))
            {
                subscriber.Handler(set);
            }
        }
    }

    private void RemoveCapturedSubscriber(FilteredSubscriber<Action<ChangeSet>> entry)
    {
        lock (_subscriptionGate)
        {
            _capturedSubscribers = RemoveFrom(_capturedSubscribers, entry);
        }
    }

    private void RemoveRecordingSubscriber(FilteredSubscriber<Action<string, RowChange>> entry)
    {
        lock (_subscriptionGate)
        {
            _recordingSubscribers = RemoveFrom(_recordingSubscribers, entry);
        }
    }

    private static FilteredSubscriber<THandler>[] RemoveFrom<THandler>(
        FilteredSubscriber<THandler>[] current,
        FilteredSubscriber<THandler> target)
    {
        int index = Array.IndexOf(current, target);
        if (index < 0)
        {
            return current;
        }

        if (current.Length == 1)
        {
            return [];
        }

        var next = new FilteredSubscriber<THandler>[current.Length - 1];
        Array.Copy(current, 0, next, 0, index);
        Array.Copy(current, index + 1, next, index, current.Length - index - 1);
        return next;
    }

    private sealed class FilteredSubscriber<THandler>(THandler handler, string? ignoreOrigin)
    {
        public THandler Handler { get; } = handler;

        public bool Accepts(string? origin) =>
            ignoreOrigin is null || !string.Equals(origin, ignoreOrigin, StringComparison.Ordinal);
    }

    private sealed class SubscriptionHandle(Action unsubscribe) : IDisposable
    {
        private Action? _unsubscribe = unsubscribe;

        public void Dispose() => System.Threading.Interlocked.Exchange(ref _unsubscribe, null)?.Invoke();
    }
}
