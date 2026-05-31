using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Engine-owned registry of active reactive subscriptions implementing the pull-drain delivery model.
/// </summary>
/// <remarks>
/// Committed <see cref="ChangeSet"/>s are buffered as they are captured and only fed through the
/// subscriptions when <see cref="Dispatch"/> is called on the consumer's thread. Writes performed
/// inside a callback are captured into a fresh buffer and surface on the next drain, never recursively.
/// </remarks>
public sealed class ReactiveRegistry
{
    private readonly DataVoEngine _engine;
    private readonly object _gate = new();
    private readonly List<Registration> _subscriptions = [];
    private readonly Queue<ChangeSet> _buffer = new();
    private bool _hooked;

    /// <summary>
    /// Initializes a new registry bound to the supplied engine.
    /// </summary>
    /// <param name="engine">The owning engine.</param>
    public ReactiveRegistry(DataVoEngine engine)
    {
        _engine = engine;
    }

    /// <summary>
    /// Gets or sets the maximum number of concurrently active subscriptions.
    /// </summary>
    public int MaxSubscriptions { get; set; } = 256;

    /// <summary>
    /// Registers a new reactive subscription for the supplied SQL, seeds it from current table contents,
    /// and returns a handle whose disposal removes it.
    /// </summary>
    /// <param name="ctx">The context whose session resolves the active database.</param>
    /// <param name="sql">The single-table linear <c>SELECT … WHERE</c> to subscribe to.</param>
    /// <param name="onChanged">The callback invoked with non-empty query changes on each drain.</param>
    /// <returns>A disposable handle that unregisters the subscription.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the subscription cap is exceeded.</exception>
    public IDisposable Add(DataVoContext ctx, string sql, Action<QueryChange> onChanged)
    {
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(onChanged);

        var subscription = new ReactiveSubscription(sql);
        string databaseName = ResolveDatabase(ctx);

        Registration registration;
        lock (_gate)
        {
            if (_subscriptions.Count >= MaxSubscriptions)
            {
                throw new InvalidOperationException(
                    $"Reactive subscription cap of {MaxSubscriptions} reached.");
            }

            EnsureHookedNoLock();
            registration = new Registration(subscription, onChanged);
            _subscriptions.Add(registration);
        }

        SeedSubscription(subscription, databaseName);
        return new SubscriptionHandle(this, registration);
    }

    /// <summary>
    /// Drains all buffered change sets through the active subscriptions, invoking callbacks for any
    /// non-empty results on the calling thread.
    /// </summary>
    public void Dispatch()
    {
        ChangeSet[] pending;
        Registration[] snapshot;

        lock (_gate)
        {
            if (_buffer.Count == 0 || _subscriptions.Count == 0)
            {
                _buffer.Clear();
                return;
            }

            pending = _buffer.ToArray();
            _buffer.Clear();
            snapshot = _subscriptions.ToArray();
        }

        foreach (ChangeSet set in pending)
        {
            foreach (Registration registration in snapshot)
            {
                if (registration.Disposed)
                {
                    continue;
                }

                if (!set.Tables.Any(table => table.Equals(registration.Subscription.Table, StringComparison.OrdinalIgnoreCase)))
                {
                    continue;
                }

                List<RowChange> tableChanges = set.Changes
                    .Where(change => change.Table.Equals(registration.Subscription.Table, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                if (tableChanges.Count == 0)
                {
                    continue;
                }

                QueryChange result = registration.Subscription.Apply(tableChanges);
                if (!result.IsEmpty)
                {
                    registration.Callback(result);
                }
            }
        }
    }

    private void EnsureHookedNoLock()
    {
        if (_hooked)
        {
            return;
        }

        _engine.Changes.Enabled = true;
        _engine.Changes.Captured += Enqueue;
        _hooked = true;
    }

    private void Enqueue(ChangeSet set)
    {
        lock (_gate)
        {
            _buffer.Enqueue(set);
        }
    }

    private void Remove(Registration registration)
    {
        lock (_gate)
        {
            registration.Disposed = true;
            _subscriptions.Remove(registration);
        }
    }

    private void SeedSubscription(ReactiveSubscription subscription, string databaseName)
    {
        using IDisposable _ = DataVoEngine.PushCurrent(_engine);

        Dictionary<long, Dictionary<string, object?>> rows =
            _engine.StorageContext.GetTableContents(subscription.Table, databaseName);

        subscription.Seed(rows.Select(pair =>
            (pair.Key, (IReadOnlyDictionary<string, object?>)pair.Value)));
    }

    private static string ResolveDatabase(DataVoContext ctx)
    {
        string? databaseName = ctx.Engine.Sessions.Get(ctx.SessionId);
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new InvalidOperationException(
                "No database selected for the current session. Execute USE <database> before subscribing.");
        }

        return databaseName;
    }

    private sealed class Registration(ReactiveSubscription subscription, Action<QueryChange> callback)
    {
        public ReactiveSubscription Subscription { get; } = subscription;
        public Action<QueryChange> Callback { get; } = callback;
        public bool Disposed { get; set; }
    }

    private sealed class SubscriptionHandle(ReactiveRegistry registry, Registration registration) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            registry.Remove(registration);
        }
    }
}
