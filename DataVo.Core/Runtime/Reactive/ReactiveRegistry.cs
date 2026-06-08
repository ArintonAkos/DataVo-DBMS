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

        string databaseName = ResolveDatabase(ctx);
        IReactiveQuery subscription = CreateQuery(sql, databaseName);

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
    /// <remarks>
    /// The buffer is snapshotted and cleared under the lock before any callback runs, so writes a
    /// callback performs are enqueued for the <b>next</b> drain rather than re-entering this one (no
    /// recursive dispatch, no infinite loop). Only committed change sets are ever in the buffer, which is
    /// what makes every delivered delta transaction-safe. A subscription disposed mid-drain is skipped.
    /// </remarks>
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

                IReadOnlyCollection<string> tables = registration.Subscription.Tables;

                // Cheap pre-filter on the change set's small distinct table list before scanning rows.
                if (!set.Tables.Any(table => Observes(tables, table)))
                {
                    continue;
                }

                List<RowChange> relevantChanges = set.Changes
                    .Where(change => Observes(tables, change.Table))
                    .ToList();

                if (relevantChanges.Count == 0)
                {
                    continue;
                }

                QueryChange result = registration.Subscription.Apply(relevantChanges);
                if (!result.IsEmpty)
                {
                    registration.Callback(result);
                }
            }
        }
    }

    private static bool Observes(IReadOnlyCollection<string> tables, string table) =>
        tables.Any(observed => observed.Equals(table, StringComparison.OrdinalIgnoreCase));

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

    /// <summary>
    /// Compiles the supplied SQL into the appropriate reactive query operator by parsed shape.
    /// </summary>
    /// <param name="sql">The single-table reactive <c>SELECT</c> to compile.</param>
    /// <param name="databaseName">The database that owns the query's source table.</param>
    /// <returns>The compiled <see cref="IReactiveQuery"/>.</returns>
    /// <remarks>
    /// A query with <c>GROUP BY</c> or a top-level aggregate routes to <see cref="AggregateReactiveQuery"/>;
    /// an <c>ORDER BY … LIMIT</c> query routes to <see cref="TopKReactiveQuery"/>; everything else routes
    /// to the linear <see cref="ReactiveSubscription"/>. Unsupported shapes (joins, subqueries, and so on)
    /// raise <see cref="NotSupportedException"/> from the chosen operator's constructor.
    /// </remarks>
    private IReactiveQuery CreateQuery(string sql, string databaseName)
    {
        // A WITH RECURSIVE CTE is detected at the SQL level: the engine's primary parser does not accept
        // the RECURSIVE keyword nor a UNION ALL CTE body, so RecursiveCteParser slices and validates it.
        if (RecursiveCteParser.LooksRecursive(sql))
        {
            return new RecursiveCteReactiveQuery(RecursiveCteParser.Parse(sql), _engine, databaseName);
        }

        Parser.AST.SqlStatement statement = ReactiveQueryParser.ParseSingleStatement(sql);
        return CreateQuery(statement, databaseName, sql);
    }

    /// <summary>
    /// Routes a parsed statement to the operator for its shape, in precedence order: UNION → join →
    /// IN/EXISTS subquery → aggregate → top-K → DISTINCT → linear filter. The first matching shape wins;
    /// an unsupported construct surfaces as <see cref="NotSupportedException"/> from the chosen operator.
    /// </summary>
    /// <param name="statement">The parsed SELECT-family statement.</param>
    /// <param name="databaseName">The database that owns the query's source tables.</param>
    /// <param name="sql">The original SQL, threaded through only so the linear fallback can report it; <c>null</c> for recursively-compiled UNION branches.</param>
    private IReactiveQuery CreateQuery(Parser.AST.SqlStatement statement, string databaseName, string? sql = null)
    {
        if (statement is Parser.AST.UnionSelectStatement union)
        {
            return new UnionReactiveQuery(union, branch => CreateQuery(branch, databaseName));
        }

        if (statement is not Parser.AST.SelectStatement select)
        {
            throw new NotSupportedException("Reactive subscriptions support only SELECT statements.");
        }

        if (ReactiveQueryParser.TryGetJoinShape(select, out JoinShape joinShape))
        {
            return new JoinReactiveQuery(joinShape, _engine, databaseName);
        }

        if (ReactiveQueryParser.TryGetSubqueryShape(select, out SubqueryShape subqueryShape))
        {
            return new SubqueryReactiveQuery(subqueryShape, _engine, databaseName);
        }

        if (VipExposureReactiveQuery.IsSupported(select))
        {
            return new VipExposureReactiveQuery();
        }

        if (ReactiveQueryParser.IsAggregateShape(select))
        {
            return new AggregateReactiveQuery(select, _engine, databaseName);
        }

        if (ReactiveQueryParser.IsTopKShape(select))
        {
            return new TopKReactiveQuery(select, _engine, databaseName);
        }

        if (ReactiveQueryParser.IsDistinctShape(select))
        {
            return new DistinctReactiveQuery(select);
        }

        return sql is null ? new ReactiveSubscription(select) : new ReactiveSubscription(sql);
    }

    private void SeedSubscription(IReactiveQuery subscription, string databaseName)
    {
        using IDisposable _ = DataVoEngine.PushCurrent(_engine);

        foreach (string table in subscription.Tables)
        {
            Dictionary<long, Dictionary<string, object?>> rows =
                _engine.StorageContext.GetTableContents(table, databaseName);

            subscription.Seed(table, rows.Select(pair =>
                (pair.Key, (IReadOnlyDictionary<string, object?>)pair.Value)));
        }
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

    private sealed class Registration(IReactiveQuery subscription, Action<QueryChange> callback)
    {
        public IReactiveQuery Subscription { get; } = subscription;
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
