using DataVo.Core.Runtime.Changes;
using DataVo.Core.StorageEngine;

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

        Registration registration = RegisterUnderLock(new Registration(subscription, onChanged));

        SeedSubscription(subscription, databaseName);
        return new SubscriptionHandle(this, registration);
    }

    /// <summary>
    /// Test-only seam: registers a pre-built reactive operator (bypassing SQL compilation), seeds it,
    /// and returns a disposing handle. Exists to exercise the borrowed-to-materialized delta bridge
    /// end-to-end through the same <see cref="Dispatch"/> path as production subscriptions, without
    /// migrating real operators (Phase 1).
    /// </summary>
    internal IDisposable AddCompiledForTest(DataVoContext ctx, IReactiveQuery query, Action<QueryChange> onChanged)
    {
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(onChanged);

        string databaseName = ResolveDatabase(ctx);

        Registration registration = RegisterUnderLock(new Registration(query, onChanged));

        SeedSubscription(query, databaseName);
        return new SubscriptionHandle(this, registration);
    }

    /// <summary>
    /// Registers a zero-allocation subscription that delivers borrowed <see cref="QueryChangeRef"/>
    /// deltas (no materialization) for query shapes whose operator implements
    /// <see cref="IBorrowedReactiveQuery"/>. Throws <see cref="NotSupportedException"/> — with no side
    /// effects — for shapes that have not been migrated to borrowed emit.
    /// </summary>
    public IDisposable SubscribeZeroAlloc(DataVoContext ctx, string sql, QueryDeltaHandler onChanged)
    {
        ArgumentNullException.ThrowIfNull(ctx);
        ArgumentNullException.ThrowIfNull(onChanged);

        string databaseName = ResolveDatabase(ctx);
        IReactiveQuery subscription = CreateQuery(sql, databaseName);

        // Support check BEFORE any side effect (no registration, no hook enable, no seed on throw).
        if (subscription is not IBorrowedReactiveQuery borrowed)
        {
            throw new NotSupportedException(
                "Zero-allocation subscriptions are not yet supported for this query shape.");
        }

        var registration = new Registration(
            borrowed,
            onChanged,
            new QueryChangeBuilder(borrowed.OutputSchema),
            new List<RowChange>());

        RegisterUnderLock(registration);
        SeedSubscription(subscription, databaseName);
        return new SubscriptionHandle(this, registration);
    }

    /// <summary>
    /// Registers a pre-built registration under the subscription lock: enforces the cap, ensures the
    /// change hook is attached, and appends it. Shared by every registration path so the cap/hook policy
    /// lives in one place.
    /// </summary>
    private Registration RegisterUnderLock(Registration registration)
    {
        lock (_gate)
        {
            if (_subscriptions.Count >= MaxSubscriptions)
            {
                throw new InvalidOperationException(
                    $"Reactive subscription cap of {MaxSubscriptions} reached.");
            }

            EnsureHookedNoLock();
            _subscriptions.Add(registration);
            return registration;
        }
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

                if (registration.IsBorrowed)
                {
                    DispatchBorrowed(registration, set);
                }
                else
                {
                    DispatchOwned(registration, set);
                }
            }
        }
    }

    // Owned path — behavior unchanged (extracted verbatim from the previous inline loop body).
    private static void DispatchOwned(Registration registration, ChangeSet set)
    {
        IReadOnlyCollection<string> tables = registration.Subscription.Tables;

        if (!set.Tables.Any(table => Observes(tables, table)))
        {
            return;
        }

        List<RowChange> relevantChanges = set.Changes
            .Where(change => Observes(tables, change.Table))
            .ToList();

        if (relevantChanges.Count == 0)
        {
            return;
        }

        QueryChange result = registration.Subscription.Apply(relevantChanges);
        if (!result.IsEmpty)
        {
            registration.Callback!(result);
        }
    }

    // Borrowed (zero-alloc) path — manual loops, no LINQ/closures/enumerators; reused scratch list.
    private static void DispatchBorrowed(Registration registration, ChangeSet set)
    {
        string[] observed = registration.ObservedTables;

        if (!ObservesAny(set.Tables, observed))
        {
            return;
        }

        List<RowChange> scratch = registration.Scratch!;
        scratch.Clear();
        IReadOnlyList<RowChange> changes = set.Changes;
        for (int i = 0; i < changes.Count; i++)
        {
            RowChange change = changes[i];
            if (ObservesTable(observed, change.Table))
            {
                scratch.Add(change);
            }
        }

        if (scratch.Count == 0)
        {
            return;
        }

        QueryChangeBuilder builder = registration.Builder!;
        builder.Reset();
        ((IBorrowedReactiveQuery)registration.Subscription).ApplyInto(scratch, builder);
        QueryChangeRef delta = builder.Build();
        if (!delta.IsEmpty)
        {
            registration.BorrowedHandler!(in delta);
        }
    }

    private static bool ObservesAny(IReadOnlyList<string> setTables, string[] observed)
    {
        for (int i = 0; i < setTables.Count; i++)
        {
            if (ObservesTable(observed, setTables[i]))
            {
                return true;
            }
        }

        return false;
    }

    private static bool ObservesTable(string[] observed, string table)
    {
        for (int i = 0; i < observed.Length; i++)
        {
            if (observed[i].Equals(table, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
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
            Dictionary<long, StoredRow> rows =
                _engine.StorageContext.GetTypedTableContents(table, databaseName);

            subscription.Seed(table, rows.Select(pair =>
                (pair.Key, (IReadOnlyDictionary<string, object?>)pair.Value.AsDictionary())));
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

    private sealed class Registration
    {
        // Owned (materialized) registration.
        public Registration(IReactiveQuery subscription, Action<QueryChange> callback)
        {
            Subscription = subscription;
            Callback = callback;
            ObservedTables = ToArray(subscription.Tables);
        }

        // Borrowed (zero-allocation) registration.
        public Registration(
            IBorrowedReactiveQuery subscription,
            QueryDeltaHandler borrowedHandler,
            QueryChangeBuilder builder,
            List<RowChange> scratch)
        {
            Subscription = subscription;
            BorrowedHandler = borrowedHandler;
            Builder = builder;
            Scratch = scratch;
            ObservedTables = ToArray(subscription.Tables);
        }

        public IReactiveQuery Subscription { get; }
        public Action<QueryChange>? Callback { get; }
        public QueryDeltaHandler? BorrowedHandler { get; }
        public QueryChangeBuilder? Builder { get; }
        public List<RowChange>? Scratch { get; }
        public string[] ObservedTables { get; }
        public bool Disposed { get; set; }
        public bool IsBorrowed => BorrowedHandler is not null;

        private static string[] ToArray(IReadOnlyCollection<string> tables)
        {
            var array = new string[tables.Count];
            int i = 0;
            foreach (string table in tables)
            {
                array[i++] = table;
            }

            return array;
        }
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
