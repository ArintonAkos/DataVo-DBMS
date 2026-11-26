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
    private Registration[] _subscriptionsSnapshot = [];
    private List<ChangeSet> _incomingBuffer = new(16);
    private List<ChangeSet>? _spareBuffer = new(16);
    private bool _hooked;
    private bool _enabledCapture;

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
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(ctx);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(onChanged);

        string databaseName = ResolveDatabase(ctx);
        IReactiveQuery subscription = CreateQuery(sql, databaseName);

        Registration registration = RegisterUnderLock(new Registration(subscription, onChanged));

        SeedSubscription(subscription, databaseName);
        return new SubscriptionHandle(this, registration);
    }

    /// <summary>
    /// Registers a new reactive subscription from a pre-parsed <see cref="DataVoPreparedStatement"/>
    /// with bound parameters, bypassing SQL text parsing while preserving AST isolation across
    /// concurrent subscriptions sharing the same prepared statement.
    /// </summary>
    /// <param name="ctx">The context whose session resolves the active database.</param>
    /// <param name="statement">The prepared <c>SELECT</c> statement to observe.</param>
    /// <param name="onChanged">The callback invoked with non-empty query changes on each drain.</param>
    /// <param name="parameters">The parameter values bound into a cloned AST for this subscription.</param>
    /// <returns>A disposable handle that unregisters the subscription.</returns>
    public IDisposable Add(
        DataVoContext ctx,
        DataVoPreparedStatement statement,
        Action<QueryChange> onChanged,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(ctx);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(statement);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(onChanged);

        (IReactiveQuery subscription, string databaseName) = CompilePreparedQuery(ctx, statement, parameters);
        Registration registration = RegisterUnderLock(new Registration(subscription, onChanged));

        SeedSubscription(subscription, databaseName);
        return new SubscriptionHandle(this, registration);
    }

    /// <summary>
    /// Registers a zero-allocation reactive subscription from a pre-parsed <see cref="DataVoPreparedStatement"/>
    /// with bound parameters, delivering borrowed <see cref="QueryChangeRef"/> deltas without SQL re-parsing.
    /// </summary>
    /// <param name="ctx">The context whose session resolves the active database.</param>
    /// <param name="statement">The prepared <c>SELECT</c> statement to observe.</param>
    /// <param name="onChanged">The borrowed-delta callback invoked on each drain.</param>
    /// <param name="parameters">The parameter values bound into a cloned AST for this subscription.</param>
    /// <returns>A disposable handle that unregisters the subscription.</returns>
    public IDisposable SubscribeZeroAlloc(
        DataVoContext ctx,
        DataVoPreparedStatement statement,
        QueryDeltaHandler onChanged,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(ctx);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(statement);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(onChanged);

        (IReactiveQuery subscription, string databaseName) = CompilePreparedQuery(ctx, statement, parameters);
        return RegisterBorrowed(subscription, databaseName, onChanged);
    }

    /// <summary>
    /// Registers a pre-built direct-dispatch reactive operator (such as a zero-allocation buffered window),
    /// seeds it from current table contents, and returns a handle whose disposal removes it.
    /// </summary>
    internal IDisposable AddDirect(DataVoContext ctx, IDirectReactiveQuery query)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(ctx);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(query);

        string databaseName = ResolveDatabase(ctx);
        var registration = new Registration(query, new List<RowChange>());
        RegisterUnderLock(registration);

        SeedSubscription(query, databaseName);
        return new SubscriptionHandle(this, registration);
    }

    /// <summary>
    /// Registers a pre-built reactive operator delivering owned <see cref="QueryChange"/> notifications,
    /// seeds it, and returns a handle whose disposal removes it.
    /// </summary>
    internal IDisposable AddCompiled(DataVoContext ctx, IReactiveQuery query, Action<QueryChange> onChanged)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(ctx);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(query);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(onChanged);

        string databaseName = ResolveDatabase(ctx);
        Registration registration = RegisterUnderLock(new Registration(query, onChanged));

        SeedSubscription(query, databaseName);
        return new SubscriptionHandle(this, registration);
    }

    /// <summary>
    /// Test-only seam: registers a pre-built reactive operator (bypassing SQL compilation), seeds it,
    /// and returns a disposing handle. Exists to exercise the borrowed-to-materialized delta bridge
    /// end-to-end through the same <see cref="Dispatch"/> path as production subscriptions, without
    /// migrating real operators (Phase 1).
    /// </summary>
    internal IDisposable AddCompiledForTest(DataVoContext ctx, IReactiveQuery query, Action<QueryChange> onChanged)
        => AddCompiled(ctx, query, onChanged);

    /// <summary>
    /// Registers a zero-allocation subscription that delivers borrowed <see cref="QueryChangeRef"/>
    /// deltas (no materialization) for query shapes whose operator implements
    /// <see cref="IBorrowedReactiveQuery"/>. Throws <see cref="NotSupportedException"/> — with no side
    /// effects — for shapes that have not been migrated to borrowed emit.
    /// </summary>
    public IDisposable SubscribeZeroAlloc(DataVoContext ctx, string sql, QueryDeltaHandler onChanged)
    {
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(ctx);
        DataVo.Core.Compat.ThrowHelper.ThrowIfNull(onChanged);

        string databaseName = ResolveDatabase(ctx);
        IReactiveQuery subscription = CreateQuery(sql, databaseName);
        return RegisterBorrowed(subscription, databaseName, onChanged);
    }

    private (IReactiveQuery Query, string DatabaseName) CompilePreparedQuery(
        DataVoContext ctx,
        DataVoPreparedStatement statement,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        if (!ReferenceEquals(statement.Engine, _engine))
        {
            throw new ArgumentException(
                "Prepared statement belongs to a different DataVoContext.",
                nameof(statement));
        }

        string databaseName = ResolveDatabase(ctx);
        Parser.AST.SqlStatement boundStatement = statement.BindSingleReactiveStatement(parameters);
        Parser.Binding.SqlNameResolver.Resolve(boundStatement, _engine, databaseName);
        IReactiveQuery subscription = CreateQuery(boundStatement, databaseName);
        return (subscription, databaseName);
    }

    private IDisposable RegisterBorrowed(
        IReactiveQuery subscription,
        string databaseName,
        QueryDeltaHandler onChanged)
    {
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
            _subscriptionsSnapshot = _subscriptions.ToArray();
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
        List<ChangeSet> pending;
        Registration[] snapshot;

        lock (_gate)
        {
            if (_incomingBuffer.Count == 0 || _subscriptions.Count == 0)
            {
                _incomingBuffer.Clear();
                return;
            }

            pending = _incomingBuffer;
            _incomingBuffer = _spareBuffer ?? new List<ChangeSet>(16);
            _spareBuffer = null;
            snapshot = _subscriptionsSnapshot;
        }

        try
        {
            for (int setIndex = 0; setIndex < pending.Count; setIndex++)
            {
                ChangeSet set = pending[setIndex];
                for (int subIndex = 0; subIndex < snapshot.Length; subIndex++)
                {
                    Registration registration = snapshot[subIndex];
                    if (registration.Disposed)
                    {
                        continue;
                    }

                    if (registration.DirectQuery is not null)
                    {
                        DispatchDirect(registration, set);
                    }
                    else if (registration.IsBorrowed)
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
        finally
        {
            pending.Clear();
            lock (_gate)
            {
                _spareBuffer ??= pending;
            }
        }
    }

    // Direct zero-alloc path — filters relevant changes into reused scratch and delegates directly.
    private static void DispatchDirect(Registration registration, ChangeSet set)
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

        registration.DirectQuery!.ApplyAndNotify(scratch);
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

        _enabledCapture = !_engine.Changes.Enabled;
        _engine.Changes.Enabled = true;
        _engine.Changes.Captured += Enqueue;
        _hooked = true;
    }

    /// <summary>
    /// Detaches from change capture once the last subscription is gone, so a long-lived engine stops
    /// buffering change sets nobody will drain. Capture is switched back off only when this registry turned
    /// it on and no other subscriber still listens. Caller holds <c>_gate</c>.
    /// </summary>
    private void UnhookNoLock()
    {
        if (!_hooked)
        {
            return;
        }

        _engine.Changes.Captured -= Enqueue;
        _hooked = false;
        _incomingBuffer.Clear();
        if (_enabledCapture && !_engine.Changes.HasInterestedSubscribers(origin: null))
        {
            _engine.Changes.Enabled = false;
        }

        _enabledCapture = false;
    }

    private void Enqueue(ChangeSet set)
    {
        lock (_gate)
        {
            // A change set published while the last subscription was being removed has no reader.
            if (_subscriptions.Count > 0)
            {
                _incomingBuffer.Add(set);
            }
        }
    }

    private void Remove(Registration registration)
    {
        lock (_gate)
        {
            registration.Disposed = true;
            _subscriptions.Remove(registration);
            _subscriptionsSnapshot = _subscriptions.ToArray();
            if (_subscriptions.Count == 0)
            {
                UnhookNoLock();
            }
        }
    }

    /// <summary>Gets the number of buffered, undrained change sets (test diagnostics).</summary>
    internal int PendingChangeSetCount
    {
        get
        {
            lock (_gate)
            {
                return _incomingBuffer.Count;
            }
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
            RecursiveCteShape shape = RecursiveCteParser.Parse(sql);
            shape = shape with
            {
                BaseTable = _engine.Catalog.FindTableName(shape.BaseTable, databaseName) ?? shape.BaseTable,
                RecursiveBaseTable = _engine.Catalog.FindTableName(shape.RecursiveBaseTable, databaseName) ?? shape.RecursiveBaseTable,
            };
            return new RecursiveCteReactiveQuery(shape, _engine, databaseName);
        }

        Parser.AST.SqlStatement statement = ReactiveQueryParser.ParseSingleStatement(sql);
        Parser.Binding.SqlNameResolver.Resolve(statement, _engine, databaseName);
        return CreateQuery(statement, databaseName);
    }

    /// <summary>
    /// Routes a parsed statement to the operator for its shape, in precedence order: UNION → join →
    /// IN/EXISTS subquery → aggregate → top-K → DISTINCT → linear filter. The first matching shape wins;
    /// an unsupported construct surfaces as <see cref="NotSupportedException"/> from the chosen operator.
    /// </summary>
    /// <param name="statement">The parsed SELECT-family statement.</param>
    /// <param name="databaseName">The database that owns the query's source tables.</param>
    private IReactiveQuery CreateQuery(Parser.AST.SqlStatement statement, string databaseName)
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

        return new ReactiveSubscription(select);
    }

    private void SeedSubscription(IReactiveQuery subscription, string databaseName)
    {
        using IDisposable _ = DataVoEngine.PushCurrent(_engine);

        foreach (string table in subscription.Tables)
        {
            Dictionary<RowKey, StoredRow> rows =
                _engine.StorageContext.GetTypedTableContents(table, databaseName);

            subscription.Seed(table, rows.Select(pair =>
                (pair.Key, (IReadOnlyDictionary<string, object?>)pair.Value.AsDictionary())));
        }
    }

    /// <summary>
    /// Resolves the session's database for a new subscription and enforces read access, since a
    /// subscription seeds from and then streams table contents.
    /// </summary>
    private static string ResolveDatabase(DataVoContext ctx)
    {
        ctx.Engine.AuthorizeDirectAccess(ctx.SessionId, Security.DatabasePermission.ReadData);
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

        // Direct (zero-allocation buffered window) registration.
        public Registration(IDirectReactiveQuery directQuery, List<RowChange> scratch)
        {
            Subscription = directQuery;
            DirectQuery = directQuery;
            Scratch = scratch;
            ObservedTables = ToArray(directQuery.Tables);
        }

        public IReactiveQuery Subscription { get; }
        public IDirectReactiveQuery? DirectQuery { get; }
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
