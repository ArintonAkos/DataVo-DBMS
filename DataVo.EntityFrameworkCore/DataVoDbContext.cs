using DataVo.Data;
using DataVo.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Linq.Expressions;

namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Base class for EF Core contexts that target DataVo for persistence.
///
/// <para><b>Quick-start pattern:</b></para>
/// <code>
/// public class MyDbContext : DataVoDbContext
/// {
///     public DbSet&lt;Product&gt; Products { get; set; }
///
///     public MyDbContext(DbContextOptions&lt;MyDbContext&gt; options) : base(options) { }
/// }
///
/// // At application startup:
/// using var ctx = new MyDbContext(options);       // configure with UseDataVo(...)
/// await ctx.Database.EnsureCreatedAsync();        // creates DataVo schema
/// ctx.LoadFromDataVo();                           // seeds EF from existing DataVo data
///
/// // Normal EF reads now work:
/// var products = ctx.Products.Where(p => p.IsActive).ToList();
///
/// // Writes go to both InMemory and DataVo automatically:
/// ctx.Add(new Product { ... });
/// ctx.SaveChanges();
/// </code>
///
/// <para><b>What this class adds over plain <see cref="DbContext"/>:</b></para>
/// <list type="bullet">
///   <item>
///     <see cref="LoadFromDataVo"/> — bulk-reads all DataVo tables into the EF change tracker
///     so LINQ queries return live DataVo data without a full EF provider.
///   </item>
///   <item>
///     Auto schema-guard — the first <see cref="SaveChanges(bool)"/> call per context instance
///     ensures the DataVo schema exists (idempotent), eliminating the need to call
///     <c>Database.EnsureCreated()</c> manually before every write.
///   </item>
///   <item>
///     <see cref="ExecuteSqlOnDataVo"/> — routes raw SQL to DataVo when
///     <c>Database.ExecuteSqlRaw</c> is not available (InMemory does not support it).
///   </item>
/// </list>
/// </summary>
public abstract class DataVoDbContext : DbContext
{
    private static readonly string[] SupportedGuardedOperators =
    [
        "Where",
        "OrderBy",
        "OrderByDescending",
        "ThenBy",
        "ThenByDescending",
        "Skip",
        "Take",
        "Include",
        "Select (simple projection)",
        "Any",
        "Count",
        "FirstOrDefault"
    ];

    private static readonly string[] BlockedGuardedOperators =
    [
        nameof(Queryable.GroupBy),
        nameof(Queryable.Join),
        nameof(Queryable.GroupJoin),
        nameof(Queryable.Union),
        nameof(Queryable.Intersect),
        nameof(Queryable.Except),
        nameof(Queryable.Zip)
    ];

    private bool _schemaEnsured;

    // ------------------------------------------------------------------ constructors

    /// <summary>
    /// Initialises a new DataVo EF bridge context.
    /// </summary>
    protected DataVoDbContext()
    {
    }

    /// <summary>
    /// Initialises a new DataVo EF bridge context using externally built options.
    /// </summary>
    protected DataVoDbContext(DbContextOptions options) : base(options)
    {
    }

    // ------------------------------------------------------------------ query bridge: LoadFromDataVo

    /// <summary>
    /// Reads all mapped DataVo tables and attaches the rows as <see cref="EntityState.Unchanged"/>
    /// entries into the current context.
    ///
    /// <para>Call this after context construction to make LINQ queries reflect DataVo data.
    /// Any previously tracked entities are detached first, so the change tracker is a clean
    /// mirror of what DataVo contains.</para>
    ///
    /// <para>
    /// You do NOT need to call this if you only write data in the current context session —
    /// the interceptor keeps InMemory and DataVo in sync during <see cref="SaveChanges"/>.
    /// You DO need to call it when data was written by another process, a previous context
    /// session, or external tooling.
    /// </para>
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <c>UseDataVo(…)</c> is not configured on this context.
    /// </exception>
    /// <exception cref="DataVoEfException">
    /// Thrown when the DataVo storage cannot be read or entity materialisation fails.
    /// </exception>
    public void LoadFromDataVo()
    {
        string connectionString = DataVoDbContextExtensions.ResolveConfiguredConnectionString(this);
        DataVoEntityMaterializer.LoadIntoContext(this, connectionString);
    }

    /// <summary>
    /// Calls <c>Database.EnsureCreated()</c> (creates the DataVo schema) and then
    /// <see cref="LoadFromDataVo"/> in one step, returning <c>true</c> when the schema was
    /// freshly created, <c>false</c> when it already existed.
    /// </summary>
    public bool EnsureCreatedAndLoad()
    {
        bool created = Database.EnsureCreated();
        LoadFromDataVo();
        return created;
    }

    /// <summary>
    /// Returns the active DataVo provider mode for this context instance.
    /// </summary>
    public DataVoProviderModeStatus GetActiveProviderModeStatus()
    {
        return this.GetService<DataVoProviderModeResolver>().Resolve();
    }

    /// <summary>
    /// Returns the current guarded query capability profile for this bridge layer.
    /// </summary>
    public static DataVoGuardedQueryCapabilities GetGuardedQueryCapabilities()
    {
        return new DataVoGuardedQueryCapabilities(
            SupportedOperators: SupportedGuardedOperators,
            BlockedOperators: BlockedGuardedOperators,
            Guidance: "Guarded queries refresh EF state from DataVo first. Complex set-shaping operators are explicitly blocked until provider-native translation is implemented.");
    }

    /// <summary>
    /// Returns metadata/capability parity checks for provider-identity preview readiness.
    /// </summary>
    public DataVoProviderIdentityParityReport GetProviderIdentityParityReport()
    {
        var modeStatus = GetActiveProviderModeStatus();
        var entityTypes = Model.GetEntityTypes().ToList();

        int queryableEntities = entityTypes.Count(static entityType =>
            !entityType.IsOwned() &&
            entityType.ClrType is { IsAbstract: false } &&
            entityType.FindPrimaryKey() is not null &&
            entityType.GetTableName() is not null);

        var warnings = new List<string>();
        foreach (IEntityType entityType in entityTypes.Where(static entityType => !entityType.IsOwned() && entityType.ClrType is { IsAbstract: false }))
        {
            if (entityType.GetTableName() is null)
            {
                warnings.Add($"Entity '{entityType.DisplayName()}' is not mapped to a table.");
            }

            if (entityType.FindPrimaryKey() is null)
            {
                warnings.Add($"Entity '{entityType.DisplayName()}' has no primary key.");
            }
        }

        return new DataVoProviderIdentityParityReport(
            ModeStatus: modeStatus,
            HostProviderName: Database.ProviderName,
            HostProviderConfigured: !string.IsNullOrWhiteSpace(Database.ProviderName),
            NativePreviewOperators: DataVoQueryTranslationAnalyzer.GetNativePreviewOperators(),
            BlockedOperators: DataVoQueryTranslationAnalyzer.GetBlockedOperators(),
            MappedEntityTypeCount: entityTypes.Count,
            QueryableEntityTypeCount: queryableEntities,
            MetadataWarnings: warnings);
    }

    /// <summary>
    /// Performs a non-executing preflight check for guarded LINQ query-shape support.
    /// </summary>
    public bool CanExecuteGuardedQuery<TEntity>(
        Func<IQueryable<TEntity>, IQueryable<TEntity>> queryShape,
        out string? reason,
        bool asNoTracking = true)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(queryShape);

        try
        {
            var diagnostics = ExplainQueryFromDataVo(queryShape, asNoTracking);

            if (diagnostics.IsBlocked)
            {
                reason = diagnostics.Summary;
                return false;
            }

            reason = null;
            return true;
        }
        catch (DataVoEfException ex) when (ex.Operation == DataVoEfOperation.Query)
        {
            reason = ex.Message;
            return false;
        }
        catch (Exception ex)
        {
            reason = ex.Message;
            return false;
        }
    }

    /// <summary>
    /// Returns structured diagnostics describing whether the provided shape will execute through
    /// native translation preview, guarded fallback, or be blocked.
    /// </summary>
    public DataVoQueryTranslationDiagnostics ExplainQueryFromDataVo<TEntity>(
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? queryShape = null,
        bool asNoTracking = true)
        where TEntity : class
    {
        IQueryable<TEntity> query = ComposeQuery(queryShape, asNoTracking);
        return DataVoQueryTranslationAnalyzer.Analyze(query.Expression, GetActiveProviderModeStatus());
    }

    /// <summary>
    /// Returns structured diagnostics for projection queries executed via <see cref="ProjectFromDataVo{TEntity, TDto}"/>.
    /// </summary>
    public DataVoQueryTranslationDiagnostics ExplainProjectFromDataVo<TEntity, TDto>(
        Expression<Func<TEntity, TDto>> selector,
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? queryShape = null,
        bool asNoTracking = true)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(selector);

        IQueryable<TEntity> query = ComposeQuery(queryShape, asNoTracking);
        IQueryable<TDto> projected = query.Select(selector);
        return DataVoQueryTranslationAnalyzer.Analyze(projected.Expression, GetActiveProviderModeStatus());
    }

    /// <summary>
    /// Guarded query entry-point that refreshes EF's in-memory mirror from DataVo and then
    /// executes the provided LINQ shape against the refreshed set.
    /// </summary>
    /// <typeparam name="TEntity">Entity type to query.</typeparam>
    /// <param name="queryShape">
    /// Optional LINQ composition function (e.g. <c>q =&gt; q.Where(...)</c>,
    /// <c>q =&gt; q.Include(...).OrderBy(...)</c>).
    /// </param>
    /// <param name="asNoTracking">
    /// When <c>true</c> (default), applies <c>AsNoTracking()</c> before <paramref name="queryShape"/>.
    /// </param>
    /// <returns>Materialised query results.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <c>UseDataVo(…)</c> is not configured.
    /// </exception>
    /// <exception cref="DataVoEfException">
    /// Thrown with <see cref="DataVoEfOperation.Query"/> when guarded query execution fails.
    /// </exception>
    public List<TEntity> QueryFromDataVo<TEntity>(
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? queryShape = null,
        bool asNoTracking = true)
        where TEntity : class
    {
        try
        {
            IQueryable<TEntity> query = ComposeQuery(queryShape, asNoTracking);
            var diagnostics = DataVoQueryTranslationAnalyzer.Analyze(query.Expression, GetActiveProviderModeStatus());
            EnsureQueryIsNotBlocked(diagnostics);

            if (diagnostics.WillUseNativeTranslationPreview)
            {
                return DataVoNativeQueryExecutor.ExecuteEntityQuery<TEntity>(this, query.Expression);
            }

            LoadFromDataVo();
            return query.ToList();
        }
        catch (DataVoEfException)
        {
            throw;
        }
        catch (InvalidOperationException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DataVoEfException(
                DataVoEfOperation.Query,
                $"Guarded DataVo LINQ query failed for entity '{typeof(TEntity).Name}'.",
                ex);
        }
    }

    /// <summary>
    /// Async version of <see cref="QueryFromDataVo{TEntity}(Func{IQueryable{TEntity}, IQueryable{TEntity}}?, bool)"/>.
    /// </summary>
    public async Task<List<TEntity>> QueryFromDataVoAsync<TEntity>(
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? queryShape = null,
        bool asNoTracking = true,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        try
        {
            IQueryable<TEntity> query = ComposeQuery(queryShape, asNoTracking);
            var diagnostics = DataVoQueryTranslationAnalyzer.Analyze(query.Expression, GetActiveProviderModeStatus());
            EnsureQueryIsNotBlocked(diagnostics);

            if (diagnostics.WillUseNativeTranslationPreview)
            {
                return await Task.FromResult(DataVoNativeQueryExecutor.ExecuteEntityQuery<TEntity>(this, query.Expression));
            }

            LoadFromDataVo();
            return await query.ToListAsync(cancellationToken);
        }
        catch (DataVoEfException)
        {
            throw;
        }
        catch (InvalidOperationException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DataVoEfException(
                DataVoEfOperation.Query,
                $"Guarded DataVo LINQ query failed for entity '{typeof(TEntity).Name}'.",
                ex);
        }
    }

    /// <summary>
    /// Returns whether any row exists after refreshing from DataVo and applying an optional predicate.
    /// </summary>
    public bool AnyFromDataVo<TEntity>(
        Expression<Func<TEntity, bool>>? predicate = null,
        bool asNoTracking = true)
        where TEntity : class
    {
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? shape = predicate is null
            ? null
            : query => query.Where(predicate);

        IQueryable<TEntity> query = CreateGuardedQuery(shape, asNoTracking);
        return query.Any();
    }

    /// <summary>
    /// Async version of <see cref="AnyFromDataVo{TEntity}(Expression{Func{TEntity, bool}}?, bool)"/>.
    /// </summary>
    public async Task<bool> AnyFromDataVoAsync<TEntity>(
        Expression<Func<TEntity, bool>>? predicate = null,
        bool asNoTracking = true,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? shape = predicate is null
            ? null
            : query => query.Where(predicate);

        IQueryable<TEntity> query = CreateGuardedQuery(shape, asNoTracking);
        return await query.AnyAsync(cancellationToken);
    }

    /// <summary>
    /// Returns count after refreshing from DataVo and applying an optional predicate.
    /// </summary>
    public int CountFromDataVo<TEntity>(
        Expression<Func<TEntity, bool>>? predicate = null,
        bool asNoTracking = true)
        where TEntity : class
    {
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? shape = predicate is null
            ? null
            : query => query.Where(predicate);

        IQueryable<TEntity> query = CreateGuardedQuery(shape, asNoTracking);
        return query.Count();
    }

    /// <summary>
    /// Async version of <see cref="CountFromDataVo{TEntity}(Expression{Func{TEntity, bool}}?, bool)"/>.
    /// </summary>
    public async Task<int> CountFromDataVoAsync<TEntity>(
        Expression<Func<TEntity, bool>>? predicate = null,
        bool asNoTracking = true,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? shape = predicate is null
            ? null
            : query => query.Where(predicate);

        IQueryable<TEntity> query = CreateGuardedQuery(shape, asNoTracking);
        return await query.CountAsync(cancellationToken);
    }

    /// <summary>
    /// Returns the first matching entity or <c>null</c> after refreshing from DataVo.
    /// </summary>
    public TEntity? FirstOrDefaultFromDataVo<TEntity>(
        Expression<Func<TEntity, bool>>? predicate = null,
        bool asNoTracking = true)
        where TEntity : class
    {
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? shape = predicate is null
            ? null
            : query => query.Where(predicate);

        IQueryable<TEntity> query = CreateGuardedQuery(shape, asNoTracking);
        return query.FirstOrDefault();
    }

    /// <summary>
    /// Async version of <see cref="FirstOrDefaultFromDataVo{TEntity}(Expression{Func{TEntity, bool}}?, bool)"/>.
    /// </summary>
    public async Task<TEntity?> FirstOrDefaultFromDataVoAsync<TEntity>(
        Expression<Func<TEntity, bool>>? predicate = null,
        bool asNoTracking = true,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? shape = predicate is null
            ? null
            : query => query.Where(predicate);

        IQueryable<TEntity> query = CreateGuardedQuery(shape, asNoTracking);
        return await query.FirstOrDefaultAsync(cancellationToken);
    }

    // ------------------------------------------------------------------ projection bridge

    /// <summary>
    /// Refreshes EF from DataVo, applies an optional LINQ shape, then projects every matching
    /// entity through <paramref name="selector"/> — returning lightweight DTOs without
    /// loading full tracked entities into the change tracker.
    /// </summary>
    /// <typeparam name="TEntity">Source entity type.</typeparam>
    /// <typeparam name="TDto">Projection result type (can be a record, struct, or anonymous-type equivalent).</typeparam>
    /// <param name="selector">Projection expression. Must not be <c>null</c>.</param>
    /// <param name="queryShape">
    /// Optional LINQ composition applied to the entity set <em>before</em> projection
    /// (e.g. <c>q =&gt; q.Where(...).OrderBy(...)</c>).
    /// Blocked operators (GroupBy, Join, etc.) are detected here and throw
    /// <see cref="DataVoEfException"/> with <see cref="DataVoEfOperation.Query"/>.
    /// </param>
    /// <param name="asNoTracking">When <c>true</c> (default), queries entities as no-tracking.</param>
    /// <returns>List of projected DTOs.</returns>
    public List<TDto> ProjectFromDataVo<TEntity, TDto>(
        Expression<Func<TEntity, TDto>> selector,
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? queryShape = null,
        bool asNoTracking = true)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(selector);

        IQueryable<TEntity> query = ComposeQuery(queryShape, asNoTracking);
        IQueryable<TDto> projected = query.Select(selector);
        var diagnostics = DataVoQueryTranslationAnalyzer.Analyze(projected.Expression, GetActiveProviderModeStatus());
        EnsureQueryIsNotBlocked(diagnostics);

        if (diagnostics.WillUseNativeTranslationPreview)
        {
            return DataVoNativeQueryExecutor.ExecuteProjectionQuery<TEntity, TDto>(this, projected.Expression);
        }

        query = CreateGuardedQuery(queryShape, asNoTracking);
        return query.Select(selector).ToList();
    }

    /// <summary>
    /// Async version of <see cref="ProjectFromDataVo{TEntity, TDto}"/>.
    /// </summary>
    public async Task<List<TDto>> ProjectFromDataVoAsync<TEntity, TDto>(
        Expression<Func<TEntity, TDto>> selector,
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? queryShape = null,
        bool asNoTracking = true,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(selector);

        IQueryable<TEntity> query = ComposeQuery(queryShape, asNoTracking);
        IQueryable<TDto> projected = query.Select(selector);
        var diagnostics = DataVoQueryTranslationAnalyzer.Analyze(projected.Expression, GetActiveProviderModeStatus());
        EnsureQueryIsNotBlocked(diagnostics);

        if (diagnostics.WillUseNativeTranslationPreview)
        {
            return await Task.FromResult(DataVoNativeQueryExecutor.ExecuteProjectionQuery<TEntity, TDto>(this, projected.Expression));
        }

        query = CreateGuardedQuery(queryShape, asNoTracking);
        return await query.Select(selector).ToListAsync(cancellationToken);
    }

    private IQueryable<TEntity> ComposeQuery<TEntity>(
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? queryShape,
        bool asNoTracking)
        where TEntity : class
    {
        IQueryable<TEntity> query = Set<TEntity>();
        if (asNoTracking)
        {
            query = query.AsNoTracking();
        }

        return queryShape is null ? query : queryShape(query);
    }

    private IQueryable<TEntity> CreateGuardedQuery<TEntity>(
        Func<IQueryable<TEntity>, IQueryable<TEntity>>? queryShape,
        bool asNoTracking)
        where TEntity : class
    {
        LoadFromDataVo();

        IQueryable<TEntity> query = Set<TEntity>();
        if (asNoTracking)
        {
            query = query.AsNoTracking();
        }

        query = queryShape is null ? query : queryShape(query);
        var diagnostics = DataVoQueryTranslationAnalyzer.Analyze(query.Expression, GetActiveProviderModeStatus());
        EnsureQueryIsNotBlocked(diagnostics);
        return query;
    }

    private static void EnsureQueryIsNotBlocked(DataVoQueryTranslationDiagnostics diagnostics)
    {
        if (!diagnostics.IsBlocked)
        {
            return;
        }

        throw new DataVoEfException(
            DataVoEfOperation.Query,
            diagnostics.Summary);
    }

    // ------------------------------------------------------------------ raw SQL bridge

    /// <summary>
    /// Executes a raw SQL statement directly against DataVo (bypassing EF's InMemory pipeline).
    ///
    /// <para>
    /// Use this instead of <c>context.Database.ExecuteSqlRaw</c>, which throws when the
    /// underlying EF provider is InMemory because InMemory does not support raw SQL.
    /// </para>
    /// </summary>
    /// <param name="sql">The SQL statement to execute.</param>
    /// <param name="parameters">Named parameters to bind.</param>
    /// <returns>Number of rows affected.</returns>
    public int ExecuteSqlOnDataVo(string sql, params (string Name, object? Value)[] parameters)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);

        string connectionString = DataVoDbContextExtensions.ResolveConfiguredConnectionString(this);

        try
        {
            using var connection = new DataVoConnection(connectionString);
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = sql;

            foreach (var (name, value) in parameters)
            {
                command.Parameters.Add(new DataVoParameter { ParameterName = name, Value = value ?? DBNull.Value });
            }

            int affected = command.ExecuteNonQuery();
            if (sql.TrimStart().StartsWith("INSERT", StringComparison.OrdinalIgnoreCase) && affected == 0)
            {
                throw new DataVoEfException(
                    DataVoEfOperation.RawSql,
                    $"DataVo INSERT affected 0 rows. SQL may be invalid or rejected: {sql}");
            }

            return affected;
        }
        catch (Exception ex) when (ex is not DataVoEfException)
        {
            throw new DataVoEfException(
                DataVoEfOperation.RawSql,
                $"DataVo raw SQL execution failed. SQL: {sql}",
                ex);
        }
    }

    // ------------------------------------------------------------------ SaveChanges override (schema guard + EnsureCreated)

    /// <inheritdoc />
    /// <remarks>
    /// The first time <c>SaveChanges</c> is called on this context instance, the DataVo schema
    /// is created if it does not already exist (idempotent).  Subsequent calls skip this check.
    /// The actual DataVo write is handled by the <see cref="Infrastructure.Internal.DataVoSaveChangesInterceptor"/>.
    /// </remarks>
    public override int SaveChanges(bool acceptAllChangesOnSuccess)
    {
        EnsureSchemaOnFirstSave();
        return base.SaveChanges(acceptAllChangesOnSuccess);
    }

    /// <inheritdoc />
    public override Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default)
    {
        EnsureSchemaOnFirstSave();
        return base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
    }

    // ------------------------------------------------------------------ helpers

    private void EnsureSchemaOnFirstSave()
    {
        if (_schemaEnsured)
        {
            return;
        }

        var options = this.GetService<IDbContextOptions>();
        var extension = options.FindExtension<Infrastructure.Internal.DataVoOptionsExtension>();
        string? connectionString = extension?.BuildEffectiveConnectionString();

        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            this.EnsureDataVoCreated(connectionString);
            _schemaEnsured = true;
        }
    }
}
