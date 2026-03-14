using DataVo.Data;
using DataVo.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

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
    private bool _schemaEnsured;

    // ------------------------------------------------------------------ constructors

    protected DataVoDbContext()
    {
    }

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
