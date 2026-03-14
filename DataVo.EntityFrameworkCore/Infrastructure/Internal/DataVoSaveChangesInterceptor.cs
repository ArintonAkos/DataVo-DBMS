using DataVo.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using System.Runtime.CompilerServices;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

/// <summary>
/// EF Core interceptor that automatically writes EF tracked changes to DataVo
/// whenever <c>DbContext.SaveChanges()</c> or <c>SaveChangesAsync()</c> is called.
///
/// Requires <c>UseDataVo(connectionString)</c> (or the typed fluent overload) in the
/// context options.  If DataVo is not configured the interceptor is a no-op.
///
/// The interceptor does NOT override EF's own persistence step (e.g. InMemory), so
/// query-side state in the current provider remains in sync with DataVo.
/// </summary>
internal sealed class DataVoSaveChangesInterceptor : SaveChangesInterceptor
{
    private static readonly ConditionalWeakTable<DbContext, SchemaState> _schemaStateByContext = new();

    // ------------------------------------------------------------------ sync

    public override InterceptionResult<int> SavingChanges(
        DbContextEventData eventData,
        InterceptionResult<int> result)
    {
        if (eventData.Context is { } context)
        {
            ExecuteOnDataVo(context);
        }

        return result; // let EF (e.g. InMemory) also complete its save
    }

    // ------------------------------------------------------------------ async

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        if (eventData.Context is { } context)
        {
            ExecuteOnDataVo(context);
        }

        return ValueTask.FromResult(result);
    }

    // ------------------------------------------------------------------ core

    private static void ExecuteOnDataVo(DbContext context)
    {
        if (DataVoBridgeExecutionContext.SuppressDataVoWrites)
        {
            return;
        }

        // Only act when UseDataVo(...) has been configured.
        var options = context.GetService<IDbContextOptions>();
        var extension = options.FindExtension<DataVoOptionsExtension>();
        string? connectionString = extension?.BuildEffectiveConnectionString();

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return; // DataVo not configured — nothing to do
        }

        EnsureSchemaCreatedIfNeeded(context, connectionString);

        // Execute CRUD without touching the change-tracker state; EF AcceptsAllChanges
        // after the full SaveChanges pipeline completes.
        DataVoChangeTrackerExtensions.ExecuteDataVoCrud(context, connectionString);
    }

    private static void EnsureSchemaCreatedIfNeeded(DbContext context, string connectionString)
    {
        // DataVoDbContext already performs its own first-save schema guard.
        if (context is DataVoDbContext)
        {
            return;
        }

        var state = _schemaStateByContext.GetOrCreateValue(context);
        if (state.SchemaEnsured)
        {
            return;
        }

        context.EnsureDataVoCreated(connectionString);
        state.SchemaEnsured = true;
    }

    private sealed class SchemaState
    {
        public bool SchemaEnsured { get; set; }
    }
}
