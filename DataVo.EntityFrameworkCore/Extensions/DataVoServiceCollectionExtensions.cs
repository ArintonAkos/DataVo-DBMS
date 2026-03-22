using DataVo.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Registers DataVo EF Core services.
/// </summary>
public static class DataVoServiceCollectionExtensions
{
    /// <summary>
    /// Adds DataVo EF Core services to the dependency injection container.
    /// </summary>
    /// <remarks>
    /// Called automatically from <see cref="DataVoOptionsExtension.ApplyServices"/> when
    /// <c>UseDataVo(...)</c> is configured on the context options.
    /// </remarks>
    public static IServiceCollection AddEntityFrameworkDataVo(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<DataVoProviderBootstrapMarker>();
        services.TryAddScoped<DataVoProviderModeResolver>();

        // Replace whatever IDatabaseCreator the base provider registered (e.g. InMemory's no-op
        // creator) so that Database.EnsureCreated() and Database.EnsureDeleted() route to DataVo.
        services.AddScoped<IDatabaseCreator, DataVoDatabaseCreator>();

        return services;
    }
}
