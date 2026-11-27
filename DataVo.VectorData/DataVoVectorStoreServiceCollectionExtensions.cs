using System.Diagnostics.CodeAnalysis;
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;
using DataVo.VectorData;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers DataVo vector stores and collections with dependency injection.</summary>
/// <example>
/// <code>
/// services.AddDataVoVectorStore(new DataVoConfig { StorageMode = StorageMode.Lsm, DataDirectory = "./vectors" });
/// services.AddDataVoCollection&lt;Guid, Doc&gt;("docs");
/// services.AddDataVoDynamicCollection("notes", notesDefinition);
/// // inject VectorStore, DataVoVectorStore, VectorStoreCollection&lt;Guid, Doc&gt; or IVectorSearchable&lt;Doc&gt;
/// </code>
/// </example>
public static class DataVoVectorStoreServiceCollectionExtensions
{
    /// <summary>
    /// Registers a singleton <see cref="DataVoVectorStore"/> (also as <see cref="VectorStore"/>) over a DataVo engine
    /// opened with <paramref name="config"/>. Without an embedding generator in <paramref name="options"/>, a registered
    /// <c>IEmbeddingGenerator&lt;string, Embedding&lt;float&gt;&gt;</c> is used when there is one.
    /// </summary>
    public static IServiceCollection AddDataVoVectorStore(this IServiceCollection services, DataVoConfig config, DataVoVectorStoreOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(config);
        return services.AddDataVoVectorStore(provider => new DataVoVectorStore(config, WithGenerator(provider, options)));
    }

    /// <summary>
    /// Registers a singleton <see cref="DataVoVectorStore"/> (also as <see cref="VectorStore"/>) over the
    /// <see cref="DataVoContext"/> registered in the container, which keeps ownership of it.
    /// </summary>
    public static IServiceCollection AddDataVoVectorStore(this IServiceCollection services, DataVoVectorStoreOptions? options = null) =>
        services.AddDataVoVectorStore(provider => new DataVoVectorStore(provider.GetRequiredService<DataVoContext>(), WithGenerator(provider, options)));

    /// <summary>Registers a singleton <see cref="DataVoVectorStore"/> built by <paramref name="factory"/> (also as <see cref="VectorStore"/>).</summary>
    public static IServiceCollection AddDataVoVectorStore(this IServiceCollection services, Func<IServiceProvider, DataVoVectorStore> factory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(factory);
        services.AddSingleton(factory);
        services.AddSingleton<VectorStore>(static provider => provider.GetRequiredService<DataVoVectorStore>());
        return services;
    }

    /// <summary>
    /// Registers a keyed singleton <see cref="DataVoVectorStore"/> (also as a keyed <see cref="VectorStore"/>) over a DataVo
    /// engine opened with <paramref name="config"/>.
    /// </summary>
    public static IServiceCollection AddKeyedDataVoVectorStore(this IServiceCollection services, object? serviceKey, DataVoConfig config, DataVoVectorStoreOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(config);
        services.AddKeyedSingleton(serviceKey, (provider, _) => new DataVoVectorStore(config, WithGenerator(provider, options)));
        services.AddKeyedSingleton<VectorStore>(serviceKey, static (provider, key) => provider.GetRequiredKeyedService<DataVoVectorStore>(key));
        return services;
    }

    /// <summary>
    /// Registers a singleton <see cref="DataVoVectorStoreCollection{TKey, TRecord}"/> named <paramref name="name"/> in the
    /// registered <see cref="DataVoVectorStore"/>, also as <see cref="VectorStoreCollection{TKey, TRecord}"/> and
    /// <see cref="IVectorSearchable{TRecord}"/>. Native AOT and trim safe.
    /// </summary>
    public static IServiceCollection AddDataVoCollection<TKey, [DynamicallyAccessedMembers(DataVoVectorStoreCollection.RecordMembers)] TRecord>(
        this IServiceCollection services,
        string name,
        VectorStoreCollectionDefinition? definition = null)
        where TKey : notnull
        where TRecord : class
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        services.AddSingleton(provider => new DataVoVectorStoreCollection<TKey, TRecord>(provider.GetRequiredService<DataVoVectorStore>(), name, definition));
        services.AddSingleton<VectorStoreCollection<TKey, TRecord>>(static provider => provider.GetRequiredService<DataVoVectorStoreCollection<TKey, TRecord>>());
        services.AddSingleton<IVectorSearchable<TRecord>>(static provider => provider.GetRequiredService<DataVoVectorStoreCollection<TKey, TRecord>>());
        return services;
    }

    /// <summary>
    /// Registers a keyed singleton <see cref="DataVoVectorStoreCollection{TKey, TRecord}"/> named <paramref name="name"/>
    /// under <paramref name="serviceKey"/>, also as a keyed <see cref="VectorStoreCollection{TKey, TRecord}"/> and
    /// <see cref="IVectorSearchable{TRecord}"/>. It lives in the <see cref="DataVoVectorStore"/> registered under
    /// <paramref name="storeKey"/> (a keyed store from <see cref="AddKeyedDataVoVectorStore"/>), or in the unkeyed store
    /// when <paramref name="storeKey"/> is null. Native AOT and trim safe.
    /// </summary>
    public static IServiceCollection AddKeyedDataVoCollection<TKey, [DynamicallyAccessedMembers(DataVoVectorStoreCollection.RecordMembers)] TRecord>(
        this IServiceCollection services,
        object? serviceKey,
        string name,
        VectorStoreCollectionDefinition? definition = null,
        object? storeKey = null)
        where TKey : notnull
        where TRecord : class
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        services.AddKeyedSingleton(serviceKey, (provider, _) => new DataVoVectorStoreCollection<TKey, TRecord>(ResolveStore(provider, storeKey), name, definition));
        services.AddKeyedSingleton<VectorStoreCollection<TKey, TRecord>>(serviceKey, static (provider, key) => provider.GetRequiredKeyedService<DataVoVectorStoreCollection<TKey, TRecord>>(key));
        services.AddKeyedSingleton<IVectorSearchable<TRecord>>(serviceKey, static (provider, key) => provider.GetRequiredKeyedService<DataVoVectorStoreCollection<TKey, TRecord>>(key));
        return services;
    }

    /// <summary>
    /// Registers a singleton <see cref="DataVoDynamicVectorStoreCollection"/> named <paramref name="name"/> with the schema
    /// <paramref name="definition"/> in the registered <see cref="DataVoVectorStore"/>, also as
    /// <c>VectorStoreCollection&lt;object, Dictionary&lt;string, object?&gt;&gt;</c> and
    /// <c>IVectorSearchable&lt;Dictionary&lt;string, object?&gt;&gt;</c>. Needs no reflection.
    /// </summary>
    public static IServiceCollection AddDataVoDynamicCollection(this IServiceCollection services, string name, VectorStoreCollectionDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(definition);
        services.AddSingleton(provider => new DataVoDynamicVectorStoreCollection(provider.GetRequiredService<DataVoVectorStore>(), name, definition));
        services.AddSingleton<VectorStoreCollection<object, Dictionary<string, object?>>>(static provider => provider.GetRequiredService<DataVoDynamicVectorStoreCollection>());
        services.AddSingleton<IVectorSearchable<Dictionary<string, object?>>>(static provider => provider.GetRequiredService<DataVoDynamicVectorStoreCollection>());
        return services;
    }

    /// <summary>
    /// Registers a keyed singleton <see cref="DataVoDynamicVectorStoreCollection"/> under <paramref name="serviceKey"/>,
    /// also as the keyed dynamic <see cref="VectorStoreCollection{TKey, TRecord}"/> and <see cref="IVectorSearchable{TRecord}"/>,
    /// in the store registered under <paramref name="storeKey"/> (or the unkeyed store when null).
    /// </summary>
    public static IServiceCollection AddKeyedDataVoDynamicCollection(
        this IServiceCollection services,
        object? serviceKey,
        string name,
        VectorStoreCollectionDefinition definition,
        object? storeKey = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(definition);
        services.AddKeyedSingleton(serviceKey, (provider, _) => new DataVoDynamicVectorStoreCollection(ResolveStore(provider, storeKey), name, definition));
        services.AddKeyedSingleton<VectorStoreCollection<object, Dictionary<string, object?>>>(serviceKey, static (provider, key) => provider.GetRequiredKeyedService<DataVoDynamicVectorStoreCollection>(key));
        services.AddKeyedSingleton<IVectorSearchable<Dictionary<string, object?>>>(serviceKey, static (provider, key) => provider.GetRequiredKeyedService<DataVoDynamicVectorStoreCollection>(key));
        return services;
    }

    private static DataVoVectorStore ResolveStore(IServiceProvider provider, object? storeKey) =>
        storeKey is null ? provider.GetRequiredService<DataVoVectorStore>() : provider.GetRequiredKeyedService<DataVoVectorStore>(storeKey);

    private static DataVoVectorStoreOptions WithGenerator(IServiceProvider provider, DataVoVectorStoreOptions? options)
    {
        var resolved = new DataVoVectorStoreOptions
        {
            DatabaseName = options?.DatabaseName ?? new DataVoVectorStoreOptions().DatabaseName,
            EmbeddingGenerator = options?.EmbeddingGenerator ?? provider.GetService<IEmbeddingGenerator<string, Embedding<float>>>(),
        };
        return resolved;
    }
}
