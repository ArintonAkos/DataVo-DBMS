using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;

namespace DataVo.VectorData;

/// <summary>Options of a <see cref="DataVoVectorStore"/>.</summary>
public sealed class DataVoVectorStoreOptions
{
    /// <summary>The DataVo database that holds the store's collections (created when missing). Defaults to <c>VectorStore</c>.</summary>
    public string DatabaseName { get; set; } = "VectorStore";

    /// <summary>
    /// The embedding generator for vector properties declared as <see cref="string"/> (embedded on upsert) and for
    /// searches by text. A collection definition or vector property may set its own.
    /// </summary>
    public IEmbeddingGenerator? EmbeddingGenerator { get; set; }
}

/// <summary>
/// A <see cref="VectorStore"/> over an embedded DataVo database: each collection is a table with <c>VECTOR(n)</c>
/// columns and HNSW or FLAT indexes, in memory or persisted by the LSM engine, queried with DataVo SQL.
/// </summary>
/// <example>
/// <code>
/// using var store = new DataVoVectorStore(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = "./vectors" });
/// var docs = new DataVoVectorStoreCollection&lt;Guid, Doc&gt;(store, "docs");
/// await docs.EnsureCollectionExistsAsync();
/// </code>
/// </example>
public sealed class DataVoVectorStore : VectorStore
{
    /// <summary>The <see cref="VectorStoreMetadata.VectorStoreSystemName"/> of DataVo stores.</summary>
    public const string SystemName = "datavo";

    private readonly bool _ownsContext;

    /// <summary>Creates a store over an existing <paramref name="context"/>; the caller keeps ownership of the context.</summary>
    public DataVoVectorStore(DataVoContext context, DataVoVectorStoreOptions? options = null)
        : this(context, options, ownsContext: false)
    {
    }

    /// <summary>Creates a store over a new DataVo engine opened with <paramref name="config"/>; disposing the store closes it.</summary>
    public DataVoVectorStore(DataVoConfig config, DataVoVectorStoreOptions? options = null)
        : this(new DataVoContext(config ?? throw new ArgumentNullException(nameof(config))), options, ownsContext: true)
    {
    }

    private DataVoVectorStore(DataVoContext context, DataVoVectorStoreOptions? options, bool ownsContext)
    {
        ArgumentNullException.ThrowIfNull(context);
        Options = options ?? new DataVoVectorStoreOptions();
        DataVoSqlNames.ValidateIdentifier(Options.DatabaseName, "database");
        Context = context;
        _ownsContext = ownsContext;
        EnsureDatabase(context, Options.DatabaseName);
        Pool = new DataVoSessionPool(context, Options.DatabaseName);
    }

    /// <summary>The DataVo context the store runs on.</summary>
    public DataVoContext Context { get; }

    /// <summary>The store's options.</summary>
    public DataVoVectorStoreOptions Options { get; }

    internal DataVoSessionPool Pool { get; }

    /// <summary>
    /// Gets a typed collection. Prefer constructing <see cref="DataVoVectorStoreCollection{TKey, TRecord}"/> directly in
    /// trimmed or Native AOT apps: this override follows <see cref="VectorStore.GetCollection{TKey, TRecord}"/>, which is
    /// annotated as needing unreferenced code.
    /// </summary>
    [RequiresUnreferencedCode("Reflects over TRecord's properties; in trimmed or Native AOT apps construct DataVoVectorStoreCollection<TKey, TRecord> directly.")]
    [RequiresDynamicCode("Reflects over TRecord's properties; in trimmed or Native AOT apps construct DataVoVectorStoreCollection<TKey, TRecord> directly.")]
    public override VectorStoreCollection<TKey, TRecord> GetCollection<TKey, TRecord>(string name, VectorStoreCollectionDefinition? definition = null) =>
        new DataVoVectorStoreCollection<TKey, TRecord>(this, name, definition);

    /// <inheritdoc />
    public override VectorStoreCollection<object, Dictionary<string, object?>> GetDynamicCollection(string name, VectorStoreCollectionDefinition definition) =>
        new DataVoDynamicVectorStoreCollection(this, name, definition);

    /// <inheritdoc />
    public override async IAsyncEnumerable<string> ListCollectionNamesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (string table in Context.Engine.Catalog.GetTables(Options.DatabaseName))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return table;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override Task<bool> CollectionExistsAsync(string name, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(TableExists(Pool, name));
    }

    /// <inheritdoc />
    public override Task EnsureCollectionDeletedAsync(string name, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        DropTable(Pool, name);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override object? GetService(Type serviceType, object? serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(serviceType);
        return serviceKey is not null ? null
            : serviceType == typeof(VectorStoreMetadata) ? new VectorStoreMetadata { VectorStoreSystemName = SystemName, VectorStoreName = Options.DatabaseName }
            : serviceType == typeof(DataVoContext) ? Context
            : serviceType.IsInstanceOfType(this) ? this
            : Options.EmbeddingGenerator is { } generator && serviceType.IsInstanceOfType(generator) ? generator
            : null;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing && _ownsContext)
        {
            Context.Dispose();
        }

        base.Dispose(disposing);
    }

    internal static bool TableExists(DataVoSessionPool pool, string name) =>
        pool.Context.Engine.Catalog.TableExists(name, pool.DatabaseName);

    internal static void DropTable(DataVoSessionPool pool, string name)
    {
        DataVoSqlNames.ValidateCollectionName(name);
        if (!TableExists(pool, name))
        {
            return;
        }

        using DataVoSessionPool.Lease lease = pool.Rent();
        QueryResult result = lease.Execute($"DROP TABLE {name}");
        if (result.IsError)
        {
            throw new VectorStoreException($"DataVo could not drop collection '{name}': {string.Join(" ", result.Messages)}")
            {
                VectorStoreSystemName = SystemName,
                VectorStoreName = pool.DatabaseName,
                CollectionName = name,
                OperationName = "EnsureCollectionDeleted",
            };
        }

        pool.SchemaChanged();
    }

    private static void EnsureDatabase(DataVoContext context, string databaseName)
    {
        if (context.Engine.Catalog.DatabaseExists(databaseName))
        {
            return;
        }

        QueryResult result = context.Execute($"CREATE DATABASE {databaseName}", Guid.NewGuid())[^1];
        if (result.IsError && !context.Engine.Catalog.DatabaseExists(databaseName))
        {
            throw new VectorStoreException($"DataVo could not create database '{databaseName}': {string.Join(" ", result.Messages)}")
            {
                VectorStoreSystemName = SystemName,
                VectorStoreName = databaseName,
                OperationName = "CreateDatabase",
            };
        }
    }
}
