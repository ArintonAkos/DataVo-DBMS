using System.Linq.Expressions;
using Microsoft.Extensions.VectorData;

namespace DataVo.VectorData;

/// <summary>
/// A collection of dynamic records (<see cref="Dictionary{TKey, TValue}"/> of property name to value) whose schema
/// comes from a <see cref="VectorStoreCollectionDefinition"/>, stored like <see cref="DataVoVectorStoreCollection{TKey, TRecord}"/>.
/// Needs no reflection, so it is Native AOT and trim safe. Filters index the record by name: <c>r =&gt; (string)r["Category"]! == "docs"</c>.
/// </summary>
public sealed class DataVoDynamicVectorStoreCollection : VectorStoreCollection<object, Dictionary<string, object?>>
{
    private readonly DataVoCollectionEngine<Dictionary<string, object?>> _engine;

    /// <summary>Creates a dynamic collection named <paramref name="name"/> in <paramref name="store"/>.</summary>
    public DataVoDynamicVectorStoreCollection(DataVoVectorStore store, string name, VectorStoreCollectionDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(definition);
        _engine = new DataVoCollectionEngine<Dictionary<string, object?>>(
            store.Pool,
            name,
            DataVoCollectionModel.BuildDynamic(definition, store.Options.EmbeddingGenerator),
            new DictionaryRecordMapper());
    }

    /// <inheritdoc />
    public override string Name => _engine.Name;

    /// <inheritdoc />
    public override Task<bool> CollectionExistsAsync(CancellationToken cancellationToken = default) => Task.FromResult(_engine.CollectionExists());

    /// <inheritdoc />
    public override Task EnsureCollectionExistsAsync(CancellationToken cancellationToken = default)
    {
        _engine.EnsureCollectionExists();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override Task EnsureCollectionDeletedAsync(CancellationToken cancellationToken = default)
    {
        _engine.EnsureCollectionDeleted();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override async Task<Dictionary<string, object?>?> GetAsync(object key, RecordRetrievalOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        await foreach (Dictionary<string, object?> record in _engine.GetAsync([key], options?.IncludeVectors ?? false, cancellationToken).ConfigureAwait(false))
        {
            return record;
        }

        return null;
    }

    /// <inheritdoc />
    public override IAsyncEnumerable<Dictionary<string, object?>> GetAsync(IEnumerable<object> keys, RecordRetrievalOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        return _engine.GetAsync([.. keys], options?.IncludeVectors ?? false, cancellationToken);
    }

    /// <inheritdoc />
    public override IAsyncEnumerable<Dictionary<string, object?>> GetAsync(
        Expression<Func<Dictionary<string, object?>, bool>> filter,
        int top,
        FilteredRecordRetrievalOptions<Dictionary<string, object?>>? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return DataVoVectorStoreCollection.ToAsync(_engine.GetFiltered(filter, top, options), cancellationToken);
    }

    /// <inheritdoc />
    public override Task DeleteAsync(object key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        _engine.Delete([key]);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override Task DeleteAsync(IEnumerable<object> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        _engine.Delete([.. keys]);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override Task UpsertAsync(Dictionary<string, object?> record, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        return _engine.UpsertAsync([record], cancellationToken);
    }

    /// <inheritdoc />
    public override Task UpsertAsync(IEnumerable<Dictionary<string, object?>> records, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(records);
        return _engine.UpsertAsync([.. records], cancellationToken);
    }

    /// <inheritdoc />
    public override IAsyncEnumerable<VectorSearchResult<Dictionary<string, object?>>> SearchAsync<TInput>(
        TInput searchValue,
        int top,
        VectorSearchOptions<Dictionary<string, object?>>? options = null,
        CancellationToken cancellationToken = default) =>
        _engine.SearchAsync(searchValue, top, options, cancellationToken);

    /// <inheritdoc />
    public override object? GetService(Type serviceType, object? serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(serviceType);
        return serviceKey is not null ? null
            : serviceType == typeof(VectorStoreCollectionMetadata) ? _engine.Metadata
            : serviceType.IsInstanceOfType(this) ? this
            : null;
    }

    /// <summary>Reads and writes dictionary entries keyed by property name.</summary>
    private sealed class DictionaryRecordMapper : IDataVoRecordMapper<Dictionary<string, object?>>
    {
        public object? GetValue(Dictionary<string, object?> record, DataVoPropertyModel property) =>
            record.TryGetValue(property.Name, out object? value) ? value : null;

        public void SetValue(Dictionary<string, object?> record, DataVoPropertyModel property, object? value) => record[property.Name] = value;

        public Dictionary<string, object?> CreateRecord() => new(StringComparer.Ordinal);
    }
}
