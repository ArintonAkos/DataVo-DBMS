using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using DataVo.Core;
using Microsoft.Extensions.VectorData;

namespace DataVo.VectorData;

/// <summary>
/// A <see cref="VectorStoreCollection{TKey, TRecord}"/> stored in one DataVo table. The record's
/// <see cref="VectorStoreKeyAttribute"/> property is the primary key (<see cref="int"/>, <see cref="long"/>,
/// <see cref="Guid"/> or <see cref="string"/>), each <see cref="VectorStoreDataAttribute"/> property a column (B-tree
/// indexed when <c>IsIndexed</c>), and each <see cref="VectorStoreVectorAttribute"/> property a <c>VECTOR(n)</c>
/// column with an <c>HNSW</c> or <c>FLAT</c> index for its distance function. A
/// <see cref="VectorStoreCollectionDefinition"/>, when given, replaces the attributes.
/// </summary>
/// <remarks>
/// Constructing the collection directly is Native AOT and trim safe: the record type's public properties and
/// parameterless constructor are preserved through <see cref="DynamicallyAccessedMembersAttribute"/>. Filters are
/// translated to SQL without compiling expressions.
/// </remarks>
/// <typeparam name="TKey">The key type: <see cref="int"/>, <see cref="long"/>, <see cref="Guid"/> or <see cref="string"/>.</typeparam>
/// <typeparam name="TRecord">The record type.</typeparam>
/// <example>
/// <code>
/// var store = new DataVoVectorStore(context);
/// var docs = new DataVoVectorStoreCollection&lt;int, Doc&gt;(store, "docs");
/// await docs.EnsureCollectionExistsAsync();
/// await docs.UpsertAsync(new Doc { Id = 1, Text = "hello", Embedding = vector });
/// await foreach (var hit in docs.SearchAsync(queryVector, top: 3)) { }
/// </code>
/// </example>
public sealed class DataVoVectorStoreCollection<TKey, [DynamicallyAccessedMembers(DataVoVectorStoreCollection.RecordMembers)] TRecord>
    : VectorStoreCollection<TKey, TRecord>
    where TKey : notnull
    where TRecord : class
{
    private readonly DataVoCollectionEngine<TRecord> _engine;

    /// <summary>Creates a collection named <paramref name="name"/> in <paramref name="store"/>.</summary>
    /// <param name="store">The vector store (its DataVo context and database).</param>
    /// <param name="name">The collection (table) name: letters, digits and underscores.</param>
    /// <param name="definition">Optional schema replacing the record type's attributes.</param>
    public DataVoVectorStoreCollection(DataVoVectorStore store, string name, VectorStoreCollectionDefinition? definition = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        DataVoVectorStoreCollection.RequireKeyType(typeof(TKey));
        DataVoCollectionModel model = DataVoCollectionModel.Build(typeof(TRecord), definition, store.Options.EmbeddingGenerator);
        if (model.Key.Type != typeof(TKey))
        {
            throw new ArgumentException($"{typeof(TRecord).Name}'s key '{model.Key.Name}' is a {model.Key.Type.Name}, not a {typeof(TKey).Name}.");
        }

        _engine = new DataVoCollectionEngine<TRecord>(store.Pool, name, model, new TypedRecordMapper());
    }

    /// <inheritdoc />
    public override string Name => _engine.Name;

    /// <inheritdoc />
    public override Task<bool> CollectionExistsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_engine.CollectionExists());
    }

    /// <inheritdoc />
    public override Task EnsureCollectionExistsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _engine.EnsureCollectionExists();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override Task EnsureCollectionDeletedAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _engine.EnsureCollectionDeleted();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override async Task<TRecord?> GetAsync(TKey key, RecordRetrievalOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();
        await foreach (TRecord record in _engine.GetAsync([key], options?.IncludeVectors ?? false, cancellationToken).ConfigureAwait(false))
        {
            return record;
        }

        return null;
    }

    /// <inheritdoc />
    public override IAsyncEnumerable<TRecord> GetAsync(IEnumerable<TKey> keys, RecordRetrievalOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        return _engine.GetAsync([.. keys.Select(static key => (object)key)], options?.IncludeVectors ?? false, cancellationToken);
    }

    /// <inheritdoc />
    public override IAsyncEnumerable<TRecord> GetAsync(
        Expression<Func<TRecord, bool>> filter,
        int top,
        FilteredRecordRetrievalOptions<TRecord>? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return DataVoVectorStoreCollection.ToAsync(_engine.GetFiltered(filter, top, options), cancellationToken);
    }

    /// <inheritdoc />
    public override Task DeleteAsync(TKey key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();
        _engine.Delete([key]);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override Task DeleteAsync(IEnumerable<TKey> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        cancellationToken.ThrowIfCancellationRequested();
        _engine.Delete([.. keys.Select(static key => (object)key)]);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override Task UpsertAsync(TRecord record, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(record);
        cancellationToken.ThrowIfCancellationRequested();
        return _engine.UpsertAsync([record], cancellationToken);
    }

    /// <inheritdoc />
    public override Task UpsertAsync(IEnumerable<TRecord> records, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(records);
        cancellationToken.ThrowIfCancellationRequested();
        return _engine.UpsertAsync([.. records], cancellationToken);
    }

    /// <inheritdoc />
    public override IAsyncEnumerable<VectorSearchResult<TRecord>> SearchAsync<TInput>(
        TInput searchValue,
        int top,
        VectorSearchOptions<TRecord>? options = null,
        CancellationToken cancellationToken = default) =>
        _engine.SearchAsync(searchValue, top, options, cancellationToken);

    /// <inheritdoc />
    public override object? GetService(Type serviceType, object? serviceKey = null)
    {
        ArgumentNullException.ThrowIfNull(serviceType);
        return serviceKey is not null ? null
            : serviceType == typeof(VectorStoreCollectionMetadata) ? _engine.Metadata
            : serviceType == typeof(DataVoContext) ? _engine.Context
            : serviceType.IsInstanceOfType(this) ? this
            : _engine.FindEmbeddingGenerator(serviceType);
    }

    /// <summary>Reads and writes record properties through their <see cref="System.Reflection.PropertyInfo"/>.</summary>
    private sealed class TypedRecordMapper : IDataVoRecordMapper<TRecord>
    {
        public object? GetValue(TRecord record, DataVoPropertyModel property) => property.Property!.GetValue(record);

        public void SetValue(TRecord record, DataVoPropertyModel property, object? value)
        {
            if (value is null && property.Type.IsValueType && Nullable.GetUnderlyingType(property.Type) is null)
            {
                return; // leave the default of a non-nullable value property
            }

            property.Property!.SetValue(record, value);
        }

        public TRecord CreateRecord() => Activator.CreateInstance<TRecord>();
    }
}

/// <summary>Shared helpers of the DataVo collections.</summary>
public static class DataVoVectorStoreCollection
{
    /// <summary>The record members the collections read through reflection: public properties and the parameterless constructor.</summary>
    public const DynamicallyAccessedMemberTypes RecordMembers =
        DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor;

    internal static void RequireKeyType(Type keyType)
    {
        if (keyType != typeof(int) && keyType != typeof(long) && keyType != typeof(Guid) && keyType != typeof(string) && keyType != typeof(object))
        {
            throw new NotSupportedException($"DataVo collections key records by int, long, Guid or string, not {keyType.Name}.");
        }
    }

    internal static async IAsyncEnumerable<T> ToAsync<T>(IEnumerable<T> items, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (T item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }
}
