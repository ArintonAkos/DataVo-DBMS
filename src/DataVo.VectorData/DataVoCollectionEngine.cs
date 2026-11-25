using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.VectorData;

namespace DataVo.VectorData;

/// <summary>Reads, writes and creates records of one shape (typed objects or dictionaries) for the collection engine.</summary>
internal interface IDataVoRecordMapper<TRecord>
{
    object? GetValue(TRecord record, DataVoPropertyModel property);

    void SetValue(TRecord record, DataVoPropertyModel property, object? value);

    TRecord CreateRecord();
}

/// <summary>
/// The SQL behind a collection: one DataVo table with a key column, data columns and <c>VECTOR(n)</c> columns,
/// B-tree indexes on indexed data columns and an HNSW or FLAT index per vector column. Shared by the typed and the
/// dynamic collection; every operation runs on its own leased session, and writes of several records run in one
/// transaction.
/// </summary>
internal sealed class DataVoCollectionEngine<TRecord>
    where TRecord : class
{
    /// <summary>Largest number of keys bound into one <c>IN (...)</c> list.</summary>
    private const int KeyBatch = 256;
    private const string ScoreColumn = "__datavo_score";

    private readonly DataVoSessionPool _pool;
    private readonly IDataVoRecordMapper<TRecord> _mapper;
    private readonly string _selectAll;
    private readonly string _selectWithoutVectors;
    private readonly string _insert;
    private readonly string _deleteByKey;

    public DataVoCollectionEngine(DataVoSessionPool pool, string name, DataVoCollectionModel model, IDataVoRecordMapper<TRecord> mapper)
    {
        DataVoSqlNames.ValidateCollectionName(name);
        _pool = pool;
        Name = name;
        Model = model;
        _mapper = mapper;
        _selectAll = string.Join(", ", model.All.Select(property => property.Column));
        _selectWithoutVectors = string.Join(", ", model.All.Where(property => property.Role != DataVoPropertyRole.Vector).Select(property => property.Column));
        _insert = $"INSERT INTO {name} ({_selectAll}) VALUES ({string.Join(", ", model.All.Select((_, i) => "@v" + i.ToString(CultureInfo.InvariantCulture)))})";
        _deleteByKey = $"DELETE FROM {name} WHERE {model.Key.Column} = @k";
    }

    public string Name { get; }

    public DataVoCollectionModel Model { get; }

    public VectorStoreCollectionMetadata Metadata => new()
    {
        VectorStoreSystemName = DataVoVectorStore.SystemName,
        VectorStoreName = _pool.DatabaseName,
        CollectionName = Name,
    };

    public bool CollectionExists() => DataVoVectorStore.TableExists(_pool, Name);

    /// <summary>Creates the table and its indexes unless the table exists.</summary>
    public void EnsureCollectionExists()
    {
        if (CollectionExists())
        {
            return;
        }

        var columns = new List<string> { $"{Model.Key.Column} {Model.Key.SqlType} PRIMARY KEY" };
        columns.AddRange(Model.Data.Concat(Model.Vectors).Select(property => $"{property.Column} {property.SqlType}"));
        using DataVoSessionPool.Lease lease = _pool.Rent();
        Check(lease.Execute($"CREATE TABLE {Name} ({string.Join(", ", columns)})"), "EnsureCollectionExists");
        foreach (DataVoPropertyModel data in Model.Data.Where(property => property.IsIndexed))
        {
            Check(lease.Execute($"CREATE INDEX {IndexName(data)} ON {Name} ({data.Column}) USING BTREE"), "EnsureCollectionExists");
        }

        foreach (DataVoPropertyModel vector in Model.Vectors)
        {
            string method = vector.IndexKind == IndexKind.Flat ? "FLAT" : "HNSW";
            Check(lease.Execute($"CREATE INDEX {IndexName(vector)} ON {Name} ({vector.Column}) USING {method} WITH (metric = '{vector.IndexMetric}')"), "EnsureCollectionExists");
        }

        _pool.SchemaChanged();
    }

    public void EnsureCollectionDeleted() => DataVoVectorStore.DropTable(_pool, Name);

    /// <summary>Inserts or replaces <paramref name="records"/> in one transaction, generating embeddings and Guid keys first.</summary>
    public async Task UpsertAsync(IReadOnlyList<TRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0)
        {
            return;
        }

        Dictionary<DataVoPropertyModel, IList<Embedding<float>>>? generated = await GenerateEmbeddingsAsync(records, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();

        var rows = new DataVoQueryParameter[records.Count][];
        var keys = new object[records.Count];
        for (int r = 0; r < records.Count; r++)
        {
            TRecord record = records[r];
            object key = KeyOf(record, assign: true);
            keys[r] = key;
            var values = new DataVoQueryParameter[Model.All.Count];
            for (int i = 0; i < Model.All.Count; i++)
            {
                DataVoPropertyModel property = Model.All[i];
                object? value = property.VectorKind == DataVoVectorKind.Generated && property.Role == DataVoPropertyRole.Vector
                    ? GeneratedVector(generated!, property, r)
                    : i == 0 ? key : _mapper.GetValue(record, property);
                if (property.Role == DataVoPropertyRole.Vector && value is not null)
                {
                    value = CheckDimensions(property, value);
                }

                values[i] = DataVoQueryParameter.FromObject("v" + i.ToString(CultureInfo.InvariantCulture), DataVoValueMapper.ToColumn(value));
            }

            rows[r] = values;
        }

        InTransaction("Upsert", lease =>
        {
            for (int r = 0; r < rows.Length; r++)
            {
                Check(lease.Execute(_deleteByKey, KeyParameter("k", keys[r])), "Upsert");
                Check(lease.Execute(_insert, rows[r]), "Upsert");
            }
        });
    }

    /// <summary>Deletes the records with <paramref name="keys"/> in one transaction; missing keys are ignored.</summary>
    public void Delete(IReadOnlyList<object> keys)
    {
        if (keys.Count == 0)
        {
            return;
        }

        InTransaction("Delete", lease =>
        {
            foreach (object key in keys)
            {
                Check(lease.Execute(_deleteByKey, KeyParameter("k", key)), "Delete");
            }
        });
    }

    /// <summary>The records with <paramref name="keys"/> that exist, in no particular order.</summary>
    public async IAsyncEnumerable<TRecord> GetAsync(IReadOnlyList<object> keys, bool includeVectors, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        RequireReadableVectors(includeVectors);
        for (int start = 0; start < keys.Count; start += KeyBatch)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int count = Math.Min(KeyBatch, keys.Count - start);
            var parameters = new DataVoQueryParameter[count];
            var placeholders = new StringBuilder();
            for (int i = 0; i < count; i++)
            {
                string name = "k" + i.ToString(CultureInfo.InvariantCulture);
                parameters[i] = KeyParameter(name, keys[start + i]);
                placeholders.Append(i == 0 ? "@" : ", @").Append(name);
            }

            string sql = count == 1
                ? $"SELECT {Columns(includeVectors)} FROM {Name} WHERE {Model.Key.Column} = @k0"
                : $"SELECT {Columns(includeVectors)} FROM {Name} WHERE {Model.Key.Column} IN ({placeholders})";
            foreach (TRecord record in Query(sql, parameters, includeVectors, "Get"))
            {
                yield return record;
            }
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <summary>Up to <paramref name="top"/> records matching <paramref name="filter"/>, ordered and skipped as asked.</summary>
    public IEnumerable<TRecord> GetFiltered(LambdaExpression filter, int top, FilteredRecordRetrievalOptions<TRecord>? options)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(top, 1);
        bool includeVectors = options?.IncludeVectors ?? false;
        RequireReadableVectors(includeVectors);
        var parameters = new List<DataVoQueryParameter>();
        var sql = new StringBuilder($"SELECT {Columns(includeVectors)} FROM {Name}");
        AppendWhere(sql, filter, parameters);
        if (options?.OrderBy is { } orderBy)
        {
            var orders = new List<string>();
            foreach (FilteredRecordRetrievalOptions<TRecord>.OrderByDefinition.SortInfo sort in orderBy(new()).Values)
            {
                DataVoPropertyModel property = DataVoFilterTranslator.SelectedProperty(sort.PropertySelector, Model);
                orders.Add(property.Column + (sort.Ascending ? " ASC" : " DESC"));
            }

            if (orders.Count > 0)
            {
                sql.Append(" ORDER BY ").AppendJoin(", ", orders);
            }
        }

        AppendPage(sql, top, options?.Skip ?? 0);
        return Query(sql.ToString(), [.. parameters], includeVectors, "Get");
    }

    /// <summary>The <paramref name="top"/> records nearest to <paramref name="searchValue"/>, with their scores.</summary>
    public async IAsyncEnumerable<VectorSearchResult<TRecord>> SearchAsync<TInput>(
        TInput searchValue,
        int top,
        VectorSearchOptions<TRecord>? options,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TInput : notnull
    {
        ArgumentNullException.ThrowIfNull(searchValue);
        ArgumentOutOfRangeException.ThrowIfLessThan(top, 1);
        options ??= new();
        DataVoPropertyModel vector = Model.ResolveVector(options.VectorProperty is { } selector
            ? DataVoFilterTranslator.SelectedProperty(selector, Model).Name
            : null);
        RequireReadableVectors(options.IncludeVectors);
        ReadOnlyMemory<float> query = await ToQueryVectorAsync(vector, searchValue, cancellationToken).ConfigureAwait(false);
        if (query.Length != vector.Dimensions)
        {
            throw new ArgumentException($"The search vector has {query.Length} dimensions; '{vector.Name}' has {vector.Dimensions}.", nameof(searchValue));
        }

        var parameters = new List<DataVoQueryParameter> { DataVoQueryParameter.FromObject("q", query) };
        string distance = $"{vector.Column} {vector.DistanceOperator} @q";
        var sql = new StringBuilder($"SELECT {Columns(options.IncludeVectors)}, {distance} AS {ScoreColumn} FROM {Name}");
        var conditions = new List<string>();
        if (options.Filter is { } filter)
        {
            conditions.Add(DataVoFilterTranslator.Translate(filter, Model, parameters));
        }

        if (options.ScoreThreshold is { } threshold && DistanceBound(vector, threshold) is { } bound)
        {
            parameters.Add(DataVoQueryParameter.FromObject("s", bound));
            conditions.Add($"{distance} <= @s");
        }

        if (conditions.Count > 0)
        {
            sql.Append(" WHERE ").AppendJoin(" AND ", conditions);
        }

        sql.Append(" ORDER BY ").Append(ScoreColumn).Append(" ASC");
        AppendPage(sql, top, options.Skip);
        cancellationToken.ThrowIfCancellationRequested();

        using DataVoSessionPool.Lease lease = _pool.Rent();
        QueryResult result = Check(lease.Execute(sql.ToString(), [.. parameters]), "Search");
        foreach (Dictionary<string, object?> row in result.Data)
        {
            double raw = Convert.ToDouble(row[ScoreColumn], CultureInfo.InvariantCulture);
            yield return new VectorSearchResult<TRecord>(ToRecord(row, options.IncludeVectors), Score(vector, raw));
        }
    }

    /// <summary>The key of <paramref name="record"/>; an empty auto-generated Guid key gets a new value when <paramref name="assign"/>.</summary>
    private object KeyOf(TRecord record, bool assign)
    {
        object? key = _mapper.GetValue(record, Model.Key);
        if (Model.Key.IsAutoGenerated && assign && key is Guid guid && guid == Guid.Empty)
        {
            key = Guid.NewGuid();
            _mapper.SetValue(record, Model.Key, key);
        }

        return key ?? throw new ArgumentException($"A record has no value for its key '{Model.Key.Name}'.");
    }

    private DataVoQueryParameter KeyParameter(string name, object key) =>
        DataVoQueryParameter.FromObject(name, Model.Key.Type == typeof(object) ? Convert.ToString(key, CultureInfo.InvariantCulture) : key);

    private async Task<Dictionary<DataVoPropertyModel, IList<Embedding<float>>>?> GenerateEmbeddingsAsync(IReadOnlyList<TRecord> records, CancellationToken cancellationToken)
    {
        Dictionary<DataVoPropertyModel, IList<Embedding<float>>>? generated = null;
        foreach (DataVoPropertyModel vector in Model.Vectors.Where(property => property.VectorKind == DataVoVectorKind.Generated))
        {
            var inputs = new List<object?>(records.Count);
            foreach (TRecord record in records)
            {
                inputs.Add(_mapper.GetValue(record, vector));
            }

            generated ??= [];
            generated[vector] = await DataVoEmbeddings.GenerateAsync(vector, inputs, cancellationToken).ConfigureAwait(false);
        }

        return generated;
    }

    private static object? GeneratedVector(Dictionary<DataVoPropertyModel, IList<Embedding<float>>> generated, DataVoPropertyModel property, int row) =>
        generated[property][row] is { } embedding ? embedding.Vector : null;

    private static object CheckDimensions(DataVoPropertyModel property, object value)
    {
        int length = value switch
        {
            ReadOnlyMemory<float> memory => memory.Length,
            float[] array => array.Length,
            Embedding<float> embedding => embedding.Vector.Length,
            _ => property.Dimensions,
        };
        return length == property.Dimensions
            ? value
            : throw new ArgumentException($"Vector '{property.Name}' has {length} dimensions; the collection declares {property.Dimensions}.");
    }

    private async Task<ReadOnlyMemory<float>> ToQueryVectorAsync<TInput>(DataVoPropertyModel vector, TInput searchValue, CancellationToken cancellationToken)
        where TInput : notnull
    {
        switch (searchValue)
        {
            case ReadOnlyMemory<float> memory:
                return memory;
            case float[] array:
                return array;
            case Embedding<float> embedding:
                return embedding.Vector;
        }

        if (vector.EmbeddingGenerator is null)
        {
            throw new NotSupportedException(
                $"Search input of type {typeof(TInput).Name} needs an embedding generator; search with ReadOnlyMemory<float>, float[] or Embedding<float>, or configure one.");
        }

        IList<Embedding<float>> embeddings = await DataVoEmbeddings.GenerateAsync(vector, [searchValue], cancellationToken).ConfigureAwait(false);
        return embeddings[0].Vector;
    }

    /// <summary>The largest raw distance a <paramref name="threshold"/> on the property's score admits.</summary>
    private static double? DistanceBound(DataVoPropertyModel vector, double threshold) => vector.DistanceFunction switch
    {
        DistanceFunction.CosineSimilarity => 1 - threshold,
        DistanceFunction.EuclideanSquaredDistance => threshold < 0 ? -1 : Math.Sqrt(threshold),
        _ => threshold,
    };

    /// <summary>The score reported for a raw DataVo distance in the property's distance function.</summary>
    private static double Score(DataVoPropertyModel vector, double distance) => vector.DistanceFunction switch
    {
        DistanceFunction.CosineSimilarity => 1 - distance,
        DistanceFunction.EuclideanSquaredDistance => distance * distance,
        _ => distance,
    };

    private void AppendWhere(StringBuilder sql, LambdaExpression filter, List<DataVoQueryParameter> parameters) =>
        sql.Append(" WHERE ").Append(DataVoFilterTranslator.Translate(filter, Model, parameters));

    private static void AppendPage(StringBuilder sql, int top, int skip)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(skip);
        sql.Append(" LIMIT ").Append(top.ToString(CultureInfo.InvariantCulture));
        if (skip > 0)
        {
            sql.Append(" OFFSET ").Append(skip.ToString(CultureInfo.InvariantCulture));
        }
    }

    private string Columns(bool includeVectors) => includeVectors ? _selectAll : _selectWithoutVectors;

    private void RequireReadableVectors(bool includeVectors)
    {
        if (includeVectors && Model.Vectors.Any(property => property.VectorKind == DataVoVectorKind.Generated))
        {
            throw new NotSupportedException("IncludeVectors is not supported for vector properties filled by an embedding generator: the record holds the generator's input, not the vector.");
        }
    }

    private List<TRecord> Query(string sql, DataVoQueryParameter[] parameters, bool includeVectors, string operation)
    {
        using DataVoSessionPool.Lease lease = _pool.Rent();
        QueryResult result = Check(lease.Execute(sql, parameters), operation);
        var records = new List<TRecord>(result.Data.Count);
        foreach (Dictionary<string, object?> row in result.Data)
        {
            records.Add(ToRecord(row, includeVectors));
        }

        return records;
    }

    private TRecord ToRecord(Dictionary<string, object?> row, bool includeVectors)
    {
        TRecord record = _mapper.CreateRecord();
        foreach (DataVoPropertyModel property in Model.All)
        {
            if (property.Role == DataVoPropertyRole.Vector && !includeVectors)
            {
                continue;
            }

            if (row.TryGetValue(property.Column, out object? cell))
            {
                _mapper.SetValue(record, property, DataVoValueMapper.FromColumn(cell, property.Type));
            }
        }

        return record;
    }

    private void InTransaction(string operation, Action<DataVoSessionPool.Lease> work)
    {
        using DataVoSessionPool.Lease lease = _pool.Rent();
        Check(lease.Execute("BEGIN"), operation);
        try
        {
            work(lease);
            Check(lease.Execute("COMMIT"), operation);
        }
        catch
        {
            lease.Execute("ROLLBACK");
            throw;
        }
    }

    private string IndexName(DataVoPropertyModel property) => $"ix_{Name}_{property.Column}";

    private QueryResult Check(QueryResult result, string operation) =>
        result.IsError
            ? throw new VectorStoreException($"DataVo {operation} on collection '{Name}' failed: {string.Join(" ", result.Messages)}")
            {
                VectorStoreSystemName = DataVoVectorStore.SystemName,
                VectorStoreName = _pool.DatabaseName,
                CollectionName = Name,
                OperationName = operation,
            }
            : result;
}
