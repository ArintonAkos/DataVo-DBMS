using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using ReactiveRowRef = DataVo.Core.Runtime.Reactive.RowRef;

namespace DataVo.Core;

/// <summary>
/// Compiles and registers strongly-typed LINQ reactive subscriptions (<c>Subscribe</c> and zero-allocation
/// <c>SubscribeInto</c>) over <see cref="DataVoTable{T}"/> and its composed query types.
/// </summary>
internal static class DataVoReactiveLinq
{
    /// <summary>
    /// Subscribes to incremental <see cref="QueryChange{T}"/> deltas for a model query.
    /// </summary>
    public static IDisposable SubscribeModels<T, TState>(
        DataVoQuery<T, TState> source,
        DataVoQueryShape<T> shape,
        Action<QueryChange<T>> onChanged)
    {
        Compat.ThrowHelper.ThrowIfNull(onChanged);
        DataVoTable<T> table = source.Table;

        if (shape.IsOrderCompiled && source.TryBind(out string? where, out DataVoQueryParameter[] parameters))
        {
            return SubscribeCompiledModels(table, where, parameters, shape, onChanged);
        }

        if (shape.IsOrdered || shape.Skip > 0 || shape.Take >= 0)
        {
            throw new NotSupportedException(
                "Reactive subscriptions with OrderBy, Skip, or Take require compiled predicates and column sort keys. " +
                "Enable DataVo interceptors and use static lambdas over table columns.");
        }

        return SubscribeFallbackModels(source, table, onChanged);
    }

    /// <summary>
    /// Subscribes to incremental <see cref="QueryChange{TResult}"/> deltas for a projected query.
    /// </summary>
    public static IDisposable SubscribeProjected<T, TState, TResult>(
        DataVoQuery<T, TState> source,
        DataVoQueryShape<T> shape,
        DataVoProjection<T, TResult>? projection,
        Func<T, TResult> selector,
        Action<QueryChange<TResult>> onChanged)
    {
        Compat.ThrowHelper.ThrowIfNull(selector);
        Compat.ThrowHelper.ThrowIfNull(onChanged);
        DataVoTable<T> table = source.Table;

        if (projection is not null
            && shape.IsOrderCompiled
            && source.TryBind(out string? where, out DataVoQueryParameter[] parameters))
        {
            return SubscribeCompiledProjection(table, where, parameters, shape, projection, onChanged);
        }

        return SubscribeModels(source, shape, change => DeliverMapped(change, selector, onChanged));
    }

    /// <summary>
    /// Subscribes a live model window directly into <paramref name="destination"/>, writing the current window
    /// in place and invoking <paramref name="onUpdated"/> with the live element count.
    /// </summary>
    public static IDisposable SubscribeModelsInto<T, TState>(
        DataVoQuery<T, TState> source,
        DataVoQueryShape<T> shape,
        Memory<T> destination,
        Action<int> onUpdated)
    {
        Compat.ThrowHelper.ThrowIfNull(onUpdated);
        DataVoTable<T> table = source.Table;

        (string canonicalTable, string primaryKey, BufferedWindowSortKey[] sortKeys, int skip, int take,
            ReactivePredicate? predicate, Func<IReadOnlyDictionary<string, object?>, bool>? fallbackPredicate) =
            BuildWindowSpec(source, shape, table);

        if (table.ReactiveRowReader is { } reactiveReader)
        {
            var projector = new DelegateModelProjector<T>(reactiveReader);
            var query = new BufferedWindowReactiveQuery<T, DelegateModelProjector<T>>(
                canonicalTable,
                primaryKey,
                table.ModelColumnNames,
                sortKeys,
                skip,
                take,
                predicate,
                fallbackPredicate,
                projector,
                destination,
                onUpdated);
            return table.Context.Engine.Reactive.AddDirect(table.Context, query);
        }

        var fallbackProjector = new DictionaryModelProjector<T>(table);
        var fallbackQuery = new BufferedWindowReactiveQuery<T, DictionaryModelProjector<T>>(
            canonicalTable,
            primaryKey,
            table.ModelColumnNames,
            sortKeys,
            skip,
            take,
            predicate,
            fallbackPredicate,
            fallbackProjector,
            destination,
            onUpdated);
        return table.Context.Engine.Reactive.AddDirect(table.Context, fallbackQuery);
    }

    /// <summary>
    /// Subscribes a live projected window directly into <paramref name="destination"/> using a source-generated
    /// zero-allocation <see cref="IReactiveRowProjector{TProjection}"/> struct when compiled.
    /// </summary>
    public static IDisposable SubscribeProjectedInto<T, TState, TResult>(
        DataVoQuery<T, TState> source,
        DataVoQueryShape<T> shape,
        DataVoProjection<T, TResult>? projection,
        Func<T, TResult> selector,
        Memory<TResult> destination,
        Action<int> onUpdated)
    {
        Compat.ThrowHelper.ThrowIfNull(selector);
        Compat.ThrowHelper.ThrowIfNull(onUpdated);
        DataVoTable<T> table = source.Table;

        (string canonicalTable, string primaryKey, BufferedWindowSortKey[] sortKeys, int skip, int take,
            ReactivePredicate? predicate, Func<IReadOnlyDictionary<string, object?>, bool>? fallbackPredicate) =
            BuildWindowSpec(source, shape, table);

        if (projection is not null)
        {
            return projection.SubscribeInto(
                table.Context,
                canonicalTable,
                primaryKey,
                sortKeys,
                skip,
                take,
                predicate,
                fallbackPredicate,
                destination,
                onUpdated);
        }

        var fallbackProjector = new SelectorFallbackProjector<T, TResult>(table, selector);
        var query = new BufferedWindowReactiveQuery<TResult, SelectorFallbackProjector<T, TResult>>(
            canonicalTable,
            primaryKey,
            table.ModelColumnNames,
            sortKeys,
            skip,
            take,
            predicate,
            fallbackPredicate,
            fallbackProjector,
            destination,
            onUpdated);
        return table.Context.Engine.Reactive.AddDirect(table.Context, query);
    }

    /// <summary>Materializes an owned <see cref="QueryChange"/> into a typed <see cref="QueryChange{TResult}"/>.</summary>
    internal static void DeliverOwned<TResult>(
        QueryChange change,
        Func<IReadOnlyDictionary<string, object?>, TResult> read,
        Action<QueryChange<TResult>> onChanged)
    {
        TResult[] added = MapOwned(change.Added, read);
        TResult[] removed = MapOwned(change.Removed, read);
        TResult[] updated = MapOwned(change.Updated, read);
        TResult[] updatedBefore = MapOwned(change.UpdatedBefore, read);
        onChanged(new QueryChange<TResult>(added, removed, updated, updatedBefore));
    }

    /// <summary>Materializes a borrowed <see cref="QueryChangeRef"/> using a value-type struct projector.</summary>
    internal static void DeliverBorrowed<TResult, TProjector>(
        in QueryChangeRef change,
        TProjector projector,
        Action<QueryChange<TResult>> onChanged)
        where TProjector : struct, IReactiveRowProjector<TResult>
    {
        TResult[] added = MapBorrowed<TResult, TProjector>(change.Added, projector);
        TResult[] removed = MapBorrowed<TResult, TProjector>(change.Removed, projector);
        TResult[] updated = MapBorrowed<TResult, TProjector>(change.Updated, projector);
        TResult[] updatedBefore = MapBorrowed<TResult, TProjector>(change.UpdatedBefore, projector);
        onChanged(new QueryChange<TResult>(added, removed, updated, updatedBefore));
    }

    private static IDisposable SubscribeCompiledModels<T>(
        DataVoTable<T> table,
        string? where,
        DataVoQueryParameter[] parameters,
        DataVoQueryShape<T> shape,
        Action<QueryChange<T>> onChanged)
    {
        (DataVoPreparedStatement statement, DataVoQueryParameter[] boundParams, bool isTopK) =
            PrepareSubscriptionStatement(table, table.ModelColumns, where, parameters, shape);

        if (isTopK && table.ReactiveRowReader is { } reactiveReader)
        {
            var projector = new DelegateModelProjector<T>(reactiveReader);
            return table.Context.SubscribeZeroAlloc(
                statement,
                (in QueryChangeRef delta) => DeliverBorrowed(in delta, projector, onChanged),
                boundParams);
        }

        return table.Context.Subscribe(
            statement,
            change => DeliverOwned(change, table.Read, onChanged),
            boundParams);
    }

    private static IDisposable SubscribeCompiledProjection<T, TResult>(
        DataVoTable<T> table,
        string? where,
        DataVoQueryParameter[] parameters,
        DataVoQueryShape<T> shape,
        DataVoProjection<T, TResult> projection,
        Action<QueryChange<TResult>> onChanged)
    {
        (DataVoPreparedStatement statement, DataVoQueryParameter[] boundParams, bool isTopK) =
            PrepareSubscriptionStatement(table, projection.ColumnList, where, parameters, shape);

        if (isTopK && projection.HasReactiveProjector)
        {
            return projection.SubscribeBorrowed(table.Context, statement, boundParams, onChanged);
        }

        return table.Context.Subscribe(
            statement,
            change => DeliverOwned(change, projection.ReadDictionary, onChanged),
            boundParams);
    }

    private static (DataVoPreparedStatement Statement, DataVoQueryParameter[] Parameters, bool IsTopK)
        PrepareSubscriptionStatement<T>(
            DataVoTable<T> table,
            string columns,
            string? where,
            DataVoQueryParameter[] parameters,
            DataVoQueryShape<T> shape)
    {
        bool needsWindow = shape.IsOrdered || shape.Take >= 0 || shape.Skip > 0;
        string? orderBy = shape.IsOrdered
            ? shape.OrderBy
            : (needsWindow ? table.KeyColumn : null);
        long limit = shape.Take >= 0
            ? shape.Take
            : (needsWindow ? int.MaxValue : -1);

        string sql = DataVoSql.Select(columns, table.TableName, where, orderBy, limit >= 0, shape.Skip > 0);
        DataVoPreparedStatement statement = table.GetOrPrepareStatement(sql);
        DataVoQueryParameter[] boundParams = DataVoSql.Paged(parameters, shape.Skip, limit);
        return (statement, boundParams, orderBy is not null && limit >= 0);
    }

    private static IDisposable SubscribeFallbackModels<T, TState>(
        DataVoQuery<T, TState> source,
        DataVoTable<T> table,
        Action<QueryChange<T>> onChanged)
    {
        string databaseName = ResolveDatabase(table.Context);
        string canonicalTable = table.Context.Engine.Catalog.FindTableName(table.TableName, databaseName) ?? table.TableName;
        var query = new TypedFallbackLinearQuery<T, TState>(
            canonicalTable,
            ReactiveTableLayouts.ForTable(table.Context.Engine, canonicalTable, databaseName),
            source,
            table);
        return table.Context.Engine.Reactive.AddCompiled(
            table.Context,
            query,
            change => DeliverOwned(change, table.Read, onChanged));
    }

    private static (
        string CanonicalTable,
        string PrimaryKey,
        BufferedWindowSortKey[] SortKeys,
        int Skip,
        int Take,
        ReactivePredicate? Predicate,
        Func<IReadOnlyDictionary<string, object?>, bool>? FallbackPredicate)
        BuildWindowSpec<T, TState>(
            DataVoQuery<T, TState> source,
            DataVoQueryShape<T> shape,
            DataVoTable<T> table)
    {
        if (shape.IsOrdered && !shape.IsOrderCompiled)
        {
            throw new NotSupportedException(
                "SubscribeInto requires compiled column sort keys (such as p => p.Score). " +
                "Enable DataVo interceptors and sort by mapped table columns.");
        }

        string databaseName = ResolveDatabase(table.Context);
        string canonicalTable = table.Context.Engine.Catalog.FindTableName(table.TableName, databaseName) ?? table.TableName;
        string primaryKey = table.KeyColumn;

        BufferedWindowSortKey[] sortKeys = shape.Keys.Length == 0
            ? []
            : BuildSortKeys(shape.Keys);
        int skip = shape.Skip > int.MaxValue ? int.MaxValue : (int)Math.Max(0, shape.Skip);
        int take = shape.Take < 0 ? int.MaxValue : shape.Take > int.MaxValue ? int.MaxValue : (int)shape.Take;

        ReactivePredicate? predicate = null;
        Func<IReadOnlyDictionary<string, object?>, bool>? fallbackPredicate = null;

        if (source.TryBind(out string? where, out DataVoQueryParameter[] parameters))
        {
            if (!string.IsNullOrEmpty(where))
            {
                string filterSql = DataVoSql.Select(primaryKey, table.TableName, where, null, false, false);
                DataVoPreparedStatement stmt = table.GetOrPrepareStatement(filterSql);
                var bound = (SelectStatement)stmt.BindSingleReactiveStatement(parameters);
                predicate = ReactivePredicate.Compile(bound.WhereExpression);
            }
        }
        else
        {
            fallbackPredicate = row => source.MatchesInMemory(table.Read(row));
        }

        return (canonicalTable, primaryKey, sortKeys, skip, take, predicate, fallbackPredicate);
    }

    private static BufferedWindowSortKey[] BuildSortKeys<T>(DataVoSortKey<T>[] keys)
    {
        var result = new BufferedWindowSortKey[keys.Length];
        for (int i = 0; i < keys.Length; i++)
        {
            DataVoOrderKey column = keys[i].Column!;
            result[i] = new BufferedWindowSortKey(column.Column, !column.Descending);
        }

        return result;
    }

    private static string ResolveDatabase(DataVoContext context)
    {
        string? databaseName = context.Engine.Sessions.Get(context.SessionId);
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new InvalidOperationException(
                "No database selected for the current session. Execute USE <database> before subscribing.");
        }

        return databaseName;
    }

    private static void DeliverMapped<T, TResult>(
        QueryChange<T> source,
        Func<T, TResult> selector,
        Action<QueryChange<TResult>> onChanged)
    {
        onChanged(new QueryChange<TResult>(
            MapList(source.Added, selector),
            MapList(source.Removed, selector),
            MapList(source.Updated, selector),
            MapList(source.UpdatedBefore, selector)));
    }

    private static TResult[] MapOwned<TResult>(
        IReadOnlyList<IReadOnlyDictionary<string, object?>> rows,
        Func<IReadOnlyDictionary<string, object?>, TResult> read)
    {
        if (rows.Count == 0)
        {
            return [];
        }

        var result = new TResult[rows.Count];
        for (int i = 0; i < rows.Count; i++)
        {
            result[i] = read(rows[i]);
        }

        return result;
    }

    private static TResult[] MapBorrowed<TResult, TProjector>(RowSet rows, TProjector projector)
        where TProjector : struct, IReactiveRowProjector<TResult>
    {
        if (rows.Count == 0)
        {
            return [];
        }

        var result = new TResult[rows.Count];
        for (int i = 0; i < rows.Count; i++)
        {
            ReactiveRowRef row = rows[i];
            result[i] = projector.Project(in row);
        }

        return result;
    }

    private static TResult[] MapList<T, TResult>(IReadOnlyList<T> items, Func<T, TResult> selector)
    {
        if (items.Count == 0)
        {
            return [];
        }

        var result = new TResult[items.Count];
        for (int i = 0; i < items.Count; i++)
        {
            result[i] = selector(items[i]);
        }

        return result;
    }

    private readonly struct DelegateModelProjector<T>(DataVoReactiveRowReader<T> read) : IReactiveRowProjector<T>
    {
        public T Project(in ReactiveRowRef row) => read(in row);
    }

    private readonly struct DictionaryModelProjector<T>(DataVoTable<T> table) : IReactiveRowProjector<T>
    {
        public T Project(in ReactiveRowRef row) => table.Read(row.ToOwnedDictionary());
    }

    private readonly struct SelectorFallbackProjector<T, TResult>(DataVoTable<T> table, Func<T, TResult> selector)
        : IReactiveRowProjector<TResult>
    {
        public TResult Project(in ReactiveRowRef row) =>
            selector(table.ReactiveRowReader is { } reader
                ? reader(in row)
                : table.Read(row.ToOwnedDictionary()));
    }

    /// <summary>
    /// A linear subscription whose filter is a C# predicate evaluated on materialized models (the LINQ shapes the
    /// interceptors could not compile to SQL). It emits whole catalog rows; owned delivery maps them to models.
    /// </summary>
    private sealed class TypedFallbackLinearQuery<T, TState>(
        string tableName,
        ReactiveRowLayout layout,
        DataVoQuery<T, TState> source,
        DataVoTable<T> table) : IBorrowedReactiveQuery
    {
        private readonly CellValue[] _before = new CellValue[layout.Count];
        private readonly CellValue[] _after = new CellValue[layout.Count];

        public IReadOnlyCollection<string> Tables => [tableName];

        public ReactiveRowSchema OutputSchema => layout.Schema;

        public void Seed(string table, IEnumerable<TypedRow> rows)
        {
            // Predicate transitions are evaluated directly on before/after images (matching ReactiveSubscription).
        }

        public void ApplyInto(IReadOnlyList<RowChange> changes, QueryChangeBuilder builder)
        {
            for (int i = 0; i < changes.Count; i++)
            {
                RowChange change = changes[i];
                bool beforeMatches = layout.TryLoadBefore(change, _before) && source.MatchesInMemory(table.Read(change.Before!));
                bool afterMatches = layout.TryLoadAfter(change, _after) && source.MatchesInMemory(table.Read(change.After!));

                if (beforeMatches && afterMatches)
                {
                    builder.AddUpdatedBeforeRow(_before);
                    builder.AddUpdatedRow(_after);
                }
                else if (afterMatches)
                {
                    builder.AddAddedRow(_after);
                }
                else if (beforeMatches)
                {
                    builder.AddRemovedRow(_before);
                }
            }
        }
    }
}
