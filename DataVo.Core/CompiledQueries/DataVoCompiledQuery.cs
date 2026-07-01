using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using DataVo.Core.BTree;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Transactions;
using DataVo.Core.Utils;

namespace DataVo.Core.CompiledQueries;

/// <summary>
/// Executes source-generated compiled query plans against a <see cref="DataVoContext"/>.
/// </summary>
public static class DataVoCompiledQuery
{
    private static readonly ConditionalWeakTable<DataVoContext, ConcurrentDictionary<PreparedSelectSingleCacheKey, object>> PreparedSelectSingleCache = new();

    /// <summary>
    /// Executes a select plan and returns the first mapped row, or <c>default</c> when no row matches.
    /// </summary>
    public static TResult? SelectSingle<TResult>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        Func<Dictionary<string, object?>, TResult> mapper)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentNullException.ThrowIfNull(mapper);

        if (plan.Kind != DataVoCompiledQueryKind.SelectSingle)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as SelectSingle.");
        }

        IReadOnlyList<TResult> rows = ExecuteSelect(context, plan, parameters, mapper);
        return rows.Count == 0 ? default : rows[0];
    }

    /// <summary>
    /// Executes a select plan and returns every mapped row that matches the plan predicate.
    /// </summary>
    public static IReadOnlyList<TResult> SelectMany<TResult>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        Func<Dictionary<string, object?>, TResult> mapper)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentNullException.ThrowIfNull(mapper);

        if (plan.Kind != DataVoCompiledQueryKind.SelectMany && plan.Kind != DataVoCompiledQueryKind.SelectSingle)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as SelectMany.");
        }

        return ExecuteSelect(context, plan, parameters, mapper);
    }

    /// <summary>
    /// Executes a select plan and returns every row projected by <paramref name="mapper"/> directly from typed
    /// cells — no dictionary materialization, no boxing. Behavior matches <see cref="SelectMany{TResult}"/>.
    /// </summary>
    public static IReadOnlyList<T> SelectManyTyped<T>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        CompiledRowMapper<T> mapper)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentNullException.ThrowIfNull(mapper);

        if (plan.Kind != DataVoCompiledQueryKind.SelectMany && plan.Kind != DataVoCompiledQueryKind.SelectSingle)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as SelectMany.");
        }

        return ExecuteSelectTyped(context, plan, parameters, mapper);
    }

    /// <summary>
    /// Executes a select plan and returns the first projected row, or <c>default</c> when none matches.
    /// </summary>
    public static T? SelectSingleTyped<T>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        CompiledRowMapper<T> mapper)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentNullException.ThrowIfNull(mapper);

        if (plan.Kind != DataVoCompiledQueryKind.SelectSingle)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as SelectSingle.");
        }

        DataVoPreparedSelectSingle<T> prepared = GetOrPrepareSelectSingleTyped(context, plan, mapper);
        return prepared.ExecuteParameterValue(RequiredSingleParameterValue(parameters, plan.WhereParameterName!));
    }

    /// <summary>
    /// Prepares a typed point-lookup plan by resolving the access path and projection metadata once. The returned
    /// handle is optimized for warm scalar key execution and avoids dictionary, StoredRow, and projection setup
    /// allocations on the indexed hit path.
    /// </summary>
    public static DataVoPreparedSelectSingle<T> PrepareSelectSingleTyped<T>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        CompiledRowMapper<T> mapper)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(mapper);

        if (plan.Kind != DataVoCompiledQueryKind.SelectSingle)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be prepared as SelectSingle.");
        }

        string databaseName = ResolveCurrentDatabase(context);
        PreparedProjection projection = BuildPreparedProjection(context, plan, databaseName);
        string? indexName = ResolvePreparedIndexName(context, plan, databaseName);
        return new DataVoPreparedSelectSingle<T>(context, plan, databaseName, indexName, projection, mapper);
    }

    /// <summary>
    /// Prepares a typed multi-row equality lookup by resolving the access path and projection metadata once.
    /// </summary>
    public static DataVoPreparedSelectMany<T> PrepareSelectManyTyped<T>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        CompiledRowMapper<T> mapper)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(mapper);

        if (plan.Kind != DataVoCompiledQueryKind.SelectMany && plan.Kind != DataVoCompiledQueryKind.SelectSingle)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be prepared as SelectMany.");
        }

        string databaseName = ResolveCurrentDatabase(context);
        PreparedProjection projection = BuildPreparedProjection(context, plan, databaseName);
        string? indexName = ResolvePreparedIndexName(context, plan, databaseName);
        return new DataVoPreparedSelectMany<T>(context, plan, databaseName, indexName, projection, mapper);
    }

    private static IReadOnlyList<T> ExecuteSelectTyped<T>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        CompiledRowMapper<T> mapper)
    {
        string databaseName = ResolveCurrentDatabase(context);
        Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
        object? expected = RequiredParameter(parameterDictionary, plan.WhereParameterName!);
        string expectedKey = BuildComparisonKey(plan.WhereColumn!, expected);

        IReadOnlyList<long>? rowIds = TryResolveMatchingRowIds(context, plan, databaseName, expectedKey);
        if (rowIds is null)
        {
            // Scan fallback: no index path resolved — reuse the full-decode finder + reader (unchanged behavior).
            List<KeyValuePair<long, StoredRow>> scanned =
                TryReadMatchingStoredRows(context, plan, databaseName, expectedKey);
            var scannedResults = new T[scanned.Count];
            for (int i = 0; i < scanned.Count; i++)
            {
                scannedResults[i] = mapper(new CompiledRowReader(scanned[i].Value.AsView()));
            }

            return scannedResults;
        }

        // Streaming projection pushdown: decode only the projected columns of each visible row into a reused
        // buffer, mapping straight to T — no StoredRow, no dictionary, no decode of unprojected columns.
        IReadOnlyList<Column> columns = context.Engine.Catalog.GetTableColumns(plan.TableName, databaseName);
        bool[] isProjected = new bool[columns.Count];
        var projectedNames = new List<string>(plan.ProjectedColumns.Count);
        for (int i = 0; i < columns.Count; i++)
        {
            if (ContainsIgnoreCase(plan.ProjectedColumns, columns[i].Name))
            {
                isProjected[i] = true;
                projectedNames.Add(columns[i].Name);
            }
        }

        var projectedSchema = new ReactiveRowSchema(projectedNames);
        var buffer = new CellValue[projectedNames.Count];

        var results = new List<T>(rowIds.Count);
        foreach (long rowId in rowIds)
        {
            if (!context.Engine.StorageContext.IsRowVisible(plan.TableName, databaseName, rowId))
            {
                continue;
            }

            if (context.Engine.StorageContext.TryReadStoredRow(plan.TableName, databaseName, rowId, out StoredRow? storedRow)
                && storedRow is not null)
            {
                results.Add(mapper(new CompiledRowReader(storedRow.AsView())));
                continue;
            }

            byte[]? bytes = context.Engine.StorageContext.TryReadRowBytes(plan.TableName, databaseName, rowId);
            if (bytes is null)
            {
                continue;
            }

            RowSerializer.DecodeProjectedCells(bytes, columns, isProjected, buffer);
            results.Add(mapper(new CompiledRowReader(new StoredRowView(projectedSchema, buffer))));
        }

        return results;
    }

    private static bool ContainsIgnoreCase(IReadOnlyList<string> values, string candidate)
    {
        for (int i = 0; i < values.Count; i++)
        {
            if (string.Equals(values[i], candidate, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static DataVoPreparedSelectSingle<T> GetOrPrepareSelectSingleTyped<T>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        CompiledRowMapper<T> mapper)
    {
        ConcurrentDictionary<PreparedSelectSingleCacheKey, object> cache =
            PreparedSelectSingleCache.GetValue(context, static _ => new ConcurrentDictionary<PreparedSelectSingleCacheKey, object>());
        var key = new PreparedSelectSingleCacheKey(plan, mapper);
        return (DataVoPreparedSelectSingle<T>)cache.GetOrAdd(
            key,
            static (_, state) => PrepareSelectSingleTyped(state.Context, state.Plan, state.Mapper),
            (Context: context, Plan: plan, Mapper: mapper));
    }

    private static PreparedProjection BuildPreparedProjection(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName)
    {
        IReadOnlyList<Column> columns = context.Engine.Catalog.GetTableColumns(plan.TableName, databaseName);
        bool[] isProjected = new bool[columns.Count];
        var projectedNames = new List<string>(plan.ProjectedColumns.Count);
        for (int i = 0; i < columns.Count; i++)
        {
            if (ContainsIgnoreCase(plan.ProjectedColumns, columns[i].Name))
            {
                isProjected[i] = true;
                projectedNames.Add(columns[i].Name);
            }
        }

        return new PreparedProjection(
            columns,
            isProjected,
            new ReactiveRowSchema(projectedNames),
            new CellValue[projectedNames.Count]);
    }

    private static string? ResolvePreparedIndexName(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName)
    {
        if (plan.AccessPath == CompiledAccessPath.SingleColumnIndex && plan.ResolvedIndexName is not null)
        {
            return plan.ResolvedIndexName;
        }

        List<string> primaryKeys = context.Engine.Catalog.GetTablePrimaryKeys(plan.TableName, databaseName);
        if (primaryKeys.Contains(plan.WhereColumn!, StringComparer.OrdinalIgnoreCase))
        {
            return $"_PK_{plan.TableName}";
        }

        return TryResolveSingleColumnIndex(context, plan.TableName, databaseName, plan.WhereColumn!, out string secondaryIndexName)
            ? secondaryIndexName
            : null;
    }

    /// <summary>
    /// Executes an insert plan for a single row and returns inserted row identifiers.
    /// </summary>
    public static IReadOnlyList<long> Insert(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);

        if (plan.Kind != DataVoCompiledQueryKind.Insert)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as Insert.");
        }

        Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
        var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < plan.InsertColumns.Count; i++)
        {
            row[plan.InsertColumns[i]] = RequiredParameter(parameterDictionary, plan.InsertParameterNames[i]);
        }

        return context.BulkInsert(plan.TableName, [row]);
    }

    /// <summary>
    /// Executes an update plan and returns the affected row count.
    /// </summary>
    public static int Update(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);

        if (plan.Kind != DataVoCompiledQueryKind.Update)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as Update.");
        }

        if (context.Engine.TransactionManager.HasActiveTransaction(context.SessionId))
        {
            throw new NotSupportedException("Compiled update plans do not support active transactions.");
        }

        // Zero-allocation byte-patch path for fixed-width, index-resolved single-row updates. Returns false
        // for any shape it cannot handle, falling through to the dictionary path below.
        if (TryUpdateFixedWidthFastPath(context, plan, parameters, out int fastPathAffected))
        {
            return fastPathAffected;
        }

        string databaseName = ResolveCurrentDatabase(context);
        Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
        object? expected = RequiredParameter(parameterDictionary, plan.WhereParameterName!);
        string expectedKey = BuildComparisonKey(plan.WhereColumn!, expected);
        List<KeyValuePair<long, Dictionary<string, object?>>> candidates = TryReadMatchingRowEntries(context, plan, databaseName, expectedKey);
        if (candidates.Count == 0)
        {
            return 0;
        }

        List<long> candidateRowIds = candidates.Select(static pair => pair.Key).ToList();
        List<long> rowWriteLocks = context.Engine.LockManager.AcquireRowWriteLocks(databaseName, plan.TableName, candidateRowIds);

        try
        {
            List<long> revalidatedRowIds = RevalidateMatchingRowIdsAfterLock(context, plan, databaseName, expectedKey, candidateRowIds, rowWriteLocks);
            if (revalidatedRowIds.Count == 0)
            {
                return 0;
            }

            Dictionary<long, Dictionary<string, object?>> existingRows =
                context.Engine.StorageContext.GetTableContents(revalidatedRowIds, plan.TableName, databaseName);
            List<long> orderedRowIds = revalidatedRowIds
                .Where(existingRows.ContainsKey)
                .ToList();

            if (orderedRowIds.Count == 0)
            {
                return 0;
            }

            foreach (long rowId in orderedRowIds)
            {
                MvccCoordinator.ValidateCanModifyRow(context.Engine, databaseName, plan.TableName, rowId, null, "UPDATE");
            }

            Dictionary<string, Column> columnsByName = GetTableColumnsByName(context, plan.TableName, databaseName);
            List<Dictionary<string, object?>> newRows = orderedRowIds
                .Select(rowId => ApplyAssignments(existingRows[rowId], plan, parameterDictionary, columnsByName))
                .ToList();

            ValidateUpdatedRows(context, plan.TableName, databaseName, orderedRowIds, existingRows, newRows);

            long statementTxId = MvccCoordinator.ResolveStatementTransactionId(context.Engine, null);
            List<Dictionary<string, object?>> oldRows = orderedRowIds
                .Select(rowId => existingRows[rowId])
                .ToList();
            ReplaceRows(context, plan.TableName, databaseName, orderedRowIds, oldRows, newRows, statementTxId);
            return newRows.Count;
        }
        finally
        {
            context.Engine.LockManager.ReleaseRowWriteLocks(databaseName, plan.TableName, rowWriteLocks);
        }
    }

    /// <summary>
    /// Executes a fixed-width update plan with unboxed primitive parameters. This is the hot-path overload used
    /// by allocation-sensitive loops after the plan has already proven eligible for byte patching.
    /// </summary>
    public static int UpdateFixedWidth(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        ReadOnlySpan<DataVoFixedWidthQueryParameter> parameters)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);

        if (plan.Kind != DataVoCompiledQueryKind.Update)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as Update.");
        }

        if (context.Engine.TransactionManager.HasActiveTransaction(context.SessionId))
        {
            throw new NotSupportedException("Compiled update plans do not support active transactions.");
        }

        if (TryUpdateFixedWidthFastPath(context, plan, parameters, out int fastPathAffected))
        {
            return fastPathAffected;
        }

        var boxed = new DataVoCompiledQueryParameter[parameters.Length];
        for (int i = 0; i < parameters.Length; i++)
        {
            boxed[i] = new DataVoCompiledQueryParameter(parameters[i].Name, parameters[i].Value.ToObject());
        }

        return Update(context, plan, boxed);
    }

    /// <summary>
    /// Executes a fixed-width update where assignment values are supplied in plan assignment order and the
    /// predicate is the integer primary key. This overload is fully allocation-free at the parameter boundary.
    /// </summary>
    public static int UpdateFixedWidthByPrimaryKey(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        DataVoFixedWidthValue primaryKeyValue,
        ReadOnlySpan<DataVoFixedWidthValue> assignmentValues)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);

        if (plan.Kind != DataVoCompiledQueryKind.Update)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as Update.");
        }

        if (assignmentValues.Length != plan.Assignments.Count)
        {
            throw new ArgumentException("Assignment values must match the compiled update assignment count.", nameof(assignmentValues));
        }

        if (context.Engine.TransactionManager.HasActiveTransaction(context.SessionId))
        {
            throw new NotSupportedException("Compiled update plans do not support active transactions.");
        }

        if (TryUpdateFixedWidthFastPath(context, plan, primaryKeyValue, assignmentValues, out int fastPathAffected))
        {
            return fastPathAffected;
        }

        var boxed = new DataVoCompiledQueryParameter[assignmentValues.Length + 1];
        int index = 0;
        foreach ((_, string parameterName) in plan.Assignments)
        {
            boxed[index] = new DataVoCompiledQueryParameter(parameterName, assignmentValues[index].ToObject());
            index++;
        }

        boxed[index] = new DataVoCompiledQueryParameter(plan.WhereParameterName!, primaryKeyValue.ToObject());
        return Update(context, plan, boxed);
    }

    /// <summary>
    /// Executes many fixed-width primary-key updates as one LSM storage batch. This preserves the LSM
    /// byte-patch fast lane while allowing a benchmark phase to amortize strict WAL fsync cost.
    /// </summary>
    public static int UpdateFixedWidthByPrimaryKeyBatch(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoFixedWidthUpdateBatchEntry> updates)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(updates);

        if (plan.Kind != DataVoCompiledQueryKind.Update)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as Update.");
        }

        DataVoEngine engine = context.Engine;
        if (!engine.Config.EnableZeroAllocCompiledUpdate
            || engine.Config.StorageMode != StorageMode.Lsm
            || engine.Changes.Enabled)
        {
            throw new NotSupportedException("Batched fixed-width primary-key updates currently require the LSM fast path.");
        }

        string databaseName = ResolveCurrentDatabase(context);
        UpdateFastPathSchema schema = ResolveUpdateFastPathSchema(engine, plan, databaseName, trustCachedSchema: true);
        int assignmentWidth = schema.AssignOrdinals.Length;
        if (!schema.Eligible
            || assignmentWidth != plan.Assignments.Count
            || assignmentWidth > 2
            || !engine.IndexManager.HasIntegerPrimaryKeyFastLane(schema.PkIndexName, plan.TableName, databaseName))
        {
            throw new NotSupportedException("Batched fixed-width primary-key updates require an eligible integer primary-key update plan.");
        }

        var operations = new List<FixedWidthPatchOperation>(updates.Count);
        for (int i = 0; i < updates.Count; i++)
        {
            DataVoFixedWidthUpdateBatchEntry update = updates[i];
            if (!TryConvertToInt64(update.PrimaryKey, out long primaryKey))
            {
                throw new ArgumentException("Primary key values must be convertible to Int64.", nameof(updates));
            }

            if (!engine.IndexManager.TryLookupIntegerPrimaryKey(primaryKey, schema.PkIndexName, plan.TableName, databaseName, out long rowId))
            {
                continue;
            }

            MvccCoordinator.ValidateCanModifyRow(engine, databaseName, plan.TableName, rowId, null, "UPDATE");
            operations.Add(new FixedWidthPatchOperation(rowId, update.Value0, update.Value1));
        }

        return engine.StorageContext.TryPatchFixedWidthRows(
            plan.TableName,
            databaseName,
            schema.Columns,
            schema.AssignOrdinals,
            operations);
    }

    /// <summary>
    /// Per-(plan, database) cache of the resolved fast-path schema, keyed off the table schema version so it
    /// invalidates on DDL. Resolving it once keeps the steady-state update path free of catalog allocations.
    /// </summary>
    private static readonly ConcurrentDictionary<(DataVoCompiledQueryPlan Plan, string Database), UpdateFastPathSchema> UpdateFastPathSchemaCache = new();

    /// <summary>
    /// Attempts the zero-allocation update: a fixed-width, index-resolved, single-row UPDATE that byte-patches
    /// the stored row in place and logs a binary WAL frame. Returns <see langword="false"/> — without mutating
    /// storage — for any shape it cannot handle, so the caller runs the legacy dictionary path instead.
    /// </summary>
    private static bool TryUpdateFixedWidthFastPath(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        out int affected)
    {
        affected = 0;
        DataVoEngine engine = context.Engine;

        // Reactive change capture needs after-image materialization the byte-patch path deliberately skips.
        if (!engine.Config.EnableZeroAllocCompiledUpdate
            || (engine.Config.StorageMode != StorageMode.Disk && engine.Config.StorageMode != StorageMode.Lsm)
            || engine.Changes.Enabled)
        {
            return false;
        }

        string databaseName = ResolveCurrentDatabase(context);
        UpdateFastPathSchema schema = ResolveUpdateFastPathSchema(engine, plan, databaseName, trustCachedSchema: false);
        if (!schema.Eligible
            || !engine.IndexManager.HasIntegerPrimaryKeyFastLane(schema.PkIndexName, plan.TableName, databaseName))
        {
            return false;
        }

        object? whereValue = FindParameterValue(parameters, plan.WhereParameterName!);
        if (whereValue is null || !TryConvertToInt64(whereValue, out long primaryKey))
        {
            return false;
        }

        if (!engine.IndexManager.TryLookupIntegerPrimaryKey(primaryKey, schema.PkIndexName, plan.TableName, databaseName, out long rowId))
        {
            affected = 0; // No row matches the predicate — a legitimate zero-row update.
            return true;
        }

        if (engine.Config.StorageMode == StorageMode.Lsm)
        {
            MvccCoordinator.ValidateCanModifyRow(engine, databaseName, plan.TableName, rowId, null, "UPDATE");
            if (!TryPatchLsmFixedWidthRow(context, plan, schema, databaseName, rowId, parameters))
            {
                return false;
            }

            affected = 1;
            return true;
        }

        List<long> rowWriteLocks = engine.LockManager.AcquireRowWriteLocks(databaseName, plan.TableName, [rowId]);
        try
        {
            // Re-resolve under the lock: a concurrent update may have moved the row to a new id.
            if (!engine.IndexManager.TryLookupIntegerPrimaryKey(primaryKey, schema.PkIndexName, plan.TableName, databaseName, out rowId))
            {
                affected = 0;
                return true;
            }

            MvccCoordinator.ValidateCanModifyRow(engine, databaseName, plan.TableName, rowId, null, "UPDATE");

            byte[]? rowBytes = engine.StorageContext.TryReadRowBytes(plan.TableName, databaseName, rowId);
            if (rowBytes is null)
            {
                affected = 0;
                return true;
            }

            // Byte-patch each assigned fixed-width cell in place. A cell that cannot be patched (null slot,
            // null new value) aborts BEFORE any storage mutation, so the dictionary path can take over.
            for (int i = 0; i < schema.AssignOrdinals.Length; i++)
            {
                object? newValue = FindParameterValue(parameters, schema.AssignParameterNames[i]);
                if (newValue is null
                    || !RowSerializer.TryOverwriteFixedWidthCell(rowBytes, schema.Columns, schema.AssignOrdinals[i], newValue))
                {
                    return false;
                }
            }

            long statementTxId = MvccCoordinator.ResolveStatementTransactionId(engine, null);

            // Out-of-place: tombstone the old row, append the patched bytes as the new row.
            engine.StorageContext.DeleteFromTable([rowId], plan.TableName, databaseName);
            long newRowId = engine.StorageContext.InsertSerializedRow(rowBytes, plan.TableName, databaseName);

            MvccCoordinator.RegisterUpdateVersion(engine, databaseName, plan.TableName, rowId, newRowId, statementTxId);

            // The primary key is unchanged, so the fast-lane entry just re-points to the new row id (upsert).
            engine.IndexManager.InsertIntegerPrimaryKeys([(primaryKey, newRowId)], schema.PkIndexName, plan.TableName, databaseName);

            if (ImplicitWalCommit.IsEnabled(engine))
            {
                engine.CommitBinaryUpdateFrameThroughGroupCommit(databaseName, plan.TableName, rowId, rowBytes);
            }

            affected = 1;
            return true;
        }
        finally
        {
            engine.LockManager.ReleaseRowWriteLocks(databaseName, plan.TableName, rowWriteLocks);
        }
    }

    private static bool TryUpdateFixedWidthFastPath(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        ReadOnlySpan<DataVoFixedWidthQueryParameter> parameters,
        out int affected)
    {
        affected = 0;
        DataVoEngine engine = context.Engine;

        if (!engine.Config.EnableZeroAllocCompiledUpdate
            || (engine.Config.StorageMode != StorageMode.Disk && engine.Config.StorageMode != StorageMode.Lsm)
            || engine.Changes.Enabled)
        {
            return false;
        }

        string databaseName = ResolveCurrentDatabase(context);
        UpdateFastPathSchema schema = ResolveUpdateFastPathSchema(engine, plan, databaseName, trustCachedSchema: false);
        if (!schema.Eligible
            || !engine.IndexManager.HasIntegerPrimaryKeyFastLane(schema.PkIndexName, plan.TableName, databaseName))
        {
            return false;
        }

        if (!TryFindParameterValue(parameters, plan.WhereParameterName!, out DataVoFixedWidthValue whereValue)
            || !TryConvertToInt64(whereValue, out long primaryKey))
        {
            return false;
        }

        if (!engine.IndexManager.TryLookupIntegerPrimaryKey(primaryKey, schema.PkIndexName, plan.TableName, databaseName, out long rowId))
        {
            affected = 0;
            return true;
        }

        if (engine.Config.StorageMode == StorageMode.Lsm)
        {
            MvccCoordinator.ValidateCanModifyRow(engine, databaseName, plan.TableName, rowId, null, "UPDATE");
            if (!TryPatchLsmFixedWidthRow(context, plan, schema, databaseName, rowId, parameters))
            {
                return false;
            }

            affected = 1;
            return true;
        }

        engine.LockManager.AcquireRowWriteLock(databaseName, plan.TableName, rowId);
        try
        {
            if (!engine.IndexManager.TryLookupIntegerPrimaryKey(primaryKey, schema.PkIndexName, plan.TableName, databaseName, out rowId))
            {
                affected = 0;
                return true;
            }

            MvccCoordinator.ValidateCanModifyRow(engine, databaseName, plan.TableName, rowId, null, "UPDATE");

            byte[]? rowBytes = engine.StorageContext.TryReadRowBytes(plan.TableName, databaseName, rowId);
            if (rowBytes is null)
            {
                affected = 0;
                return true;
            }

            for (int i = 0; i < schema.AssignOrdinals.Length; i++)
            {
                if (!TryFindParameterValue(parameters, schema.AssignParameterNames[i], out DataVoFixedWidthValue newValue)
                    || !RowSerializer.TryOverwriteFixedWidthCell(rowBytes, schema.Columns, schema.AssignOrdinals[i], newValue))
                {
                    return false;
                }
            }

            long statementTxId = MvccCoordinator.ResolveStatementTransactionId(engine, null);
            engine.StorageContext.DeleteFromTable([rowId], plan.TableName, databaseName);
            long newRowId = engine.StorageContext.InsertSerializedRow(rowBytes, plan.TableName, databaseName);

            MvccCoordinator.RegisterUpdateVersion(engine, databaseName, plan.TableName, rowId, newRowId, statementTxId);
            engine.IndexManager.InsertIntegerPrimaryKeys([(primaryKey, newRowId)], schema.PkIndexName, plan.TableName, databaseName);

            if (ImplicitWalCommit.IsEnabled(engine))
            {
                engine.CommitBinaryUpdateFrameThroughGroupCommit(databaseName, plan.TableName, rowId, rowBytes);
            }

            affected = 1;
            return true;
        }
        finally
        {
            engine.LockManager.ReleaseRowWriteLock(databaseName, plan.TableName, rowId);
        }
    }

    private static bool TryUpdateFixedWidthFastPath(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        DataVoFixedWidthValue primaryKeyValue,
        ReadOnlySpan<DataVoFixedWidthValue> assignmentValues,
        out int affected)
    {
        affected = 0;
        DataVoEngine engine = context.Engine;

        if (!engine.Config.EnableZeroAllocCompiledUpdate
            || (engine.Config.StorageMode != StorageMode.Disk && engine.Config.StorageMode != StorageMode.Lsm)
            || engine.Changes.Enabled)
        {
            return false;
        }

        string databaseName = ResolveCurrentDatabase(context);
        UpdateFastPathSchema schema = ResolveUpdateFastPathSchema(engine, plan, databaseName, trustCachedSchema: true);
        if (!schema.Eligible
            || schema.AssignOrdinals.Length != assignmentValues.Length
            || !engine.IndexManager.HasIntegerPrimaryKeyFastLane(schema.PkIndexName, plan.TableName, databaseName)
            || !TryConvertToInt64(primaryKeyValue, out long primaryKey))
        {
            return false;
        }

        if (!engine.IndexManager.TryLookupIntegerPrimaryKey(primaryKey, schema.PkIndexName, plan.TableName, databaseName, out long rowId))
        {
            affected = 0;
            return true;
        }

        if (engine.Config.StorageMode == StorageMode.Lsm)
        {
            MvccCoordinator.ValidateCanModifyRow(engine, databaseName, plan.TableName, rowId, null, "UPDATE");
            if (!context.Engine.StorageContext.TryPatchFixedWidthRow(
                plan.TableName,
                databaseName,
                rowId,
                schema.Columns,
                schema.AssignOrdinals,
                assignmentValues))
            {
                return false;
            }

            affected = 1;
            return true;
        }

        engine.LockManager.AcquireRowWriteLock(databaseName, plan.TableName, rowId);
        try
        {
            if (!engine.IndexManager.TryLookupIntegerPrimaryKey(primaryKey, schema.PkIndexName, plan.TableName, databaseName, out rowId))
            {
                affected = 0;
                return true;
            }

            MvccCoordinator.ValidateCanModifyRow(engine, databaseName, plan.TableName, rowId, null, "UPDATE");

            byte[]? rowBytes = engine.StorageContext.TryReadRowBytes(plan.TableName, databaseName, rowId);
            if (rowBytes is null)
            {
                affected = 0;
                return true;
            }

            for (int i = 0; i < schema.AssignOrdinals.Length; i++)
            {
                if (!RowSerializer.TryOverwriteFixedWidthCell(rowBytes, schema.Columns, schema.AssignOrdinals[i], assignmentValues[i]))
                {
                    return false;
                }
            }

            long statementTxId = MvccCoordinator.ResolveStatementTransactionId(engine, null);
            engine.StorageContext.DeleteFromTable([rowId], plan.TableName, databaseName);
            long newRowId = engine.StorageContext.InsertSerializedRow(rowBytes, plan.TableName, databaseName);

            MvccCoordinator.RegisterUpdateVersion(engine, databaseName, plan.TableName, rowId, newRowId, statementTxId);
            engine.IndexManager.InsertIntegerPrimaryKeys([(primaryKey, newRowId)], schema.PkIndexName, plan.TableName, databaseName);

            if (ImplicitWalCommit.IsEnabled(engine))
            {
                engine.CommitBinaryUpdateFrameThroughGroupCommit(databaseName, plan.TableName, rowId, rowBytes);
            }

            affected = 1;
            return true;
        }
        finally
        {
            engine.LockManager.ReleaseRowWriteLock(databaseName, plan.TableName, rowId);
        }
    }

    private static bool TryPatchLsmFixedWidthRow(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        UpdateFastPathSchema schema,
        string databaseName,
        long rowId,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters)
    {
        Span<DataVoFixedWidthValue> values = schema.AssignOrdinals.Length <= 16
            ? stackalloc DataVoFixedWidthValue[schema.AssignOrdinals.Length]
            : new DataVoFixedWidthValue[schema.AssignOrdinals.Length];

        for (int i = 0; i < schema.AssignOrdinals.Length; i++)
        {
            object? newValue = FindParameterValue(parameters, schema.AssignParameterNames[i]);
            if (newValue is null)
            {
                return false;
            }

            if (!DataVoFixedWidthValue.TryFromObject(newValue, out values[i]))
            {
                return false;
            }
        }

        return context.Engine.StorageContext.TryPatchFixedWidthRow(
            plan.TableName,
            databaseName,
            rowId,
            schema.Columns,
            schema.AssignOrdinals,
            values);
    }

    private static bool TryPatchLsmFixedWidthRow(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        UpdateFastPathSchema schema,
        string databaseName,
        long rowId,
        ReadOnlySpan<DataVoFixedWidthQueryParameter> parameters)
    {
        Span<DataVoFixedWidthValue> values = schema.AssignOrdinals.Length <= 16
            ? stackalloc DataVoFixedWidthValue[schema.AssignOrdinals.Length]
            : new DataVoFixedWidthValue[schema.AssignOrdinals.Length];

        for (int i = 0; i < schema.AssignOrdinals.Length; i++)
        {
            if (!TryFindParameterValue(parameters, schema.AssignParameterNames[i], out DataVoFixedWidthValue newValue))
            {
                return false;
            }

            values[i] = newValue;
        }

        return context.Engine.StorageContext.TryPatchFixedWidthRow(
            plan.TableName,
            databaseName,
            rowId,
            schema.Columns,
            schema.AssignOrdinals,
            values);
    }

    private static UpdateFastPathSchema ResolveUpdateFastPathSchema(
        DataVoEngine engine,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        bool trustCachedSchema)
    {
        var key = (plan, databaseName);
        if (trustCachedSchema && UpdateFastPathSchemaCache.TryGetValue(key, out UpdateFastPathSchema? cachedFastPath))
        {
            return cachedFastPath;
        }

        int schemaVersion = engine.Catalog.GetTableSchemaVersion(plan.TableName, databaseName);
        if (UpdateFastPathSchemaCache.TryGetValue(key, out UpdateFastPathSchema? cached)
            && cached.SchemaVersion == schemaVersion)
        {
            return cached;
        }

        UpdateFastPathSchema schema = BuildUpdateFastPathSchema(engine, plan, databaseName, schemaVersion);
        UpdateFastPathSchemaCache[key] = schema;
        return schema;
    }

    private static UpdateFastPathSchema BuildUpdateFastPathSchema(DataVoEngine engine, DataVoCompiledQueryPlan plan, string databaseName, int schemaVersion)
    {
        UpdateFastPathSchema Ineligible() => new() { SchemaVersion = schemaVersion, Eligible = false };

        IReadOnlyList<Column> columns = engine.Catalog.GetTableColumns(plan.TableName, databaseName);

        // The predicate must be the table's single integer primary key.
        List<string> primaryKeys = engine.Catalog.GetTablePrimaryKeys(plan.TableName, databaseName);
        if (primaryKeys.Count != 1 || !string.Equals(primaryKeys[0], plan.WhereColumn, StringComparison.OrdinalIgnoreCase))
        {
            return Ineligible();
        }

        int primaryKeyOrdinal = IndexOfColumn(columns, primaryKeys[0]);
        if (primaryKeyOrdinal < 0 || !RowSerializer.IsIntegerType(columns[primaryKeyOrdinal].Type))
        {
            return Ineligible();
        }

        // Only the primary-key index may exist: a secondary index would need key maintenance across the
        // out-of-place row move that the fast path does not perform.
        string primaryKeyIndexName = $"_PK_{plan.TableName}";
        foreach (IndexFile index in engine.Catalog.GetTableIndexes(plan.TableName, databaseName))
        {
            if (!string.Equals(index.IndexFileName, primaryKeyIndexName, StringComparison.OrdinalIgnoreCase))
            {
                return Ineligible();
            }
        }

        List<string> uniqueKeys = engine.Catalog.GetTableUniqueKeys(plan.TableName, databaseName);
        List<ForeignKey> foreignKeys = engine.Catalog.GetTableForeignKeys(plan.TableName, databaseName);

        var ordinals = new int[plan.Assignments.Count];
        var parameterNames = new string[plan.Assignments.Count];
        int next = 0;
        foreach ((string columnName, string parameterName) in plan.Assignments)
        {
            int ordinal = IndexOfColumn(columns, columnName);

            // Assigned columns must be non-key, fixed-width, and free of unique/foreign-key constraints so no
            // validation or index-key maintenance is required.
            if (ordinal < 0
                || ordinal == primaryKeyOrdinal
                || !RowSerializer.IsFixedWidthType(columns[ordinal].Type)
                || uniqueKeys.Contains(columnName, StringComparer.OrdinalIgnoreCase)
                || foreignKeys.Any(foreignKey => string.Equals(foreignKey.AttributeName, columnName, StringComparison.OrdinalIgnoreCase)))
            {
                return Ineligible();
            }

            ordinals[next] = ordinal;
            parameterNames[next] = parameterName;
            next++;
        }

        return new UpdateFastPathSchema
        {
            SchemaVersion = schemaVersion,
            Eligible = true,
            PkIndexName = primaryKeyIndexName,
            Columns = columns,
            AssignOrdinals = ordinals,
            AssignParameterNames = parameterNames,
        };
    }

    private static int IndexOfColumn(IReadOnlyList<Column> columns, string columnName)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            if (string.Equals(columns[i].Name, columnName, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    private static object? FindParameterValue(IReadOnlyList<DataVoCompiledQueryParameter> parameters, string name)
    {
        for (int i = 0; i < parameters.Count; i++)
        {
            if (string.Equals(parameters[i].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return parameters[i].Value;
            }
        }

        return null;
    }

    private static bool TryFindParameterValue(
        ReadOnlySpan<DataVoFixedWidthQueryParameter> parameters,
        string name,
        out DataVoFixedWidthValue value)
    {
        for (int i = 0; i < parameters.Length; i++)
        {
            if (string.Equals(parameters[i].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                value = parameters[i].Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    private static bool TryConvertToInt64(object value, out long result)
    {
        switch (value)
        {
            case long l: result = l; return true;
            case int i: result = i; return true;
            case short s: result = s; return true;
            case byte b: result = b; return true;
            default:
                try
                {
                    result = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                    return true;
                }
                catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
                {
                    result = 0;
                    return false;
                }
        }
    }

    /// <summary>Resolved metadata that makes a compiled UPDATE eligible for the zero-allocation byte-patch path.</summary>
    private sealed class UpdateFastPathSchema
    {
        public required int SchemaVersion { get; init; }

        public required bool Eligible { get; init; }

        public string PkIndexName { get; init; } = string.Empty;

        public IReadOnlyList<Column> Columns { get; init; } = [];

        public int[] AssignOrdinals { get; init; } = [];

        public string[] AssignParameterNames { get; init; } = [];
    }

    private static IReadOnlyList<TResult> ExecuteSelect<TResult>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        Func<Dictionary<string, object?>, TResult> mapper)
    {
        string databaseName = ResolveCurrentDatabase(context);
        Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
        object? expected = RequiredParameter(parameterDictionary, plan.WhereParameterName!);
        string expectedKey = BuildComparisonKey(plan.WhereColumn!, expected);

        List<Dictionary<string, object?>> rows = TryReadMatchingRowEntries(context, plan, databaseName, expectedKey)
            .Select(static pair => pair.Value)
            .ToList();

        return rows
            .Select(ProjectRowIfNeeded)
            .Select(mapper)
            .ToArray();

        Dictionary<string, object?> ProjectRowIfNeeded(Dictionary<string, object?> row)
        {
            if (plan.ProjectedColumns.Count == 0)
            {
                return row;
            }

            var projected = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (string column in plan.ProjectedColumns)
            {
                projected[column] = row.TryGetValue(column, out object? value) ? value : null;
            }

            return projected;
        }
    }

    private static List<KeyValuePair<long, Dictionary<string, object?>>> TryReadMatchingRowEntries(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string expectedKey)
    {
        return TryReadMatchingStoredRows(context, plan, databaseName, expectedKey)
            .Select(static entry => new KeyValuePair<long, Dictionary<string, object?>>(
                entry.Key,
                MaterializeStoredRow(entry.Value)))
            .ToList();
    }

    // Returns the matching row ids when an index access path resolves them (compile-time tag, then primary key,
    // then a single-column secondary index); returns null to signal the caller should use the full-decode scan
    // path. Mirrors the fall-through behavior of TryReadMatchingStoredRows (empty result or IndexException on an
    // index path falls through to scan).
    private static IReadOnlyList<long>? TryResolveMatchingRowIds(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string expectedKey)
    {
        // Returns FilterUsingIndex's read-only row-id view directly (no List/HashSet copy); the caller only enumerates it once.
        if (plan.AccessPath == CompiledAccessPath.SingleColumnIndex && plan.ResolvedIndexName is not null)
        {
            try
            {
                IReadOnlyList<long> ids = context.Engine.IndexManager.FilterUsingIndex(expectedKey, plan.ResolvedIndexName, plan.TableName, databaseName);
                if (ids.Count > 0)
                {
                    return ids;
                }
            }
            catch (IndexException)
            {
            }
        }

        List<string> primaryKeys = context.Engine.Catalog.GetTablePrimaryKeys(plan.TableName, databaseName);
        if (primaryKeys.Contains(plan.WhereColumn!, StringComparer.OrdinalIgnoreCase))
        {
            string primaryKeyIndexName = $"_PK_{plan.TableName}";
            try
            {
                IReadOnlyList<long> ids = context.Engine.IndexManager.FilterUsingIndex(expectedKey, primaryKeyIndexName, plan.TableName, databaseName);
                if (ids.Count > 0)
                {
                    return ids;
                }
            }
            catch (IndexException ex) when (IsMissingPrimaryKeyIndex(ex, primaryKeyIndexName, plan.TableName))
            {
            }
        }
        else if (TryResolveSingleColumnIndex(context, plan.TableName, databaseName, plan.WhereColumn!, out string secondaryIndexName))
        {
            try
            {
                IReadOnlyList<long> ids = context.Engine.IndexManager.FilterUsingIndex(expectedKey, secondaryIndexName, plan.TableName, databaseName);
                if (ids.Count > 0)
                {
                    return ids;
                }
            }
            catch (IndexException)
            {
            }
        }

        return null;
    }

    // Shared finder: resolves matching rows via the compile-time tag, then primary key, then a single-column
    // secondary index, then a typed full scan — returning the StoredRow itself so callers choose how to read it
    // (dictionary materialization for the legacy path, typed projection for the compiled path). Behavior is
    // identical to the previous TryReadMatchingRowEntries; only the return element type changed (StoredRow
    // instead of an already-materialized dictionary).
    private static List<KeyValuePair<long, StoredRow>> TryReadMatchingStoredRows(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string expectedKey)
    {
        // Compile-time fast path: a generator-resolved single-column index skips the per-call primary-key and
        // index catalog lookups below. A wrong/missing tag (IndexException) or an empty result falls through to
        // the runtime resolution, so correctness never depends on the compile-time bet being right.
        if (plan.AccessPath == CompiledAccessPath.SingleColumnIndex && plan.ResolvedIndexName is not null)
        {
            try
            {
                List<KeyValuePair<long, StoredRow>> tagged =
                    ReadStoredRowsViaIndex(context, plan, databaseName, plan.ResolvedIndexName, expectedKey);

                if (tagged.Count > 0)
                {
                    return tagged;
                }
            }
            catch (IndexException)
            {
            }
        }

        List<string> primaryKeys = context.Engine.Catalog.GetTablePrimaryKeys(plan.TableName, databaseName);
        bool isPrimaryKeyPredicate = primaryKeys.Contains(plan.WhereColumn!, StringComparer.OrdinalIgnoreCase);

        if (isPrimaryKeyPredicate)
        {
            string primaryKeyIndexName = $"_PK_{plan.TableName}";

            try
            {
                List<KeyValuePair<long, StoredRow>> matches =
                    ReadStoredRowsViaIndex(context, plan, databaseName, primaryKeyIndexName, expectedKey);

                if (matches.Count > 0)
                {
                    return matches;
                }
            }
            catch (IndexException ex) when (IsMissingPrimaryKeyIndex(ex, primaryKeyIndexName, plan.TableName))
            {
            }
        }
        else if (TryResolveSingleColumnIndex(context, plan.TableName, databaseName, plan.WhereColumn!, out string secondaryIndexName))
        {
            // A non-primary-key equality predicate covered by a single-column secondary (or unique) index
            // routes through that index instead of scanning the whole table — this is the O(n) -> O(log n)
            // path that mirrors the interpreted runtime (StatementEvaluatorWOJoin.HandleIndexableStatement).
            // An empty or unhealthy index falls through to the typed full scan so results stay correct even
            // when the index is stale, matching the primary-key branch's behavior.
            try
            {
                List<KeyValuePair<long, StoredRow>> matches =
                    ReadStoredRowsViaIndex(context, plan, databaseName, secondaryIndexName, expectedKey);

                if (matches.Count > 0)
                {
                    return matches;
                }
            }
            catch (IndexException)
            {
            }
        }

        Dictionary<long, StoredRow> scanned =
            context.Engine.StorageContext.GetTypedTableContents(plan.TableName, databaseName);

        // Filter typed (ordinal access + typed key extraction) so only matching rows are kept — non-matching
        // scanned rows are skipped without any materialization.
        string[] whereColumns = [plan.WhereColumn!];
        var scannedMatches = new List<KeyValuePair<long, StoredRow>>();
        foreach ((long rowId, StoredRow row) in scanned)
        {
            StoredRowView view = row.AsView();
            if (!view.Schema.TryGetOrdinal(plan.WhereColumn!, out _))
            {
                continue;
            }

            if (!string.Equals(
                    IndexKeyEncoder.BuildKeyString(view.Schema, view.Cells, whereColumns),
                    expectedKey,
                    StringComparison.Ordinal))
            {
                continue;
            }

            scannedMatches.Add(new KeyValuePair<long, StoredRow>(rowId, row));
        }

        return scannedMatches;
    }

    /// <summary>
    /// Reads the StoredRows whose IDs the named B-Tree index returns for <paramref name="expectedKey"/>.
    /// Shared by the tag, primary-key, and secondary-index access paths.
    /// </summary>
    private static List<KeyValuePair<long, StoredRow>> ReadStoredRowsViaIndex(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string indexName,
        string expectedKey)
    {
        List<long> ids =
        [
            .. context.Engine.IndexManager.FilterUsingIndex(expectedKey, indexName, plan.TableName, databaseName)
        ];

        Dictionary<long, StoredRow> indexedRows =
            context.Engine.StorageContext.GetTypedTableContents(ids, plan.TableName, databaseName);

        return ids
            .Where(indexedRows.ContainsKey)
            .Select(id => new KeyValuePair<long, StoredRow>(id, indexedRows[id]))
            .ToList();
    }

    /// <summary>
    /// Finds a single-column scalar B-Tree index (secondary or unique) that exactly covers
    /// <paramref name="whereColumn"/>. Only single-column indexes qualify because the compiled plan builds a
    /// single-column comparison key; a composite index keys on the concatenation of its columns and would not
    /// match. Vector indexes are excluded — they do not answer equality predicates.
    /// </summary>
    private static bool TryResolveSingleColumnIndex(
        DataVoContext context,
        string tableName,
        string databaseName,
        string whereColumn,
        out string indexName)
    {
        foreach (IndexFile index in context.Engine.Catalog.GetTableIndexes(tableName, databaseName))
        {
            if (index.AttributeNames.Count == 1
                && !string.IsNullOrWhiteSpace(index.IndexFileName)
                && index.AttributeNames[0].Equals(whereColumn, StringComparison.OrdinalIgnoreCase)
                && !context.Engine.IndexManager.SupportsVectorIndexType(index.IndexKind))
            {
                indexName = index.IndexFileName;
                return true;
            }
        }

        indexName = string.Empty;
        return false;
    }

    private static Dictionary<string, object?> MaterializeStoredRow(StoredRow row)
    {
        StoredRowView view = row.AsView();
        var result = new Dictionary<string, object?>(view.Count, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < view.Count; i++)
        {
            result[view.Schema.ColumnAt(i)] = view[i].ToObject();
        }

        return result;
    }

    private static List<long> RevalidateMatchingRowIdsAfterLock(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string expectedKey,
        IReadOnlyList<long> originalCandidateRowIds,
        IReadOnlyList<long> lockCoveredRowIds)
    {
        if (originalCandidateRowIds.Count == 0)
        {
            return [];
        }

        HashSet<long> originalCandidates = [.. originalCandidateRowIds];
        HashSet<long> lockCovered = [.. lockCoveredRowIds];

        return TryReadMatchingRowEntries(context, plan, databaseName, expectedKey)
            .Select(static pair => pair.Key)
            .Where(originalCandidates.Contains)
            .Where(rowId => rowId <= 0 || lockCovered.Count == 0 || lockCovered.Contains(rowId))
            .ToList();
    }

    private static bool IsMissingPrimaryKeyIndex(IndexException exception, string indexName, string tableName)
    {
        return exception.Message.Contains($"Index {indexName} on table {tableName} does not exist", StringComparison.OrdinalIgnoreCase);
    }

    private static Dictionary<string, object?> ToParameterDictionary(IReadOnlyList<DataVoCompiledQueryParameter> parameters)
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (DataVoCompiledQueryParameter parameter in parameters)
        {
            if (string.IsNullOrWhiteSpace(parameter.Name))
            {
                throw new ArgumentException("Compiled query parameter names cannot be null or whitespace.", nameof(parameters));
            }

            if (result.ContainsKey(parameter.Name))
            {
                throw new ArgumentException($"Duplicate compiled query parameter '{parameter.Name}'.", nameof(parameters));
            }

            result[parameter.Name] = parameter.Value;
        }

        return result;
    }

    private static object? RequiredParameter(IReadOnlyDictionary<string, object?> parameters, string parameterName)
    {
        if (!parameters.TryGetValue(parameterName, out object? value))
        {
            throw new ArgumentException($"Missing compiled query parameter '{parameterName}'.", nameof(parameters));
        }

        return value;
    }

    private static object? RequiredSingleParameterValue(
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        string parameterName)
    {
        bool found = false;
        object? value = null;

        for (int i = 0; i < parameters.Count; i++)
        {
            DataVoCompiledQueryParameter parameter = parameters[i];
            if (string.IsNullOrWhiteSpace(parameter.Name))
            {
                throw new ArgumentException("Compiled query parameter names cannot be null or whitespace.", nameof(parameters));
            }

            if (!string.Equals(parameter.Name, parameterName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (found)
            {
                throw new ArgumentException($"Duplicate compiled query parameter '{parameterName}'.", nameof(parameters));
            }

            found = true;
            value = parameter.Value;
        }

        if (!found)
        {
            throw new ArgumentException($"Missing compiled query parameter '{parameterName}'.", nameof(parameters));
        }

        return value;
    }

    private static string ResolveCurrentDatabase(DataVoContext context)
    {
        string? databaseName = context.Engine.Sessions.Get(context.SessionId);
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new InvalidOperationException("No database selected for the current session. Execute USE <database> first.");
        }

        return databaseName;
    }

    private static Dictionary<string, Column> GetTableColumnsByName(DataVoContext context, string tableName, string databaseName)
    {
        return context.Engine.Catalog
            .GetTableColumns(tableName, databaseName)
            .ToDictionary(
                column => column.Name,
                column => new Column
                {
                    Name = column.Name,
                    Type = column.Type,
                    Length = column.Length,
                    DefaultValue = column.DefaultValue
                },
                StringComparer.OrdinalIgnoreCase);
    }

    private static Dictionary<string, object?> ApplyAssignments(
        IReadOnlyDictionary<string, object?> oldRow,
        DataVoCompiledQueryPlan plan,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlyDictionary<string, Column> columnsByName)
    {
        var newRow = new Dictionary<string, object?>(oldRow, StringComparer.OrdinalIgnoreCase);

        foreach ((string columnName, string parameterName) in plan.Assignments)
        {
            if (!columnsByName.TryGetValue(columnName, out Column? column))
            {
                throw new BindingException($"Column {columnName} doesn't exist in table {plan.TableName}!");
            }

            object? rawValue = RequiredParameter(parameters, parameterName);
            newRow[columnName] = NormalizeAssignedValue(column, rawValue);
        }

        return newRow;
    }

    private static object? NormalizeAssignedValue(Column columnMetadata, object? rawValue)
    {
        if (rawValue == null || rawValue is DBNull)
        {
            return null;
        }

        var column = new Column
        {
            Name = columnMetadata.Name,
            Type = columnMetadata.Type,
            Length = columnMetadata.Length,
            DefaultValue = columnMetadata.DefaultValue
        };

        string formattedValue = FormatColumnValue(rawValue, column.Type);
        column.Value = formattedValue;

        object? parsedValue = column.ParsedValue;
        if (!IsParsedValueCompatible(column, parsedValue))
        {
            throw new EvaluationException($"Type of argument doesn't match with column type for {column.Name}!");
        }

        return NormalizeParsedValue(column, parsedValue);
    }

    private static string FormatColumnValue(object rawValue, string columnType)
    {
        if (rawValue is string text)
        {
            return text;
        }

        if (columnType.Equals("VECTOR", StringComparison.OrdinalIgnoreCase)
            && VectorParser.TryCoerceToVector(rawValue, out float[] vector))
        {
            return VectorParser.SerializeVector(vector);
        }

        if (rawValue is DateOnly dateOnly)
        {
            return dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        if (rawValue is DateTime dateTime && columnType.Equals("DATE", StringComparison.OrdinalIgnoreCase))
        {
            return DateOnly.FromDateTime(dateTime).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        if (rawValue is IFormattable formattable)
        {
            return formattable.ToString(null, CultureInfo.InvariantCulture);
        }

        return Convert.ToString(rawValue, CultureInfo.InvariantCulture) ?? "null";
    }

    private static bool IsParsedValueCompatible(Column column, object? parsedValue)
    {
        if (parsedValue == null)
        {
            return true;
        }

        return column.Type.ToUpperInvariant() switch
        {
            "INT" => parsedValue is int,
            "FLOAT" => parsedValue is double,
            "BIT" => parsedValue is bool,
            "DATE" => parsedValue is DateOnly,
            "VECTOR" => parsedValue is float[] vector && (column.Length <= 0 || vector.Length == column.Length),
            _ => true
        };
    }

    private static object? NormalizeParsedValue(Column column, object? parsedValue)
    {
        if (parsedValue is float[] vector)
        {
            return vector.ToArray();
        }

        if (column.Type.Equals("DATE", StringComparison.OrdinalIgnoreCase) && parsedValue is DateTime dateTime)
        {
            return DateOnly.FromDateTime(dateTime);
        }

        return parsedValue;
    }

    private static void ValidateUpdatedRows(
        DataVoContext context,
        string tableName,
        string databaseName,
        IReadOnlyList<long> rowIds,
        IReadOnlyDictionary<long, Dictionary<string, object?>> existingRows,
        IReadOnlyList<Dictionary<string, object?>> newRows)
    {
        List<string> primaryKeys = context.Engine.Catalog.GetTablePrimaryKeys(tableName, databaseName);
        List<string> uniqueKeys = context.Engine.Catalog.GetTableUniqueKeys(tableName, databaseName);
        List<ForeignKey> foreignKeys = context.Engine.Catalog.GetTableForeignKeys(tableName, databaseName);
        List<(string ChildTable, string ChildColumn, string ParentColumn, string OnDeleteAction)> childForeignKeys =
            context.Engine.Catalog.GetChildForeignKeys(tableName, databaseName);

        Dictionary<string, HashSet<string>> batchUniqueValues = InitializeBatchUniqueTracker(primaryKeys, uniqueKeys);

        for (int i = 0; i < rowIds.Count; i++)
        {
            long rowId = rowIds[i];
            Dictionary<string, object?> oldRow = existingRows[rowId];
            Dictionary<string, object?> newRow = newRows[i];
            int rowNumber = i + 1;

            ValidatePrimaryAndUniqueConstraints(context, tableName, databaseName, newRow, oldRow, primaryKeys, uniqueKeys, batchUniqueValues, rowNumber);
            ValidateForeignKeyConstraints(context, databaseName, newRow, oldRow, foreignKeys, rowNumber);
            ValidateChildForeignKeyConstraints(context, tableName, databaseName, newRow, oldRow, childForeignKeys);
        }
    }

    private static Dictionary<string, HashSet<string>> InitializeBatchUniqueTracker(
        IReadOnlyList<string> primaryKeys,
        IReadOnlyList<string> uniqueKeys)
    {
        Dictionary<string, HashSet<string>> batchUniqueValues = new(StringComparer.OrdinalIgnoreCase);
        foreach (string columnName in primaryKeys.Concat(uniqueKeys))
        {
            batchUniqueValues[columnName] = new HashSet<string>(StringComparer.Ordinal);
        }

        return batchUniqueValues;
    }

    private static void ValidatePrimaryAndUniqueConstraints(
        DataVoContext context,
        string tableName,
        string databaseName,
        Dictionary<string, object?> newRow,
        IReadOnlyDictionary<string, object?> oldRow,
        IReadOnlyList<string> primaryKeys,
        IReadOnlyList<string> uniqueKeys,
        IReadOnlyDictionary<string, HashSet<string>> batchUniqueValues,
        int rowNumber)
    {
        foreach (string columnName in primaryKeys.Concat(uniqueKeys))
        {
            if (!newRow.TryGetValue(columnName, out object? value))
            {
                continue;
            }

            if (value == null && primaryKeys.Contains(columnName, StringComparer.OrdinalIgnoreCase))
            {
                throw new EvaluationException($"Constraint violation: Primary key column {columnName} cannot be null in row {rowNumber}.");
            }

            if (value == null)
            {
                continue;
            }

            string valueText = value.ToString()!;
            string oldValueText = oldRow.TryGetValue(columnName, out object? oldValue)
                ? oldValue?.ToString() ?? "null_val"
                : "null_val";

            if (valueText == oldValueText)
            {
                continue;
            }

            if (!batchUniqueValues[columnName].Add(valueText))
            {
                throw new EvaluationException($"Update conflict: Duplicate value '{valueText}' generated within the same batch for unique column {columnName}.");
            }

            string indexName = primaryKeys.Contains(columnName, StringComparer.OrdinalIgnoreCase)
                ? $"_PK_{tableName}"
                : $"_UK_{columnName}";

            bool duplicateExists;
            try
            {
                duplicateExists = context.Engine.IndexManager.IndexContainsKey(valueText, indexName, tableName, databaseName);
            }
            catch
            {
                duplicateExists = context.Engine.StorageContext.GetTableContents(tableName, databaseName)
                    .Any(entry => entry.Value.TryGetValue(columnName, out object? existing)
                                  && existing != null
                                  && string.Equals(existing.ToString(), valueText, StringComparison.Ordinal));
            }

            if (duplicateExists)
            {
                throw new EvaluationException($"Constraint violation: Duplicate value '{valueText}' for unique column {columnName} in row {rowNumber}.");
            }
        }
    }

    private static bool TryConvertToInt64(DataVoFixedWidthValue value, out long result)
    {
        switch (value.Type)
        {
            case DataVoFixedWidthValueType.Int32:
                result = value.AsInt32();
                return true;
            case DataVoFixedWidthValueType.Int64:
                result = value.AsInt64();
                return true;
            default:
                result = 0;
                return false;
        }
    }

    private static void ValidateForeignKeyConstraints(
        DataVoContext context,
        string databaseName,
        Dictionary<string, object?> newRow,
        IReadOnlyDictionary<string, object?> oldRow,
        IReadOnlyList<ForeignKey> foreignKeys,
        int rowNumber)
    {
        foreach (ForeignKey foreignKey in foreignKeys)
        {
            if (!newRow.TryGetValue(foreignKey.AttributeName, out object? foreignKeyValue))
            {
                continue;
            }

            string newValueText = foreignKeyValue?.ToString() ?? "null";
            string oldValueText = oldRow.TryGetValue(foreignKey.AttributeName, out object? oldValue)
                ? oldValue?.ToString() ?? "null"
                : "null";

            if (newValueText == oldValueText || newValueText == "null")
            {
                continue;
            }

            if (!CheckForeignKeyConstraint(context, foreignKey, newValueText, databaseName))
            {
                throw new EvaluationException($"Foreign key violation: Value '{newValueText}' does not reference an existing parent row for foreign key column {foreignKey.AttributeName} in row {rowNumber}.");
            }
        }
    }

    private static void ValidateChildForeignKeyConstraints(
        DataVoContext context,
        string tableName,
        string databaseName,
        Dictionary<string, object?> newRow,
        IReadOnlyDictionary<string, object?> oldRow,
        IReadOnlyList<(string ChildTable, string ChildColumn, string ParentColumn, string OnDeleteAction)> childForeignKeys)
    {
        foreach ((string childTable, string childColumn, string parentColumn, _) in childForeignKeys)
        {
            string oldParentValue = oldRow.TryGetValue(parentColumn, out object? oldValue)
                ? oldValue?.ToString() ?? "null"
                : "null";
            string newParentValue = newRow.TryGetValue(parentColumn, out object? newValue)
                ? newValue?.ToString() ?? "null"
                : "null";

            if (oldParentValue == newParentValue || oldParentValue == "null")
            {
                continue;
            }

            string childIndexName = $"_FK_{childTable}_{childColumn}";
            List<long> childRowIds;

            try
            {
                childRowIds = context.Engine.IndexManager.FilterUsingIndex(oldParentValue, childIndexName, childTable, databaseName).ToList();
            }
            catch
            {
                childRowIds = FindChildRowsByTableScan(context, childTable, childColumn, oldParentValue, databaseName);
            }

            childRowIds = childRowIds
                .Where(id => id != 0 && context.Engine.StorageContext.TableContainsRow(id, childTable, databaseName))
                .ToList();

            if (childRowIds.Count > 0)
            {
                throw new EvaluationException($"Foreign key violation: Cannot update {parentColumn} ('{oldParentValue}' -> '{newParentValue}') in {tableName} because {childRowIds.Count} row(s) in {childTable} depend on it.");
            }
        }
    }

    private static bool CheckForeignKeyConstraint(
        DataVoContext context,
        ForeignKey foreignKey,
        string columnValue,
        string databaseName)
    {
        foreach (Reference reference in foreignKey.References)
        {
            if (!ReferenceExists(context, reference.ReferenceTableName, reference.ReferenceAttributeName, columnValue, databaseName))
            {
                return false;
            }
        }

        return true;
    }

    private static bool ReferenceExists(
        DataVoContext context,
        string tableName,
        string columnName,
        string expectedValue,
        string databaseName)
    {
        try
        {
            if (context.Engine.IndexManager.IndexContainsKey(expectedValue, $"_PK_{tableName}", tableName, databaseName))
            {
                return true;
            }
        }
        catch
        {
        }

        return context.Engine.StorageContext.GetTableContents(tableName, databaseName)
            .Values
            .Any(row => row.TryGetValue(columnName, out object? value)
                        && value != null
                        && string.Equals(value.ToString(), expectedValue, StringComparison.Ordinal));
    }

    private static List<long> FindChildRowsByTableScan(
        DataVoContext context,
        string childTable,
        string childColumn,
        string parentValue,
        string databaseName)
    {
        return context.Engine.StorageContext.GetTableContents(childTable, databaseName)
            .Where(pair => pair.Value.TryGetValue(childColumn, out object? value)
                           && value != null
                           && string.Equals(value.ToString(), parentValue, StringComparison.Ordinal))
            .Select(static pair => pair.Key)
            .ToList();
    }

    private static void ReplaceRows(
        DataVoContext context,
        string tableName,
        string databaseName,
        IReadOnlyList<long> oldRowIds,
        IReadOnlyList<Dictionary<string, object?>> oldRows,
        IReadOnlyList<Dictionary<string, object?>> newRows,
        long statementTxId)
    {
        List<IndexFile> indexFiles = context.Engine.Catalog.GetTableIndexes(tableName, databaseName);

        // Tombstone the old physical rows, then append the new versions and capture their row ids.
        context.Engine.StorageContext.DeleteFromTable(oldRowIds.ToList(), tableName, databaseName);

        var newRowIds = new long[newRows.Count];
        for (int i = 0; i < newRows.Count; i++)
        {
            long assignedRowId = context.Engine.StorageContext.InsertOneIntoTable(newRows[i], tableName, databaseName);
            newRowIds[i] = assignedRowId;
            MvccCoordinator.RegisterUpdateVersion(context.Engine, databaseName, tableName, oldRowIds[i], assignedRowId, statementTxId);
        }

        foreach (IndexFile index in indexFiles)
        {
            MaintainIndexForUpdate(context, tableName, databaseName, index, oldRowIds, oldRows, newRowIds, newRows);
        }

        if (ImplicitWalCommit.IsEnabled(context.Engine))
        {
            var operations = new List<WalOperation>(newRows.Count);
            for (int i = 0; i < newRows.Count; i++)
            {
                operations.Add(new WalOperation
                {
                    OperationType = WalOperationType.Update,
                    TableName = tableName,
                    RowId = oldRowIds[i],
                    UpdatedColumns = new Dictionary<string, object?>(newRows[i], newRows[i].Comparer),
                });
            }

            ImplicitWalCommit.CommitIfEnabled(context.Engine, databaseName, statementTxId, operations);
        }
    }

    /// <summary>
    /// Maintains one index across an UPDATE batch. Integer single-column indexes are kept on their O(1) fast lane
    /// (point-lookup parity with INSERT) so an updated row stays index-resolvable instead of falling into the
    /// full-table-scan fallback; other indexes use the delete-by-row-id + reinsert path. This is the fix for the
    /// concurrent-ops allocation trap where every UPDATE evicted its row from the fast lane.
    /// </summary>
    private static void MaintainIndexForUpdate(
        DataVoContext context,
        string tableName,
        string databaseName,
        IndexFile index,
        IReadOnlyList<long> oldRowIds,
        IReadOnlyList<Dictionary<string, object?>> oldRows,
        IReadOnlyList<long> newRowIds,
        IReadOnlyList<Dictionary<string, object?>> newRows)
    {
        string indexName = index.IndexFileName ?? string.Empty;
        if (string.IsNullOrWhiteSpace(indexName))
        {
            return;
        }

        var indexes = context.Engine.IndexManager;
        string indexKind = index.IndexKind ?? string.Empty;

        if (indexes.SupportsVectorIndexType(indexKind))
        {
            indexes.DeleteFromVectorIndex(oldRowIds.ToList(), indexName, tableName, databaseName, indexKind);
            for (int i = 0; i < newRows.Count; i++)
            {
                Dictionary<string, object?> newRow = newRows[i];
                if (index.AttributeNames.Count != 1)
                {
                    throw new BindingException($"Vector index '{indexName}' (type '{indexKind}') must reference exactly one VECTOR column.");
                }

                string vectorColumn = index.AttributeNames[0];
                if (!newRow.TryGetValue(vectorColumn, out object? vectorValue) || vectorValue is null)
                {
                    continue;
                }

                if (!VectorParser.TryCoerceToVector(vectorValue, out float[] vector))
                {
                    throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
                }

                indexes.InsertIntoVectorIndex(vector, newRowIds[i], indexName, tableName, databaseName, indexKind);
            }

            return;
        }

        bool isPrimaryKeyIndex = indexName.Equals($"_PK_{tableName}", StringComparison.OrdinalIgnoreCase);
        if (isPrimaryKeyIndex && indexes.HasIntegerPrimaryKeyFastLane(indexName, tableName, databaseName))
        {
            for (int i = 0; i < newRows.Count; i++)
            {
                bool oldOk = TryGetSingleIntegerKey(oldRows[i], index.AttributeNames, out long oldKey);
                bool newOk = TryGetSingleIntegerKey(newRows[i], index.AttributeNames, out long newKey);

                if (oldOk && (!newOk || oldKey != newKey))
                {
                    indexes.RemoveIntegerPrimaryKey(oldKey, indexName, tableName, databaseName);
                }

                if (newOk)
                {
                    // Upsert: overwrites the previous row id when the key is unchanged (gap-free for readers).
                    indexes.InsertIntegerPrimaryKeys([(newKey, newRowIds[i])], indexName, tableName, databaseName);
                }
            }

            return;
        }

        if (indexes.HasIntegerIndexFastLane(indexName, tableName, databaseName))
        {
            for (int i = 0; i < newRows.Count; i++)
            {
                if (TryGetSingleIntegerKey(oldRows[i], index.AttributeNames, out long oldKey))
                {
                    indexes.RemoveIntegerIndexEntry(oldKey, oldRowIds[i], indexName, tableName, databaseName);
                }

                if (TryGetSingleIntegerKey(newRows[i], index.AttributeNames, out long newKey))
                {
                    indexes.InsertIntegerIndexEntries([(newKey, newRowIds[i])], indexName, tableName, databaseName);
                }
            }

            return;
        }

        if (isPrimaryKeyIndex && indexes.HasGuidPrimaryKeyFastLane(indexName, tableName, databaseName))
        {
            for (int i = 0; i < newRows.Count; i++)
            {
                bool oldOk = TryGetSingleGuidKey(oldRows[i], index.AttributeNames, out Guid oldKey);
                bool newOk = TryGetSingleGuidKey(newRows[i], index.AttributeNames, out Guid newKey);

                if (oldOk && (!newOk || oldKey != newKey))
                {
                    indexes.RemoveGuidPrimaryKey(oldKey, indexName, tableName, databaseName);
                }

                if (newOk)
                {
                    indexes.InsertGuidPrimaryKeys([(newKey, newRowIds[i])], indexName, tableName, databaseName);
                }
            }

            return;
        }

        if (indexes.HasGuidIndexFastLane(indexName, tableName, databaseName))
        {
            for (int i = 0; i < newRows.Count; i++)
            {
                if (TryGetSingleGuidKey(oldRows[i], index.AttributeNames, out Guid oldKey))
                {
                    indexes.RemoveGuidIndexEntry(oldKey, oldRowIds[i], indexName, tableName, databaseName);
                }

                if (TryGetSingleGuidKey(newRows[i], index.AttributeNames, out Guid newKey))
                {
                    indexes.InsertGuidIndexEntries([(newKey, newRowIds[i])], indexName, tableName, databaseName);
                }
            }

            return;
        }

        // Generic scalar (string BTree) path: delete the old row ids, then reinsert non-null keys.
        indexes.DeleteFromIndex(oldRowIds.ToList(), indexName, tableName, databaseName);
        for (int i = 0; i < newRows.Count; i++)
        {
            Dictionary<string, object?> newRow = newRows[i];
            if (index.AttributeNames.Any(attributeName => !newRow.TryGetValue(attributeName, out object? attributeValue) || attributeValue == null))
            {
                continue;
            }

            string indexValue = IndexKeyEncoder.BuildKeyString(newRow, index.AttributeNames);
            indexes.InsertIntoIndex(indexValue, newRowIds[i], indexName, tableName, databaseName);
        }
    }

    private static bool TryGetSingleIntegerKey(
        IReadOnlyDictionary<string, object?> row,
        IReadOnlyList<string> attributes,
        out long key)
    {
        key = 0;
        if (attributes.Count != 1 || !row.TryGetValue(attributes[0], out object? value) || value is null)
        {
            return false;
        }

        switch (value)
        {
            case int intValue:
                key = intValue;
                return true;
            case long longValue:
                key = longValue;
                return true;
            default:
                return false;
        }
    }

    private static bool TryGetSingleGuidKey(
        IReadOnlyDictionary<string, object?> row,
        IReadOnlyList<string> attributes,
        out Guid key)
    {
        key = default;
        if (attributes.Count != 1 || !row.TryGetValue(attributes[0], out object? value) || value is null)
        {
            return false;
        }

        if (value is Guid guid)
        {
            key = guid;
            return true;
        }

        if (value is string text && Guid.TryParse(text, out Guid parsed))
        {
            key = parsed;
            return true;
        }

        return false;
    }

    private static string BuildComparisonKey(string columnName, object? value)
    {
        return BuildScalarComparisonKey(value);
    }

    internal static string BuildScalarComparisonKey(int value) =>
        string.Create(CultureInfo.InvariantCulture, $"[{value}]");

    internal static string BuildScalarComparisonKey(long value) =>
        string.Create(CultureInfo.InvariantCulture, $"[{value}]");

    internal static string BuildScalarComparisonKey(double value) =>
        string.Create(CultureInfo.InvariantCulture, $"[{value}]");

    internal static string BuildScalarComparisonKey(string? value)
    {
        if (value is null)
        {
            return string.Empty;
        }

        return VectorParser.TryParseVector(value, out float[] vector)
            ? VectorParser.SerializeVector(vector)
            : value;
    }

    internal static string BuildScalarComparisonKey(object? value)
    {
        return value switch
        {
            null => string.Empty,
            int intValue => BuildScalarComparisonKey(intValue),
            long longValue => BuildScalarComparisonKey(longValue),
            double doubleValue => BuildScalarComparisonKey(doubleValue),
            float floatValue => string.Create(CultureInfo.InvariantCulture, $"[{floatValue}]"),
            decimal decimalValue => string.Create(CultureInfo.InvariantCulture, $"[{decimalValue}]"),
            string stringValue => BuildScalarComparisonKey(stringValue),
            _ => VectorParser.TryCoerceToVector(value, out float[] vector)
                ? VectorParser.SerializeVector(vector)
                : value.ToString() ?? string.Empty
        };
    }
}
