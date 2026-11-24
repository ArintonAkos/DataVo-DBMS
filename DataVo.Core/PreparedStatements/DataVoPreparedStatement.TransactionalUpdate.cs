using DataVo.Core.Contracts.Results;
using DataVo.Core.Enums;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Statements.Mechanism;
using DataVo.Core.Parser.Utils;
using DataVo.Core.PreparedStatements;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;

namespace DataVo.Core;

/// <summary>
/// The in-transaction fast path for single-row primary-key updates with simple assignments, such as
/// <c>UPDATE standings SET Score = Score + @points WHERE PlayerId = @id</c>.
/// </summary>
/// <remarks>
/// <para>
/// Inside an explicit transaction an UPDATE only buffers the row's new values; COMMIT applies them through the
/// shared commit pipeline (MVCC conflict checks, the WAL, the LSM write batch, index maintenance and change
/// capture). The general path still re-binds the statement, re-resolves the table, re-reads constraint metadata
/// and evaluates the SET list through the generic expression evaluator on every execution. This path compiles
/// that work once per schema and does only the per-row part: an MVCC-aware primary-key lookup, the arithmetic
/// (through the same <see cref="ScalarEvaluator"/> operator semantics), and a buffered update. A row the
/// transaction already changed is updated in place in its buffer.
/// </para>
/// <para>
/// It applies to tables with a single-column integer primary key on the InMemory and LSM engines, where
/// assigned columns are numeric and take part in no primary, unique or foreign key. Anything else (including a
/// value that would need a constraint error) runs on the general path, which reports it.
/// </para>
/// </remarks>
public sealed partial class DataVoPreparedStatement
{
    private TransactionalUpdateCandidate? _candidateTransactionalUpdate;
    private volatile TransactionalUpdatePlan? _cachedTransactionalUpdatePlan;
    private long _transactionalUpdateExecutions;

    /// <summary>Gets how many executions the in-transaction update fast path completed (for tests and diagnostics).</summary>
    internal long TransactionalUpdateExecutions => Interlocked.Read(ref _transactionalUpdateExecutions);

    [ThreadStatic]
    private static object?[]? t_assignmentValues;

    /// <summary>How a SET clause computes its new value.</summary>
    private enum AssignmentKind : byte
    {
        Set,
        Add,
        Subtract,
    }

    /// <summary>One SET clause: the column, how it is computed and its operand (a parameter or a constant).</summary>
    private readonly record struct Assignment(string Column, AssignmentKind Kind, string? ParameterName, object? Constant);

    /// <summary>The statement shape, extracted once from the parsed template.</summary>
    private sealed class TransactionalUpdateCandidate
    {
        public required string TableName { get; init; }
        public required string WhereColumn { get; init; }
        public string? WhereParameterName { get; init; }
        public object? WhereConstant { get; init; }
        public required Assignment[] Assignments { get; init; }
    }

    /// <summary>The statement resolved against one database's catalog, valid until the catalog changes.</summary>
    private sealed class TransactionalUpdatePlan
    {
        public required string DatabaseName { get; init; }
        public required int SchemaVersion { get; init; }
        public required long CatalogGeneration { get; init; }
        public required bool Eligible { get; init; }
        public string StoredTable { get; init; } = string.Empty;
        public string PrimaryKeyColumn { get; init; } = string.Empty;
        public string PrimaryKeyIndex { get; init; } = string.Empty;
        public string[] AssignedColumns { get; init; } = [];
        public bool[] AssignedNotNull { get; init; } = [];
    }

    /// <summary>
    /// Extracts the fast-path shape from a parsed UPDATE: <c>WHERE key = @p</c> (or a constant) and SET clauses
    /// of the form <c>col = @p</c>, <c>col = constant</c>, <c>col = col + x</c>, <c>col = x + col</c> or
    /// <c>col = col - x</c>, where <c>x</c> is a parameter or a numeric constant.
    /// </summary>
    private static TransactionalUpdateCandidate? TryBuildTransactionalUpdateCandidate(UpdateStatement update)
    {
        if (update.TableName is null
            || string.IsNullOrWhiteSpace(update.TableName.Name)
            || update.SetClauses.Count == 0
            || update.WhereClause is not BinaryExpressionNode { Operator: Operators.EQUALS } where
            || !TryExtractSimpleEquality(where, out string whereColumn, out string? whereParameter, out object? whereConstant))
        {
            return null;
        }

        var assignments = new Assignment[update.SetClauses.Count];
        for (int i = 0; i < assignments.Length; i++)
        {
            SetClauseNode clause = update.SetClauses[i];
            if (!TryBuildAssignment(clause.ColumnName.Name, clause.Value, out assignments[i]))
            {
                return null;
            }
        }

        return new TransactionalUpdateCandidate
        {
            TableName = update.TableName.Name,
            WhereColumn = whereColumn,
            WhereParameterName = whereParameter,
            WhereConstant = whereConstant,
            Assignments = assignments,
        };
    }

    private static bool TryBuildAssignment(string column, ExpressionNode value, out Assignment assignment)
    {
        assignment = default;
        if (TryReadOperand(value, out string? parameter, out object? constant))
        {
            assignment = new Assignment(column, AssignmentKind.Set, parameter, constant);
            return true;
        }

        if (value is not BinaryExpressionNode binary
            || binary.Operator is not (Operators.ADD or Operators.SUBTRACT))
        {
            return false;
        }

        AssignmentKind kind = binary.Operator == Operators.ADD ? AssignmentKind.Add : AssignmentKind.Subtract;
        if (IsBareColumn(binary.Left, column) && TryReadOperand(binary.Right, out parameter, out constant))
        {
            assignment = new Assignment(column, kind, parameter, constant);
            return true;
        }

        // x + col: addition commutes for the numeric operands this path accepts.
        if (kind == AssignmentKind.Add && IsBareColumn(binary.Right, column) && TryReadOperand(binary.Left, out parameter, out constant))
        {
            assignment = new Assignment(column, kind, parameter, constant);
            return true;
        }

        return false;
    }

    // A parameter, or a non-null numeric constant (text constants keep the general path's quoting rules).
    private static bool TryReadOperand(ExpressionNode node, out string? parameter, out object? constant)
    {
        parameter = null;
        constant = null;
        switch (node)
        {
            case ParameterNode parameterNode:
                parameter = parameterNode.Name;
                return true;
            case LiteralNode literal when literal is not NullLiteralNode && IsNumeric(literal.Value):
                constant = literal.Value;
                return true;
            default:
                return false;
        }
    }

    private static bool IsBareColumn(ExpressionNode node, string column) =>
        node is ColumnRefNode reference
        && string.IsNullOrEmpty(reference.TableOrAlias)
        && reference.Column.Equals(column, StringComparison.OrdinalIgnoreCase);

    private static bool IsNumeric(object? value) =>
        value is int or long or short or byte or double or float or decimal;

    /// <summary>
    /// Runs the statement on the in-transaction fast path when it applies.
    /// </summary>
    /// <param name="transaction">The session's open transaction.</param>
    /// <param name="databaseName">The session's database.</param>
    /// <param name="parameters">The parameter bindings.</param>
    /// <param name="result">The statement result, when this returns <see langword="true"/>.</param>
    /// <returns><see langword="false"/> when the general path must run the statement instead.</returns>
    private bool TryExecuteTransactionalUpdate(
        TransactionContext transaction,
        string databaseName,
        ReadOnlySpan<DataVoQueryParameter> parameters,
        out QueryResult? result)
    {
        result = null;
        TransactionalUpdateCandidate candidate = _candidateTransactionalUpdate!;
        TransactionalUpdatePlan plan = GetTransactionalUpdatePlan(candidate, databaseName);
        if (!plan.Eligible
            || !TryBindOperand(candidate.WhereParameterName, candidate.WhereConstant, parameters, out object? keyValue)
            || keyValue is null
            || !SqlIndexLookupKey.TryBuildIntegerKey(keyValue, out string key))
        {
            return false;
        }

        object?[] operands = BindAssignmentOperands(candidate, parameters);
        if (operands.Length == 0)
        {
            return false;
        }

        using var engineScope = Runtime.DataVoEngine.PushCurrent(_engine);
        using IDisposable mvccScope = MvccExecutionScope.PushTransaction(transaction);

        // The same MVCC-aware lookup the general WHERE uses: committed rows visible to the snapshot, minus rows
        // this transaction deleted, plus its buffered inserts and updated rows that now hold the key.
        List<RowKey> rowIds = MvccCoordinator.FilterUsingIndex(
            _engine, databaseName, plan.StoredTable, plan.PrimaryKeyIndex, plan.PrimaryKeyColumn, key);
        transaction.ExcludeDeleted(plan.StoredTable, rowIds);
        if (rowIds.Count != 1)
        {
            // No row: nothing to do. More than one would break the key's uniqueness; let the general path decide.
            result = rowIds.Count == 0 ? Affected0Result : null;
            return rowIds.Count == 0;
        }

        return TryUpdateBufferedRow(transaction, plan, databaseName, rowIds[0], operands, out result);
    }

    /// <summary>
    /// Computes and buffers one row's new values. A row the transaction already holds in its buffer (an earlier
    /// update or its own insert) changes in place; a stored row gets one buffered update carrying the whole row.
    /// </summary>
    private bool TryUpdateBufferedRow(
        TransactionContext transaction,
        TransactionalUpdatePlan plan,
        string databaseName,
        RowKey rowId,
        object?[] operands,
        out QueryResult? result)
    {
        result = null;
        string table = plan.StoredTable;
        bool bufferedInsert = TransactionContext.IsBufferedRowKey(rowId);
        Dictionary<string, object?>? current;
        bool inBuffer;

        if (bufferedInsert)
        {
            inBuffer = transaction.TryGetBufferedInsert(table, rowId, out current);
            if (!inBuffer)
            {
                return false;
            }
        }
        else
        {
            MvccCoordinator.ValidateCanModifyRow(_engine, databaseName, table, rowId, transaction.Snapshot, "UPDATE");
            inBuffer = transaction.TryGetUpdatedRow(table, rowId, out current);
            if (!inBuffer)
            {
                current = ReadVisibleStoredRow(table, databaseName, rowId);
                if (current is null)
                {
                    result = Affected0Result;
                    return true;
                }
            }
        }

        // Every new value is computed before anything changes, so a value the fast path will not store leaves
        // the transaction untouched for the general path to report.
        if (!TryComputeNewValues(plan, current!, operands))
        {
            return false;
        }

        // A freshly read row belongs to this statement; it becomes the buffered row unless its comparer differs
        // from the case-insensitive one buffered rows use (rows deserialized from bytes are case-sensitive).
        Dictionary<string, object?> target = inBuffer || ReferenceEquals(current!.Comparer, StringComparer.OrdinalIgnoreCase)
            ? current!
            : new Dictionary<string, object?>(current, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < plan.AssignedColumns.Length; i++)
        {
            target[plan.AssignedColumns[i]] = operands[i];
        }

        if (inBuffer)
        {
            transaction.RefreshBufferedRow(table, rowId, target);
        }
        else
        {
            transaction.BufferUpdate(table, rowId, target, keyColumnsUnchanged: true);
        }

        Interlocked.Increment(ref _transactionalUpdateExecutions);
        result = Affected1Result;
        return true;
    }

    /// <summary>
    /// Reads the snapshot-visible image of a stored row the transaction has not changed yet. The lookup only
    /// returned rows whose stored version the snapshot sees, and <see cref="MvccCoordinator.ValidateCanModifyRow"/>
    /// rejected superseded ones, so the stored row is that image; only a table holding retained images (kept for
    /// older snapshots) goes through the full visibility read. Either way the returned dictionary is a fresh copy
    /// the caller owns.
    /// </summary>
    private Dictionary<string, object?>? ReadVisibleStoredRow(string table, string databaseName, RowKey rowId)
    {
        if (!_engine.VersionStorageManager.HasRetainedImages(databaseName, table))
        {
            return _engine.StorageContext.ReadCommittedRowDictionary(rowId, table, databaseName);
        }

        return _engine.StorageContext.GetTableContents([rowId], table, databaseName)
            .TryGetValue(rowId, out Dictionary<string, object?>? row) ? row : null;
    }

    /// <summary>
    /// Replaces each bound operand with the column's new value, using the general evaluator's operator semantics
    /// (NULL propagates; numbers widen to the widest operand type). Returns <see langword="false"/> when a value is
    /// one this path does not store: a non-numeric operand, or NULL in a NOT NULL column.
    /// </summary>
    private bool TryComputeNewValues(TransactionalUpdatePlan plan, Dictionary<string, object?> current, object?[] operands)
    {
        TransactionalUpdateCandidate candidate = _candidateTransactionalUpdate!;
        for (int i = 0; i < plan.AssignedColumns.Length; i++)
        {
            object? operand = operands[i];
            if (operand is not null && !IsNumeric(operand))
            {
                return false;
            }

            object? value = operand;
            AssignmentKind kind = candidate.Assignments[i].Kind;
            if (kind != AssignmentKind.Set)
            {
                current.TryGetValue(plan.AssignedColumns[i], out object? existing);
                if (existing is not null && !IsNumeric(existing))
                {
                    return false;
                }

                value = existing is null || operand is null
                    ? null
                    : ScalarEvaluator.ApplyArithmetic(kind == AssignmentKind.Add ? Operators.ADD : Operators.SUBTRACT, existing, operand);
            }

            if (value is null && plan.AssignedNotNull[i])
            {
                return false;
            }

            operands[i] = value;
        }

        return true;
    }

    /// <summary>Binds every SET operand into a per-thread scratch array; empty when a parameter is missing.</summary>
    private static object?[] BindAssignmentOperands(TransactionalUpdateCandidate candidate, ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        Assignment[] assignments = candidate.Assignments;
        object?[]? operands = t_assignmentValues;
        if (operands is null || operands.Length != assignments.Length)
        {
            operands = new object?[assignments.Length];
            t_assignmentValues = operands;
        }

        for (int i = 0; i < assignments.Length; i++)
        {
            if (!TryBindOperand(assignments[i].ParameterName, assignments[i].Constant, parameters, out operands[i]))
            {
                return [];
            }
        }

        return operands;
    }

    // A missing parameter returns false so the general path raises its usual binding error.
    private static bool TryBindOperand(
        string? parameterName,
        object? constant,
        ReadOnlySpan<DataVoQueryParameter> parameters,
        out object? value)
    {
        if (parameterName is null)
        {
            value = constant;
            return true;
        }

        if (SqlParameterBinder.TryFindParameter(parameterName, parameters, out DataVoQueryParameter parameter))
        {
            value = parameter.ToBoxedValue();
            return true;
        }

        value = null;
        return false;
    }

    private TransactionalUpdatePlan GetTransactionalUpdatePlan(TransactionalUpdateCandidate candidate, string databaseName)
    {
        int schemaVersion = _engine.Catalog.GetTableSchemaVersion(candidate.TableName, databaseName);
        long generation = _engine.Catalog.Generation;
        TransactionalUpdatePlan? plan = _cachedTransactionalUpdatePlan;
        if (plan is null
            || plan.SchemaVersion != schemaVersion
            || plan.CatalogGeneration != generation
            || !string.Equals(plan.DatabaseName, databaseName, StringComparison.Ordinal))
        {
            plan = BuildTransactionalUpdatePlan(candidate, databaseName, schemaVersion, generation);
            _cachedTransactionalUpdatePlan = plan;
        }

        return plan;
    }

    /// <summary>
    /// Resolves the candidate against the catalog. Eligible only when the WHERE names the table's single-column
    /// integer primary key (an authoritative index on InMemory and LSM), and every assigned column is numeric,
    /// not a key, and neither holds nor is referenced by a foreign key.
    /// </summary>
    private TransactionalUpdatePlan BuildTransactionalUpdatePlan(
        TransactionalUpdateCandidate candidate,
        string databaseName,
        int schemaVersion,
        long generation)
    {
        TransactionalUpdatePlan Ineligible() => new()
        {
            DatabaseName = databaseName,
            SchemaVersion = schemaVersion,
            CatalogGeneration = generation,
            Eligible = false,
        };

        string table = StoredTableName(candidate.TableName, databaseName);
        if (_engine.Config.StorageMode is not (StorageMode.InMemory or StorageMode.Lsm)
            || !_engine.Catalog.TableExists(table, databaseName))
        {
            return Ineligible();
        }

        IReadOnlyList<Column> columns = _engine.Catalog.GetTableColumns(table, databaseName);
        List<string> primaryKeys = _engine.Catalog.GetTablePrimaryKeys(table, databaseName);
        Column? keyColumn = primaryKeys.Count == 1 && primaryKeys[0].Equals(candidate.WhereColumn, StringComparison.OrdinalIgnoreCase)
            ? FindColumn(columns, primaryKeys[0])
            : null;
        if (keyColumn is null || !IsIntegerColumn(keyColumn))
        {
            return Ineligible();
        }

        HashSet<string> excluded = KeyAndForeignKeyColumns(table, databaseName);
        var assigned = new string[candidate.Assignments.Length];
        var notNull = new bool[candidate.Assignments.Length];
        for (int i = 0; i < assigned.Length; i++)
        {
            Column? column = FindColumn(columns, candidate.Assignments[i].Column);
            if (column is null || !IsNumericColumn(column) || excluded.Contains(column.Name)
                || Array.IndexOf(assigned, column.Name, 0, i) >= 0)
            {
                return Ineligible();
            }

            assigned[i] = column.Name;
            notNull[i] = column.IsNotNull;
        }

        return new TransactionalUpdatePlan
        {
            DatabaseName = databaseName,
            SchemaVersion = schemaVersion,
            CatalogGeneration = generation,
            Eligible = true,
            StoredTable = table,
            PrimaryKeyColumn = keyColumn.Name,
            PrimaryKeyIndex = $"_PK_{table}",
            AssignedColumns = assigned,
            AssignedNotNull = notNull,
        };
    }

    // Primary-key, unique, foreign-key and child-referenced (parent) columns: changing any of them needs the
    // general path's constraint checks.
    private HashSet<string> KeyAndForeignKeyColumns(string table, string databaseName)
    {
        var excluded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        excluded.UnionWith(_engine.Catalog.GetTablePrimaryKeys(table, databaseName));
        excluded.UnionWith(_engine.Catalog.GetTableUniqueKeys(table, databaseName));
        foreach (ForeignKey foreignKey in _engine.Catalog.GetTableForeignKeys(table, databaseName))
        {
            excluded.Add(foreignKey.AttributeName);
        }

        foreach ((string _, string _, string parentColumn, string _) in _engine.Catalog.GetChildForeignKeys(table, databaseName))
        {
            excluded.Add(parentColumn);
        }

        return excluded;
    }

    private static Column? FindColumn(IReadOnlyList<Column> columns, string name)
    {
        foreach (Column column in columns)
        {
            if (column.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                return column;
            }
        }

        return null;
    }

    private static bool IsIntegerColumn(Column column) =>
        column.Type.Equals("INT", StringComparison.OrdinalIgnoreCase)
        || column.Type.Equals("BIGINT", StringComparison.OrdinalIgnoreCase);

    private static bool IsNumericColumn(Column column) =>
        IsIntegerColumn(column)
        || column.Type.Equals("FLOAT", StringComparison.OrdinalIgnoreCase)
        || column.Type.Equals("DOUBLE", StringComparison.OrdinalIgnoreCase);
}
