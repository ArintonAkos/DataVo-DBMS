using DataVo.Core.Parser.AST;
using DataVo.Core.Transactions;

namespace DataVo.Core.Parser.DML;

/// <summary>
/// Represents the locked target row set acquired for an auto-commit DML statement.
/// </summary>
/// <param name="TargetRowIds">The revalidated row keys to mutate while locks are held.</param>
/// <param name="RowWriteLocks">The row-level write locks held when table escalation was not used.</param>
/// <param name="HoldsTableWriteLock">
/// <see langword="true"/> when repeated concurrent row relocations triggered a table-level write lock;
/// otherwise <see langword="false"/>.
/// </param>
internal readonly record struct AcquiredDmlRowScope(
    List<RowKey> TargetRowIds,
    List<RowKey> RowWriteLocks,
    bool HoldsTableWriteLock);

/// <summary>
/// Coordinates row-level write locking and post-lock revalidation for auto-commit <c>UPDATE</c> and
/// <c>DELETE</c> statements.
/// <para>
/// In non-clustered storage modes, an <c>UPDATE</c> replaces a row out-of-place under a newly allocated
/// <see cref="RowKey"/>. When a concurrent <c>DELETE</c> or <c>UPDATE</c> waits on the old row key,
/// revalidation detects that the original key vanished while a new matching key appeared, releases the
/// stale lock immediately, and retries against the relocated row key.
/// </para>
/// </summary>
/// <example>
/// <code>
/// AcquiredDmlRowScope scope = DmlRowLockCoordinator.Acquire(
///     Locks,
///     databaseName,
///     tableName,
///     whereExpression,
///     () => EvaluateWhere(databaseName));
/// try
/// {
///     // Mutate scope.TargetRowIds safely under held locks.
/// }
/// finally
/// {
///     DmlRowLockCoordinator.Release(Locks, databaseName, tableName, scope);
/// }
/// </code>
/// </example>
internal static class DmlRowLockCoordinator
{
    private const int MaxRelocationRetries = 8;

    /// <summary>
    /// Evaluates candidate rows, acquires the required write locks, and revalidates the candidates under
    /// those locks, retrying when a concurrent out-of-place update relocates a matching row.
    /// </summary>
    /// <param name="locks">The lock manager coordinating row and table locks.</param>
    /// <param name="databaseName">The target database name.</param>
    /// <param name="tableName">The target table name.</param>
    /// <param name="whereExpression">The statement WHERE expression, or <see langword="null"/> when absent.</param>
    /// <param name="evaluateCandidates">Callback that evaluates the current matching row keys.</param>
    /// <returns>An <see cref="AcquiredDmlRowScope"/> describing the target rows and held locks.</returns>
    public static AcquiredDmlRowScope Acquire(
        LockManager locks,
        string databaseName,
        string tableName,
        ExpressionNode? whereExpression,
        Func<List<RowKey>> evaluateCandidates)
    {
        List<RowKey> candidates = evaluateCandidates();
        if (candidates.Count == 0)
        {
            return new AcquiredDmlRowScope([], [], HoldsTableWriteLock: false);
        }

        bool triviallyTrue = whereExpression is not null && IsTriviallyTrueWhere(whereExpression);
        for (int attempt = 0; attempt <= MaxRelocationRetries; attempt++)
        {
            List<RowKey> rowWriteLocks = locks.AcquireRowWriteLocks(databaseName, tableName, candidates);
            bool keepLocks = false;
            try
            {
                if (triviallyTrue)
                {
                    keepLocks = true;
                    return new AcquiredDmlRowScope([.. candidates], rowWriteLocks, HoldsTableWriteLock: false);
                }

                List<RowKey> currentMatches = evaluateCandidates();
                (List<RowKey> revalidated, bool shouldRetry) = ClassifyRevalidatedMatches(
                    candidates,
                    rowWriteLocks,
                    currentMatches);

                if (shouldRetry)
                {
                    candidates = currentMatches;
                    continue;
                }

                if (revalidated.Count == 0)
                {
                    return new AcquiredDmlRowScope([], [], HoldsTableWriteLock: false);
                }

                keepLocks = true;
                return new AcquiredDmlRowScope(revalidated, rowWriteLocks, HoldsTableWriteLock: false);
            }
            finally
            {
                if (!keepLocks)
                {
                    locks.ReleaseRowWriteLocksImmediately(databaseName, tableName, rowWriteLocks);
                }
            }
        }

        return AcquireEscalatedFallback(locks, databaseName, tableName, candidates, evaluateCandidates);
    }

    /// <summary>
    /// Locks newly allocated out-of-place row keys before they are published to any index, ensuring that
    /// concurrent writers discovering a relocated row through one index block until all indexes and old-row
    /// tombstones finish updating.
    /// </summary>
    /// <param name="locks">The lock manager coordinating row write locks.</param>
    /// <param name="databaseName">The target database name.</param>
    /// <param name="tableName">The target table name.</param>
    /// <param name="assignedRowIds">The newly allocated row keys created by an out-of-place update.</param>
    /// <param name="alreadyHeldRowIds">Row keys already locked by the current operation.</param>
    /// <param name="skipLocking">
    /// <see langword="true"/> when locking is unnecessary (for example, clustered updates or when a table-level
    /// write lock is already held).
    /// </param>
    /// <returns>The ordered list of newly acquired row write locks to release in a <c>finally</c> block.</returns>
    public static List<RowKey> AcquireRelocatedRowWriteLocks(
        LockManager locks,
        string databaseName,
        string tableName,
        IReadOnlyList<RowKey> assignedRowIds,
        IReadOnlyList<RowKey> alreadyHeldRowIds,
        bool skipLocking)
    {
        if (skipLocking || assignedRowIds.Count == 0)
        {
            return [];
        }

        HashSet<RowKey> held = [.. alreadyHeldRowIds];
        List<RowKey> unheld = [];
        for (int i = 0; i < assignedRowIds.Count; i++)
        {
            RowKey assigned = assignedRowIds[i];
            if (assigned != default && !held.Contains(assigned))
            {
                unheld.Add(assigned);
            }
        }

        return unheld.Count > 0
            ? locks.AcquireRowWriteLocks(databaseName, tableName, unheld)
            : [];
    }

    /// <summary>
    /// Releases all locks held by <paramref name="scope"/>.
    /// </summary>
    /// <param name="locks">The lock manager that owns the held locks.</param>
    /// <param name="databaseName">The target database name.</param>
    /// <param name="tableName">The target table name.</param>
    /// <param name="scope">The acquired scope returned by <see cref="Acquire"/>.</param>
    public static void Release(
        LockManager locks,
        string databaseName,
        string tableName,
        AcquiredDmlRowScope scope)
    {
        if (scope.HoldsTableWriteLock)
        {
            locks.ReleaseWriteLock(databaseName, tableName);
            return;
        }

        if (scope.RowWriteLocks.Count > 0)
        {
            locks.ReleaseRowWriteLocks(databaseName, tableName, scope.RowWriteLocks);
        }
    }

    /// <summary>
    /// Determines whether a WHERE expression is trivially always true (such as <c>TRUE</c> or <c>1 = 1</c>).
    /// </summary>
    /// <param name="expression">The WHERE expression to inspect.</param>
    /// <returns><see langword="true"/> when the predicate unconditionally matches every row; otherwise <see langword="false"/>.</returns>
    public static bool IsTriviallyTrueWhere(ExpressionNode expression)
    {
        if (expression is LiteralNode literal)
        {
            string? value = literal.Value?.ToString();
            return value != null && value.Equals("true", StringComparison.OrdinalIgnoreCase);
        }

        if (expression is ColumnRefNode columnRef)
        {
            return columnRef.Column.Equals("true", StringComparison.OrdinalIgnoreCase);
        }

        if (expression is ResolvedColumnRefNode resolved)
        {
            return resolved.Column.Equals("true", StringComparison.OrdinalIgnoreCase);
        }

        if (expression is BinaryExpressionNode binary
            && binary.Operator == "="
            && binary.Left is LiteralNode leftLiteral
            && binary.Right is LiteralNode rightLiteral)
        {
            return string.Equals(leftLiteral.Value?.ToString(), rightLiteral.Value?.ToString(), StringComparison.Ordinal);
        }

        return false;
    }

    private static (List<RowKey> Revalidated, bool ShouldRetry) ClassifyRevalidatedMatches(
        IReadOnlyList<RowKey> originalCandidates,
        IReadOnlyList<RowKey> lockedRowIds,
        IReadOnlyList<RowKey> currentMatches)
    {
        HashSet<RowKey> lockCovered = [.. lockedRowIds];
        HashSet<RowKey> originalSet = [.. originalCandidates];

        var revalidated = new List<RowKey>(currentMatches.Count);
        bool hasUncoveredMatch = false;

        foreach (RowKey rowId in currentMatches)
        {
            bool isCovered = originalSet.Contains(rowId)
                && (rowId == default || lockCovered.Count == 0 || lockCovered.Contains(rowId));
            if (isCovered)
            {
                revalidated.Add(rowId);
            }
            else if (rowId != default)
            {
                hasUncoveredMatch = true;
            }
        }

        bool anyLockedCandidateDisappeared = revalidated.Count < lockCovered.Count;
        bool shouldRetry = anyLockedCandidateDisappeared && hasUncoveredMatch;
        return (revalidated, shouldRetry);
    }

    private static AcquiredDmlRowScope AcquireEscalatedFallback(
        LockManager locks,
        string databaseName,
        string tableName,
        List<RowKey> latestCandidates,
        Func<List<RowKey>> evaluateCandidates)
    {
        // Only escalate to a table write lock when the thread holds no locks, preventing lock-upgrade cycles.
        if (!LockManager.CurrentThreadHoldsLocks)
        {
            locks.AcquireWriteLock(databaseName, tableName);
            bool keepTableLock = false;
            try
            {
                List<RowKey> finalMatches = evaluateCandidates();
                if (finalMatches.Count == 0)
                {
                    return new AcquiredDmlRowScope([], [], HoldsTableWriteLock: false);
                }

                keepTableLock = true;
                return new AcquiredDmlRowScope(finalMatches, [], HoldsTableWriteLock: true);
            }
            finally
            {
                if (!keepTableLock)
                {
                    locks.ReleaseWriteLock(databaseName, tableName);
                }
            }
        }

        List<RowKey> fallbackLocks = locks.AcquireRowWriteLocks(databaseName, tableName, latestCandidates);
        bool keepFallback = false;
        try
        {
            List<RowKey> currentMatches = evaluateCandidates();
            (List<RowKey> revalidated, _) = ClassifyRevalidatedMatches(latestCandidates, fallbackLocks, currentMatches);
            if (revalidated.Count == 0)
            {
                return new AcquiredDmlRowScope([], [], HoldsTableWriteLock: false);
            }

            keepFallback = true;
            return new AcquiredDmlRowScope(revalidated, fallbackLocks, HoldsTableWriteLock: false);
        }
        finally
        {
            if (!keepFallback)
            {
                locks.ReleaseRowWriteLocksImmediately(databaseName, tableName, fallbackLocks);
            }
        }
    }
}
