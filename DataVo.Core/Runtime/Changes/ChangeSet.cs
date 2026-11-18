namespace DataVo.Core.Runtime.Changes;

/// <summary>
/// Represents all row mutations produced by a single committed transaction (or auto-commit DML).
/// </summary>
/// <remarks>
/// Exactly one <see cref="ChangeSet"/> is published per successful commit; rollbacks produce none.
/// The <see cref="SequenceId"/> is monotonically increasing across the owning engine.
/// </remarks>
public sealed class ChangeSet
{
    /// <summary>
    /// Initializes a new <see cref="ChangeSet"/>.
    /// </summary>
    /// <param name="sequenceId">A monotonically increasing identifier for this change set.</param>
    /// <param name="databaseName">The database the changes were applied to, or <c>null</c> if unknown.</param>
    /// <param name="changes">The ordered row mutations contained in this change set.</param>
    public ChangeSet(long sequenceId, string? databaseName, IReadOnlyList<RowChange> changes)
        : this(sequenceId, databaseName, changes, origin: null)
    {
    }

    /// <summary>
    /// Initializes a new <see cref="ChangeSet"/> written under a write origin.
    /// </summary>
    /// <param name="sequenceId">A monotonically increasing identifier for this change set.</param>
    /// <param name="databaseName">The database the changes were applied to, or <c>null</c> if unknown.</param>
    /// <param name="changes">The ordered row mutations contained in this change set.</param>
    /// <param name="origin">The write origin in effect when the changes were written (see <see cref="ChangeCapture.BeginOrigin"/>).</param>
    public ChangeSet(long sequenceId, string? databaseName, IReadOnlyList<RowChange> changes, string? origin)
    {
        Origin = origin;
        SequenceId = sequenceId;
        DatabaseName = databaseName;
        Changes = changes;
        Tables = BuildTables(changes);
    }

    /// <summary>Gets the monotonically increasing identifier for this change set.</summary>
    public long SequenceId { get; }

    /// <summary>Gets the database the changes were applied to, or <c>null</c> if unknown.</summary>
    public string? DatabaseName { get; }

    /// <summary>Gets the ordered row mutations contained in this change set.</summary>
    public IReadOnlyList<RowChange> Changes { get; }

    /// <summary>
    /// Gets the write origin the changes were made under, or <c>null</c> for ordinary local writes.
    /// </summary>
    /// <remarks>
    /// Set by <see cref="ChangeCapture.BeginOrigin"/> around the statement, batch or COMMIT that produced the
    /// changes. Subscribers can use it to tell apart writes that came from somewhere else (an import, a
    /// replication feed) from the application's own writes; every subscriber still receives every change set.
    /// </remarks>
    public string? Origin { get; }

    /// <summary>Gets the distinct table names touched by this change set.</summary>
    public IReadOnlyList<string> Tables { get; }

    private static IReadOnlyList<string> BuildTables(IReadOnlyList<RowChange> changes)
    {
        if (changes.Count == 0)
        {
            return [];
        }

        if (changes.Count == 1)
        {
            return [changes[0].Table];
        }

        var tables = new List<string>();
        for (int i = 0; i < changes.Count; i++)
        {
            string table = changes[i].Table;
            bool seen = false;
            for (int j = 0; j < tables.Count; j++)
            {
                if (tables[j].Equals(table, StringComparison.OrdinalIgnoreCase))
                {
                    seen = true;
                    break;
                }
            }

            if (!seen)
            {
                tables.Add(table);
            }
        }

        return tables;
    }
}
