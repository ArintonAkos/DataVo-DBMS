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
    {
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
