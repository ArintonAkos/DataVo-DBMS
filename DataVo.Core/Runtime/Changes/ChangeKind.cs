namespace DataVo.Core.Runtime.Changes;

/// <summary>
/// Identifies the kind of a single committed row mutation captured as part of a <see cref="ChangeSet"/>.
/// </summary>
public enum ChangeKind
{
    /// <summary>A new row was added (Z-set weight +1).</summary>
    Insert,

    /// <summary>An existing row was modified (Z-set weights -old, +new).</summary>
    Update,

    /// <summary>An existing row was removed (Z-set weight -1).</summary>
    Delete
}
