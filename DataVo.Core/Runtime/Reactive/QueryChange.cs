namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Describes how a reactive query's result set changed in response to a batch of committed mutations.
/// </summary>
/// <remarks>
/// Rows are projected to the subscription's selected columns. <see cref="Added"/> rows newly satisfy
/// the predicate, <see cref="Removed"/> rows no longer satisfy it, and <see cref="Updated"/> rows
/// remain in the result set but changed value. When an operator can supply before-images,
/// <see cref="UpdatedBefore"/> is aligned by index with <see cref="Updated"/>.
/// </remarks>
public sealed class QueryChange
{
    /// <summary>
    /// Initializes a new <see cref="QueryChange"/>.
    /// </summary>
    public QueryChange(
        IReadOnlyList<IReadOnlyDictionary<string, object?>> added,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> removed,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> updated)
        : this(added, removed, updated, [])
    {
    }

    /// <summary>
    /// Initializes a new <see cref="QueryChange"/> with update before-images.
    /// </summary>
    public QueryChange(
        IReadOnlyList<IReadOnlyDictionary<string, object?>> added,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> removed,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> updated,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> updatedBefore)
    {
        Added = added;
        Removed = removed;
        Updated = updated;
        UpdatedBefore = updatedBefore;
    }

    /// <summary>Gets the rows that newly entered the result set.</summary>
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Added { get; }

    /// <summary>Gets the rows that left the result set.</summary>
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Removed { get; }

    /// <summary>Gets the rows that stayed in the result set but changed value.</summary>
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Updated { get; }

    /// <summary>
    /// Gets the before-images for rows in <see cref="Updated"/>, aligned by index when supplied.
    /// </summary>
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> UpdatedBefore { get; }

    /// <summary>Gets a value indicating whether this change carries no added, removed, or updated rows.</summary>
    public bool IsEmpty => Added.Count == 0 && Removed.Count == 0 && Updated.Count == 0;
}
