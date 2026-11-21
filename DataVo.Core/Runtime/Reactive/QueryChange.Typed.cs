namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Describes how a strongly-typed reactive query's result set changed in response to a batch of committed mutations.
/// </summary>
/// <typeparam name="T">The projected element or model type.</typeparam>
/// <remarks>
/// <see cref="Added"/> items newly entered the result set, <see cref="Removed"/> items left it, and
/// <see cref="Updated"/> items remained in the result set with changed values. When the underlying operator
/// provides before-images, <see cref="UpdatedBefore"/> is aligned by index with <see cref="Updated"/>.
/// </remarks>
/// <example>
/// <code>
/// using var sub = db.Table&lt;Player&gt;()
///     .Where(100, static (p, min) => p.Score >= min)
///     .Subscribe(change =>
///     {
///         foreach (Player p in change.Added) Console.WriteLine($"Added {p.Name}");
///         foreach (Player p in change.Removed) Console.WriteLine($"Removed {p.Name}");
///     });
/// </code>
/// </example>
public sealed class QueryChange<T>
{
    /// <summary>
    /// Initializes a new <see cref="QueryChange{T}"/> without before-images.
    /// </summary>
    /// <param name="added">Items that newly entered the result set.</param>
    /// <param name="removed">Items that left the result set.</param>
    /// <param name="updated">Items that stayed in the result set with updated values.</param>
    public QueryChange(
        IReadOnlyList<T> added,
        IReadOnlyList<T> removed,
        IReadOnlyList<T> updated)
        : this(added, removed, updated, [])
    {
    }

    /// <summary>
    /// Initializes a new <see cref="QueryChange{T}"/> with update before-images.
    /// </summary>
    /// <param name="added">Items that newly entered the result set.</param>
    /// <param name="removed">Items that left the result set.</param>
    /// <param name="updated">Items that stayed in the result set with updated values.</param>
    /// <param name="updatedBefore">Before-images aligned by index with <paramref name="updated"/>.</param>
    public QueryChange(
        IReadOnlyList<T> added,
        IReadOnlyList<T> removed,
        IReadOnlyList<T> updated,
        IReadOnlyList<T> updatedBefore)
    {
        Added = added;
        Removed = removed;
        Updated = updated;
        UpdatedBefore = updatedBefore;
    }

    /// <summary>Gets the items that newly entered the result set.</summary>
    public IReadOnlyList<T> Added { get; }

    /// <summary>Gets the items that left the result set.</summary>
    public IReadOnlyList<T> Removed { get; }

    /// <summary>Gets the items that stayed in the result set but changed value.</summary>
    public IReadOnlyList<T> Updated { get; }

    /// <summary>Gets the before-images for items in <see cref="Updated"/>, aligned by index when supplied.</summary>
    public IReadOnlyList<T> UpdatedBefore { get; }

    /// <summary>Gets a value indicating whether this change carries no added, removed, or updated items.</summary>
    public bool IsEmpty => Added.Count == 0 && Removed.Count == 0 && Updated.Count == 0;
}
