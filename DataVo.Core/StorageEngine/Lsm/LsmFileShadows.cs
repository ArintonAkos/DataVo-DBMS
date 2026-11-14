namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// For one set of live SSTables (in search order), the rows of each file's columnar segment that a newer file of
/// the set replaces or deletes.
/// </summary>
/// <param name="files">One vector per file; null for a file no newer file overlaps.</param>
/// <param name="unordered">Whether two files share keys and neither is known to be the newer one.</param>
internal sealed class LsmFileShadows(LsmDeleteVector?[] files, bool unordered)
{
    /// <summary>A set in which two files share keys in an order that cannot be told: no scan of it can tell which rows count.</summary>
    public static readonly LsmFileShadows UnorderedSet = new([], unordered: true);

    /// <summary>Whether no scan of this set of files can tell which rows count.</summary>
    public bool Unordered { get; } = unordered;

    /// <summary>The vector of the file at <paramref name="file"/> in search order, or null.</summary>
    public LsmDeleteVector? Of(int file) => files[file];

    /// <summary>The rows to leave out of a chunk of a file, a bit per row; false when it has none.</summary>
    public bool TryGet(int file, int chunk, out ReadOnlySpan<ulong> shadowed)
    {
        if (files[file] is { } vector)
        {
            return vector.TryGet(chunk, out shadowed);
        }

        shadowed = default;
        return false;
    }
}
