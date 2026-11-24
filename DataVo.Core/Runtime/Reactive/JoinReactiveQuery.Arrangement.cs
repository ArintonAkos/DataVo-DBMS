using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// The typed per-side arrangement of <see cref="JoinReactiveQuery"/>: side rows stored as unboxed cells and
/// indexed by primary key and by join key.
/// </summary>
internal sealed partial class JoinReactiveQuery
{
    /// <summary>
    /// One row image of a join side, laid out in the side's catalog column order. Immutable once stored: a
    /// changed row arrives as a new image, so a reference comparison tells whether a row changed.
    /// </summary>
    private sealed class SideRow
    {
        private string? _identityText;

        /// <summary>Creates a row over the given cells (owned by the row from now on).</summary>
        public SideRow(CellValue[] cells) => Cells = cells;

        /// <summary>Gets the row's cells, in the side layout's column order.</summary>
        public CellValue[] Cells { get; }

        /// <summary>Gets or sets the row's primary-key identity.</summary>
        public PackedKey Pk { get; set; }

        /// <summary>Gets or sets the row's equi-join key (meaningful only when <see cref="HasJoinKey"/>).</summary>
        public PackedKey JoinKey { get; set; }

        /// <summary>Gets or sets whether every join column is non-NULL, so the row can match at all.</summary>
        public bool HasJoinKey { get; set; }

        /// <summary>Gets the rendered primary key exposed in the hidden identity column, rendered once.</summary>
        public string IdentityText => _identityText ??= Pk.Render();

        /// <summary>Clears cached state before a pooled image is reloaded.</summary>
        public void ResetCache() => _identityText = null;

        /// <summary>Reuses the rendered identity of an earlier image of the same row (same primary key).</summary>
        public void InheritIdentity(SideRow earlier) => _identityText ??= earlier._identityText;
    }

    /// <summary>
    /// One side of the join: its row layout, its rows keyed by primary key, and the same rows bucketed by
    /// join key. Retracted (before) images and emptied buckets are pooled, so steady-state updates reuse them.
    /// </summary>
    private sealed class JoinSide
    {
        private readonly int[] _primaryKeyOrdinals;
        private readonly int[] _joinKeyOrdinals;
        private readonly Stack<SideRow> _retractedPool = new();
        private readonly Stack<Dictionary<PackedKey, SideRow>> _bucketPool = new();

        /// <summary>Creates a side over the given layout and key columns.</summary>
        /// <param name="layout">The side's stored column layout (its catalog columns).</param>
        /// <param name="primaryKeys">The side's primary-key columns.</param>
        /// <param name="joinColumns">The side's join columns, in join-key order.</param>
        public JoinSide(ReactiveRowLayout layout, IReadOnlyList<string> primaryKeys, IReadOnlyList<string> joinColumns)
        {
            Layout = layout;
            _primaryKeyOrdinals = RequireOrdinals(layout, primaryKeys);
            _joinKeyOrdinals = RequireOrdinals(layout, joinColumns);
        }

        /// <summary>Gets the side's stored column layout.</summary>
        public ReactiveRowLayout Layout { get; }

        /// <summary>Gets every row of the side, keyed by primary key, regardless of join-key nullability.</summary>
        public Dictionary<PackedKey, SideRow> Rows { get; } = [];

        /// <summary>Gets the matchable rows (non-NULL join key) bucketed by join key, then by primary key.</summary>
        public Dictionary<PackedKey, Dictionary<PackedKey, SideRow>> ByKey { get; } = [];

        /// <summary>Loads a dictionary image as a new row that may be stored.</summary>
        public SideRow LoadStored(IReadOnlyDictionary<string, object?> row)
        {
            var image = new SideRow(new CellValue[Layout.Count]);
            Layout.Load(row, image.Cells);
            return Keyed(image);
        }

        /// <summary>
        /// Loads a typed image as a new row that may be stored, sharing the captured cell array when it is
        /// already in this side's layout.
        /// </summary>
        public SideRow LoadStored(TypedRow row)
        {
            if (!Layout.TryAdopt(row, out CellValue[] cells))
            {
                cells = new CellValue[Layout.Count];
                Layout.Load(row.AsRowRef(), row.Schema, cells);
            }

            return Keyed(new SideRow(cells));
        }

        /// <summary>
        /// Loads a before-image that is only probed and retracted, never stored. It comes from the pool and
        /// goes back through <see cref="Release"/> once the batch is diffed.
        /// </summary>
        public SideRow LoadRetracted(IReadOnlyDictionary<string, object?> row)
        {
            SideRow image = _retractedPool.Count > 0 ? _retractedPool.Pop() : new SideRow(new CellValue[Layout.Count]);
            image.ResetCache();
            Layout.Load(row, image.Cells);
            return Keyed(image);
        }

        /// <summary>Returns a retracted image to the pool.</summary>
        public void Release(SideRow image) => _retractedPool.Push(image);

        /// <summary>Finds the bucket of this side's rows that join with <paramref name="probe"/>.</summary>
        public bool TryGetBucket(SideRow probe, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Dictionary<PackedKey, SideRow>? bucket)
        {
            if (probe.HasJoinKey && ByKey.TryGetValue(probe.JoinKey, out bucket))
            {
                return true;
            }

            bucket = null;
            return false;
        }

        /// <summary>Returns whether any row of this side joins with <paramref name="probe"/>.</summary>
        public bool HasMatchFor(SideRow probe) => TryGetBucket(probe, out Dictionary<PackedKey, SideRow>? bucket) && bucket.Count > 0;

        /// <summary>Stores (or replaces) a row under its primary key and, when matchable, its join key.</summary>
        public void Store(SideRow row)
        {
            if (Rows.TryGetValue(row.Pk, out SideRow? earlier))
            {
                row.InheritIdentity(earlier);
            }

            Rows[row.Pk] = row;
            if (!row.HasJoinKey)
            {
                return;
            }

            if (!ByKey.TryGetValue(row.JoinKey, out Dictionary<PackedKey, SideRow>? bucket))
            {
                bucket = _bucketPool.Count > 0 ? _bucketPool.Pop() : [];
                ByKey[row.JoinKey] = bucket;
            }

            bucket[row.Pk] = row;
        }

        /// <summary>Removes the row identified by an image's primary key, using the image's join key.</summary>
        public void Remove(SideRow image)
        {
            Rows.Remove(image.Pk);
            if (!image.HasJoinKey || !ByKey.TryGetValue(image.JoinKey, out Dictionary<PackedKey, SideRow>? bucket))
            {
                return;
            }

            bucket.Remove(image.Pk);
            if (bucket.Count == 0)
            {
                ByKey.Remove(image.JoinKey);
                _bucketPool.Push(bucket);
            }
        }

        private SideRow Keyed(SideRow image)
        {
            image.Pk = PackedKey.From(image.Cells, _primaryKeyOrdinals);
            image.JoinKey = PackedKey.From(image.Cells, _joinKeyOrdinals);
            image.HasJoinKey = !image.JoinKey.HasNull;
            return image;
        }

        private static int[] RequireOrdinals(ReactiveRowLayout layout, IReadOnlyList<string> columns)
        {
            var ordinals = new int[columns.Count];
            for (int i = 0; i < columns.Count; i++)
            {
                ordinals[i] = layout.OrdinalOf(columns[i]);
                if (ordinals[i] < 0)
                {
                    throw new NotSupportedException(
                        $"Reactive join column '{columns[i]}' is not a column of its table.");
                }
            }

            return ordinals;
        }
    }
}
