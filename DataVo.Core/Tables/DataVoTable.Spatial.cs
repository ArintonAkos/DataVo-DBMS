using System.Numerics;
using DataVo.Core.Models.Catalog;

namespace DataVo.Core;

// Spatial queries over a model's 3D position columns (System.Numerics.Vector3 properties, stored as VECTOR(3)).
public sealed partial class DataVoTable<T>
{
    /// <summary>
    /// Selects the rows whose position is within <paramref name="radius"/> of <paramref name="center"/>, a point
    /// exactly on the sphere included. Rows stream a page at a time as the result is enumerated.
    /// </summary>
    /// <param name="center">The center of the sphere.</param>
    /// <param name="radius">The sphere's radius; zero matches only points at the center.</param>
    /// <param name="column">The position column; may be omitted when the model has exactly one.</param>
    /// <returns>The matching rows, in storage order.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="radius"/> is negative, NaN or infinite.</exception>
    /// <exception cref="ArgumentException"><paramref name="center"/> is not finite, or <paramref name="column"/> is not a position column.</exception>
    /// <exception cref="InvalidOperationException">The model has no position column, or several and none was named.</exception>
    /// <remarks>
    /// The test reads each row's stored coordinates before building the model, so rows outside the sphere are
    /// never materialized and the distance math allocates nothing. The query reads the whole table: an HNSW index
    /// answers nearest-neighbor questions, not range ones.
    /// </remarks>
    /// <example>
    /// <code>
    /// foreach (Ship ship in db.Table&lt;Ship&gt;().WithinRadius(station, radius: 50f))
    /// {
    ///     Console.WriteLine(ship.Name);
    /// }
    /// </code>
    /// </example>
    public IEnumerable<T> WithinRadius(Vector3 center, float radius, string? column = null)
    {
        RequireFinite(center, nameof(center));
        if (float.IsNaN(radius) || float.IsInfinity(radius) || radius < 0f)
        {
            throw new ArgumentOutOfRangeException(nameof(radius), radius, "The radius must be a finite number zero or greater.");
        }

        DataVoSpatialColumn spatial = SpatialColumn(column);
        return Filtered(spatial.Column, new SpatialFilter(center, radius));
    }

    /// <summary>
    /// Selects the rows whose position lies inside the axis-aligned box with corners <paramref name="minCorner"/>
    /// and <paramref name="maxCorner"/>, its surface included. Rows stream a page at a time as the result is enumerated.
    /// </summary>
    /// <param name="minCorner">One corner of the box.</param>
    /// <param name="maxCorner">The opposite corner; the corners may be given in either order.</param>
    /// <param name="column">The position column; may be omitted when the model has exactly one.</param>
    /// <returns>The matching rows, in storage order.</returns>
    /// <exception cref="ArgumentException">A corner has a NaN coordinate, or <paramref name="column"/> is not a position column.</exception>
    /// <exception cref="InvalidOperationException">The model has no position column, or several and none was named.</exception>
    /// <example>
    /// <code>
    /// List&lt;Ship&gt; docked = db.Table&lt;Ship&gt;().WithinBoundingBox(new Vector3(0, 0, 0), new Vector3(100, 20, 100)).ToList();
    /// </code>
    /// </example>
    public IEnumerable<T> WithinBoundingBox(Vector3 minCorner, Vector3 maxCorner, string? column = null) =>
        WithinBoundingBox(new BoundingBox3(minCorner, maxCorner), column);

    /// <summary>
    /// Selects the rows whose position lies inside a box, its surface included. Rows stream a page at a time as
    /// the result is enumerated.
    /// </summary>
    /// <param name="box">The box.</param>
    /// <param name="column">The position column; may be omitted when the model has exactly one.</param>
    /// <returns>The matching rows, in storage order.</returns>
    /// <exception cref="ArgumentException"><paramref name="column"/> is not a position column.</exception>
    /// <exception cref="InvalidOperationException">The model has no position column, or several and none was named.</exception>
    public IEnumerable<T> WithinBoundingBox(BoundingBox3 box, string? column = null)
    {
        DataVoSpatialColumn spatial = SpatialColumn(column);
        return Filtered(spatial.Column, new SpatialFilter(box));
    }

    /// <summary>
    /// Returns the <paramref name="k"/> rows closest to <paramref name="target"/>, nearest first.
    /// </summary>
    /// <param name="target">The point to measure from.</param>
    /// <param name="k">How many rows to return at most.</param>
    /// <param name="column">The position column; may be omitted when the model has exactly one.</param>
    /// <param name="exact">
    /// <see langword="true"/> to stream the table and return the exact answer even when an index exists.
    /// </param>
    /// <returns>Up to <paramref name="k"/> rows ordered by distance; ties keep storage order.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="k"/> is negative.</exception>
    /// <exception cref="ArgumentException"><paramref name="target"/> is not finite, or <paramref name="column"/> is not a position column.</exception>
    /// <exception cref="InvalidOperationException">The model has no position column, or several and none was named.</exception>
    /// <remarks>
    /// <para>
    /// When the column has a Euclidean HNSW index (<c>[SpatialIndex]</c>, or
    /// <c>CREATE INDEX … USING HNSW WITH (metric = 'euclidean')</c>), the index finds the candidates and they are
    /// ranked by their exact distance. HNSW is an approximate search: it can, rarely, miss a true neighbor. Pass
    /// <paramref name="exact"/> when every answer must be exact.
    /// </para>
    /// <para>
    /// Without such an index, or inside an explicit transaction (whose uncommitted rows the index does not
    /// hold), the answer is exact: the table is streamed once and only the best <paramref name="k"/> rows are kept.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// IReadOnlyList&lt;Ship&gt; closest = db.Table&lt;Ship&gt;().NearestNeighbors(player.Position, k: 5);
    /// </code>
    /// </example>
    public IReadOnlyList<T> NearestNeighbors(Vector3 target, int k, string? column = null, bool exact = false)
    {
        RequireFinite(target, nameof(target));
        if (k < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(k), k, "k cannot be negative.");
        }

        DataVoSpatialColumn spatial = SpatialColumn(column);
        if (k == 0)
        {
            return [];
        }

        return !exact && UsableIndex(spatial) is { } indexName
            ? NearestFromIndex(spatial.Column, indexName, target, k)
            : NearestFromScan(spatial.Column, target, k);
    }

    private DataVoSpatialColumn SpatialColumn(string? column)
    {
        DataVoSpatialColumn[] columns = _info.SpatialColumns;
        if (column is null)
        {
            return columns.Length switch
            {
                1 => columns[0],
                0 => throw new InvalidOperationException(
                    $"{typeof(T).Name} has no 3D position column. Spatial queries need a System.Numerics.Vector3 property."),
                _ => throw new InvalidOperationException(
                    $"{typeof(T).Name} has several 3D position columns ({string.Join(", ", columns.Select(static c => c.Column))}); name the one to query."),
            };
        }

        foreach (DataVoSpatialColumn candidate in columns)
        {
            if (candidate.Column.Equals(column, StringComparison.OrdinalIgnoreCase))
            {
                return candidate;
            }
        }

        throw new ArgumentException($"{column} is not a 3D position column of {typeof(T).Name}.", nameof(column));
    }

    /// <summary>Gets the index to answer a nearest-neighbor query from, or null when the query must scan.</summary>
    private string? UsableIndex(DataVoSpatialColumn spatial)
    {
        if (Context.Engine.TransactionManager.HasActiveTransaction(Context.SessionId))
        {
            return null;
        }

        string? database = Context.Engine.Sessions.Get(Context.SessionId);
        if (database is null)
        {
            return null;
        }

        // Prefer the index the model declared, but accept any Euclidean HNSW index on the column, so a table
        // indexed by hand in SQL is used too.
        string? found = null;
        foreach (IndexFile index in Context.Engine.Catalog.GetTableIndexes(_info.TableName, database))
        {
            if (index.AttributeNames.Count == 1
                && index.AttributeNames[0].Equals(spatial.Column, StringComparison.OrdinalIgnoreCase)
                && index.IndexKind.Equals("HNSW", StringComparison.OrdinalIgnoreCase)
                && index.EffectiveMetric == IndexFile.EuclideanMetric)
            {
                if (spatial.IndexName is not null && index.IndexFileName.Equals(spatial.IndexName, StringComparison.OrdinalIgnoreCase))
                {
                    return index.IndexFileName;
                }

                found ??= index.IndexFileName;
            }
        }

        return found;
    }

    private List<T> NearestFromIndex(string column, string indexName, Vector3 target, int k)
    {
        // Ranking more candidates than asked for, by exact distance, makes up for most of HNSW's misses.
        int candidates = k > int.MaxValue / 2 ? k : Math.Max(k * 2, k + 16);
        ReadOnlySpan<float> query = [target.X, target.Y, target.Z];
        List<Dictionary<string, object?>> rows = Context.SearchNearest(_info.TableName, indexName, query, candidates);

        var ranked = new (double Distance, int Order, Dictionary<string, object?> Row)[rows.Count];
        int count = 0;
        for (int i = 0; i < rows.Count; i++)
        {
            if (rows[i].TryGetValue(column, out object? value) && Coordinates(value) is { } coordinates)
            {
                ranked[count++] = (Spatial3.DistanceSquaredPrecise(coordinates, target), i, rows[i]);
            }
        }

        Array.Sort(ranked, 0, count, NearestComparer.Instance);
        var result = new List<T>(Math.Min(k, count));
        for (int i = 0; i < count && i < k; i++)
        {
            result.Add(Read(ranked[i].Row));
        }

        return result;
    }

    private List<T> NearestFromScan(string column, Vector3 target, int k)
    {
        // A max-heap of the best k rows seen so far: its root is the farthest of them, the one to evict.
        var heap = new BoundedNearestHeap(Math.Min(k, 1024), k);
        int ordinal = -1;
        int order = 0;
        foreach (DataVoRow row in Scan())
        {
            if (ordinal < 0)
            {
                ordinal = OrdinalOf(row, column);
            }

            if (Coordinates(row[ordinal]) is { } coordinates)
            {
                heap.Offer(Spatial3.DistanceSquaredPrecise(coordinates, target), order, row);
            }

            order++;
        }

        DataVoRow[] nearest = heap.DrainNearestFirst();
        var result = new List<T>(nearest.Length);
        foreach (DataVoRow row in nearest)
        {
            result.Add(Read(row));
        }

        return result;
    }

    private IEnumerable<T> Filtered(string column, SpatialFilter filter)
    {
        int ordinal = -1;
        foreach (DataVoRow row in Scan())
        {
            if (ordinal < 0)
            {
                ordinal = OrdinalOf(row, column);
            }

            if (Coordinates(row[ordinal]) is { } coordinates && filter.Matches(coordinates))
            {
                yield return Read(row);
            }
        }
    }

    private static int OrdinalOf(DataVoRow row, string column)
    {
        IReadOnlyList<string> fields = row.Fields;
        for (int i = 0; i < fields.Count; i++)
        {
            if (fields[i].Equals(column, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        throw new InvalidOperationException($"The scan of {typeof(T).Name} did not return column {column}.");
    }

    /// <summary>Gets a stored position's coordinates, or null for a NULL or malformed cell, which matches nothing.</summary>
    private static float[]? Coordinates(object? value) =>
        value is float[] { Length: Spatial3.Dimensions } coordinates ? coordinates : null;

    private static void RequireFinite(Vector3 point, string name)
    {
        if (!Spatial3.IsFinite(point))
        {
            throw new ArgumentException("Every coordinate must be a finite number.", name);
        }
    }

    /// <summary>A sphere or box test, held by value so a query captures no closure.</summary>
    private readonly struct SpatialFilter
    {
        private readonly BoundingBox3 _box;
        private readonly Vector3 _center;
        private readonly float _radius;
        private readonly bool _isSphere;

        public SpatialFilter(Vector3 center, float radius)
        {
            _center = center;
            _radius = radius;
            _isSphere = true;
            _box = BoundingBox3.AroundSphere(center, radius);
        }

        public SpatialFilter(BoundingBox3 box)
        {
            _box = box;
            _center = default;
            _radius = 0f;
            _isSphere = false;
        }

        public bool Matches(ReadOnlySpan<float> coordinates)
        {
            // The box test rejects most far rows before the distance is computed.
            if (!Spatial3.IsInside(coordinates, _box))
            {
                return false;
            }

            return !_isSphere || Spatial3.IsWithinRadius(coordinates, _center, _radius);
        }
    }

    private sealed class NearestComparer : IComparer<(double Distance, int Order, Dictionary<string, object?> Row)>
    {
        public static readonly NearestComparer Instance = new();

        public int Compare((double Distance, int Order, Dictionary<string, object?> Row) x, (double Distance, int Order, Dictionary<string, object?> Row) y)
        {
            int byDistance = x.Distance.CompareTo(y.Distance);
            return byDistance != 0 ? byDistance : x.Order.CompareTo(y.Order);
        }
    }

    /// <summary>Keeps the k nearest rows of a stream; offering a row allocates nothing once the heap is full.</summary>
    private sealed class BoundedNearestHeap(int initialCapacity, int limit)
    {
        private double[] _distances = new double[initialCapacity];
        private int[] _orders = new int[initialCapacity];
        private DataVoRow[] _rows = new DataVoRow[initialCapacity];
        private int _count;

        public void Offer(double distance, int order, DataVoRow row)
        {
            if (_count < limit)
            {
                if (_count == _distances.Length)
                {
                    int grown = (int)Math.Min((long)limit, Math.Max(4L, _distances.Length * 2L));
                    Array.Resize(ref _distances, grown);
                    Array.Resize(ref _orders, grown);
                    Array.Resize(ref _rows, grown);
                }

                Set(_count, distance, order, row);
                SiftUp(_count++);
                return;
            }

            // Later rows lose ties, so an equally distant row does not replace an earlier one.
            if (distance >= _distances[0])
            {
                return;
            }

            Set(0, distance, order, row);
            SiftDown(0);
        }

        public DataVoRow[] DrainNearestFirst()
        {
            var result = new DataVoRow[_count];
            for (int i = _count - 1; i >= 0; i--)
            {
                result[i] = _rows[0];
                _count--;
                if (_count > 0)
                {
                    Set(0, _distances[_count], _orders[_count], _rows[_count]);
                    SiftDown(0);
                }
            }

            return result;
        }

        // Farther first; among equal distances the later row is "farther", so it is evicted first.
        private bool Above(int a, int b) =>
            _distances[a] > _distances[b] || (_distances[a] == _distances[b] && _orders[a] > _orders[b]);

        private void Set(int at, double distance, int order, DataVoRow row)
        {
            _distances[at] = distance;
            _orders[at] = order;
            _rows[at] = row;
        }

        private void Swap(int a, int b)
        {
            (_distances[a], _distances[b]) = (_distances[b], _distances[a]);
            (_orders[a], _orders[b]) = (_orders[b], _orders[a]);
            (_rows[a], _rows[b]) = (_rows[b], _rows[a]);
        }

        private void SiftUp(int at)
        {
            while (at > 0)
            {
                int parent = (at - 1) / 2;
                if (!Above(at, parent))
                {
                    return;
                }

                Swap(at, parent);
                at = parent;
            }
        }

        private void SiftDown(int at)
        {
            while (true)
            {
                int left = (2 * at) + 1;
                if (left >= _count)
                {
                    return;
                }

                int right = left + 1;
                int top = right < _count && Above(right, left) ? right : left;
                if (!Above(top, at))
                {
                    return;
                }

                Swap(at, top);
                at = top;
            }
        }
    }
}
