using System.Numerics;
using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Models.Catalog;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.Spatial;

[DataVoTable("ships")]
public sealed class Ship
{
    [PrimaryKey] public int Id { get; set; }

    public string Name { get; set; } = "";

    [SpatialIndex] public Vector3 Position { get; set; }
}

/// <summary>A model with two position columns, one nullable, and no spatial index.</summary>
[DataVoTable("beacons")]
public sealed class Beacon
{
    [PrimaryKey] public int Id { get; set; }

    public Vector3 Origin { get; set; }

    public Vector3? Target { get; set; }
}

[DataVoTable("plain_rows")]
public sealed class PlainRow
{
    [PrimaryKey] public int Id { get; set; }

    public int Value { get; set; }
}

/// <summary>
/// Sphere, box and nearest-neighbor queries over <see cref="Vector3"/> columns return exactly what the same test
/// computed over every row in memory returns, in every storage mode, with and without an HNSW index.
/// </summary>
public sealed class SpatialQueryTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_spatial_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Disk, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Vector3Column_RoundTripsEveryFloatBitForBit(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureShipTable();
        var points = new[]
        {
            new Vector3(0.1f, -0.2f, 0.3f),
            new Vector3(1e-7f, 16_777_217f, -3.4028235e38f),
            new Vector3(float.Epsilon, -float.Epsilon, 123456.789f),
            new Vector3(-0f, 0f, 1f / 3f),
        };
        for (int i = 0; i < points.Length; i++)
        {
            db.Insert(new Ship { Id = i + 1, Name = $"s{i}", Position = points[i] });
        }

        for (int i = 0; i < points.Length; i++)
        {
            Ship ship = db.GetShipById(i + 1)!;
            AssertSameBits(points[i], ship.Position);
        }

        Assert.True(db.Update(new Ship { Id = 2, Name = "moved", Position = new Vector3(7, 8, 9) }));
        Assert.Equal(new Vector3(7, 8, 9), db.GetShipById(2)!.Position);
        Assert.Equal(new Vector3(7, 8, 9), db.Table<Ship>().FirstOrDefault(2, static (s, id) => s.Id == id)!.Position);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void WithinRadius_MatchesTheInMemoryAnswer(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        List<Ship> all = SeedShips(db, 600, seed: 1);
        var center = new Vector3(10, -5, 20);

        foreach (float radius in new[] { 0f, 1f, 15f, 40f, 1_000f })
        {
            int[] expected = [.. all.Where(s => Vector3.Distance(s.Position, center) <= radius).Select(static s => s.Id).Order()];
            int[] actual = [.. db.Table<Ship>().WithinRadius(center, radius).Select(static s => s.Id).Order()];
            Assert.Equal(expected, actual);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void WithinRadius_IncludesPointsExactlyOnTheSphere(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureShipTable();
        db.Insert(new Ship { Id = 1, Name = "on", Position = new Vector3(3, 4, 0) });          // distance exactly 5
        db.Insert(new Ship { Id = 2, Name = "past", Position = new Vector3(3, 4.0001f, 0) });  // just outside
        db.Insert(new Ship { Id = 3, Name = "center", Position = Vector3.Zero });
        db.Insert(new Ship { Id = 4, Name = "far-on", Position = new Vector3(1_000_003, 1_000_004, 0) });

        Assert.Equal([1, 3], db.Table<Ship>().WithinRadius(Vector3.Zero, 5f).Select(static s => s.Id).Order());
        Assert.Equal([3], db.Table<Ship>().WithinRadius(Vector3.Zero, 0f).Select(static s => s.Id));

        // Far from the origin single precision cannot tell 5 from its neighbors; the test runs in double.
        Assert.Equal([4], db.Table<Ship>().WithinRadius(new Vector3(1_000_000, 1_000_000, 0), 5f).Select(static s => s.Id));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void WithinBoundingBox_MatchesTheInMemoryAnswer(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        List<Ship> all = SeedShips(db, 600, seed: 2);
        var min = new Vector3(-20, -30, -10);
        var max = new Vector3(25, 5, 40);

        int[] expected = [.. all.Where(s => s.Position.X >= min.X && s.Position.X <= max.X
                                         && s.Position.Y >= min.Y && s.Position.Y <= max.Y
                                         && s.Position.Z >= min.Z && s.Position.Z <= max.Z)
                                .Select(static s => s.Id).Order()];
        Assert.NotEmpty(expected);
        Assert.Equal(expected, db.Table<Ship>().WithinBoundingBox(min, max).Select(static s => s.Id).Order());

        // The corners may come in either order.
        Assert.Equal(expected, db.Table<Ship>().WithinBoundingBox(max, min).Select(static s => s.Id).Order());
        Assert.Equal(expected, db.Table<Ship>().WithinBoundingBox(new BoundingBox3(max, min)).Select(static s => s.Id).Order());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void WithinBoundingBox_IncludesTheSurfaceAndEdges(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureShipTable();
        db.Insert(new Ship { Id = 1, Name = "corner", Position = new Vector3(0, 0, 0) });
        db.Insert(new Ship { Id = 2, Name = "face", Position = new Vector3(10, 5, 5) });
        db.Insert(new Ship { Id = 3, Name = "outside", Position = new Vector3(10.001f, 5, 5) });
        db.Insert(new Ship { Id = 4, Name = "inside", Position = new Vector3(5, 5, 5) });

        Assert.Equal([1, 2, 4], db.Table<Ship>().WithinBoundingBox(Vector3.Zero, new Vector3(10)).Select(static s => s.Id).Order());

        // A flat box still holds the points on its plane.
        Assert.Equal([2, 4], db.Table<Ship>().WithinBoundingBox(new Vector3(0, 0, 5), new Vector3(10, 10, 5)).Select(static s => s.Id).Order());
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void NearestNeighbors_WithHnswIndex_FindsTheTrueNeighbors(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        List<Ship> all = SeedShips(db, 2_000, seed: 3);
        Assert.Equal(IndexFile.EuclideanMetric, SpatialIndex(db, "ships").EffectiveMetric);

        var random = new Random(30);
        int found = 0;
        int wanted = 0;
        for (int query = 0; query < 50; query++)
        {
            Vector3 target = RandomPoint(random, 120);
            foreach (int k in new[] { 1, 5, 20 })
            {
                double[] truth = [.. all.Select(s => Spatial3.DistanceSquaredPrecise(s.Position, target)).Order().Take(k)];
                double[] approximate = [.. db.Table<Ship>().NearestNeighbors(target, k).Select(s => Spatial3.DistanceSquaredPrecise(s.Position, target))];
                double[] exact = [.. db.Table<Ship>().NearestNeighbors(target, k, exact: true).Select(s => Spatial3.DistanceSquaredPrecise(s.Position, target))];

                Assert.Equal(truth, exact);
                Assert.Equal(k, approximate.Length);
                Assert.Equal(approximate.Order(), approximate); // nearest first
                found += approximate.Count(distance => distance <= truth[^1]);
                wanted += k;
            }
        }

        // HNSW is approximate; ranking twice the candidates by exact distance recovers nearly every miss.
        double recall = (double)found / wanted;
        Assert.True(recall >= 0.98, $"HNSW recall was {recall:P2}");
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void NearestNeighbors_WithoutIndex_IsExactAndKeepsStorageOrderForTies(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureBeaconTable();
        var random = new Random(4);
        var all = new List<Beacon>();
        for (int id = 1; id <= 500; id++)
        {
            var beacon = new Beacon { Id = id, Origin = RandomPoint(random, 50), Target = id % 3 == 0 ? null : RandomPoint(random, 50) };
            all.Add(beacon);
            db.Insert(beacon);
        }

        Vector3 probe = new(1, 2, 3);
        foreach (int k in new[] { 1, 7, 499, 500, 2_000 })
        {
            int[] expected = [.. all.OrderBy(b => Spatial3.DistanceSquaredPrecise(b.Origin, probe)).ThenBy(static b => b.Id).Take(k).Select(static b => b.Id)];
            Assert.Equal(expected, db.Table<Beacon>().NearestNeighbors(probe, k, "Origin").Select(static b => b.Id));
        }

        // Rows whose position is NULL are never neighbors.
        int[] withTarget = [.. all.Where(static b => b.Target is not null)
            .OrderBy(b => Spatial3.DistanceSquaredPrecise(b.Target!.Value, probe)).ThenBy(static b => b.Id).Select(static b => b.Id)];
        Assert.Equal(withTarget, db.Table<Beacon>().NearestNeighbors(probe, 1_000, "target").Select(static b => b.Id));

        // Equal distances come back in storage order.
        db.Execute("DELETE FROM beacons");
        for (int id = 1; id <= 6; id++)
        {
            db.Insert(new Beacon { Id = id, Origin = id % 2 == 0 ? new Vector3(1, 0, 0) : new Vector3(0, -1, 0) });
        }

        Assert.Equal([1, 2, 3], db.Table<Beacon>().NearestNeighbors(Vector3.Zero, 3, "Origin").Select(static b => b.Id));
    }

    [Fact]
    public void NearestNeighbors_InsideATransaction_SeesUncommittedRows()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        SeedShips(db, 200, seed: 5);
        Assert.False(db.Execute("BEGIN").Single().IsError);
        db.Insert(new Ship { Id = 10_000, Name = "fresh", Position = new Vector3(500, 500, 500) });

        Assert.Equal(10_000, db.Table<Ship>().NearestNeighbors(new Vector3(501, 500, 500), 1).Single().Id);
        Assert.False(db.Execute("ROLLBACK").Single().IsError);

        Assert.NotEqual(10_000, db.Table<Ship>().NearestNeighbors(new Vector3(501, 500, 500), 1).Single().Id);
    }

    [Theory]
    [InlineData(StorageMode.Disk)]
    [InlineData(StorageMode.Lsm)]
    public void SpatialIndex_KeepsItsMetricAcrossReopen(StorageMode mode)
    {
        List<Ship> all;
        using (DataVoContext db = Open(mode))
        {
            all = SeedShips(db, 800, seed: 6);
        }

        using var reopened = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = _directory });
        reopened.Execute("USE Space");
        reopened.EnsureShipTable(); // finds the existing Euclidean index and leaves it
        Assert.Equal(IndexFile.EuclideanMetric, SpatialIndex(reopened, "ships").EffectiveMetric);

        var target = new Vector3(-12, 33, 7);
        double[] expected = [.. all.Select(s => Spatial3.DistanceSquaredPrecise(s.Position, target)).Order().Take(10)];
        double[] viaIndex = [.. reopened.Table<Ship>().NearestNeighbors(target, 10).Select(s => Spatial3.DistanceSquaredPrecise(s.Position, target))];
        Assert.True(viaIndex.Count(distance => distance <= expected[^1]) >= 9, "the reopened index lost its neighbors");
        double[] exact = [.. reopened.Table<Ship>().NearestNeighbors(target, 10, exact: true).Select(s => Spatial3.DistanceSquaredPrecise(s.Position, target))];
        Assert.Equal(expected, exact);
    }

    [Fact]
    public void Ensure_RejectsAnIndexWithTheSpatialNameButTheWrongMetric()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        AssertOk(db, "CREATE TABLE ships (Id INT PRIMARY KEY, Name VARCHAR NOT NULL, Position VEC3 NOT NULL)");
        AssertOk(db, "CREATE INDEX sx_ships_Position ON ships (Position) USING HNSW");

        var error = Assert.Throws<InvalidOperationException>(db.EnsureShipTable);
        Assert.Contains("Euclidean HNSW", error.Message);
    }

    [Fact]
    public void NearestNeighbors_UsesAEuclideanIndexCreatedInSql_AndIgnoresACosineOne()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        db.EnsureBeaconTable();
        var random = new Random(7);
        var all = new List<Beacon>();
        for (int id = 1; id <= 400; id++)
        {
            var beacon = new Beacon { Id = id, Origin = RandomPoint(random, 80) };
            all.Add(beacon);
            db.Insert(beacon);
        }

        // Cosine ranks by direction, not distance; using it would return the wrong rows, so it is ignored.
        AssertOk(db, "CREATE INDEX ix_origin_cos ON beacons (Origin) USING HNSW");
        Vector3 probe = new(5, 5, 5);
        int[] expected = [.. all.OrderBy(b => Spatial3.DistanceSquaredPrecise(b.Origin, probe)).ThenBy(static b => b.Id).Take(8).Select(static b => b.Id)];
        Assert.Equal(expected, db.Table<Beacon>().NearestNeighbors(probe, 8, "Origin").Select(static b => b.Id));

        AssertOk(db, "CREATE INDEX ix_origin_l2 ON beacons (Origin) USING HNSW WITH (metric = 'l2')");
        Assert.Equal(IndexFile.EuclideanMetric, db.Engine.Catalog.GetTableIndexes("beacons", "Space").Single(static i => i.IndexFileName == "ix_origin_l2").Metric);

        // The index answers approximately, so allow it one miss.
        int[] viaIndex = [.. db.Table<Beacon>().NearestNeighbors(probe, 8, "Origin").Select(static b => b.Id)];
        Assert.True(viaIndex.Intersect(expected).Count() >= 7, $"expected {string.Join(",", expected)}, got {string.Join(",", viaIndex)}");
        Assert.Equal(expected, db.Table<Beacon>().NearestNeighbors(probe, 8, "Origin", exact: true).Select(static b => b.Id));
    }

    [Fact]
    public void Sql_DeclaresThreeDimensionalColumnsAsVec3OrFloatArray()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        AssertOk(db, "CREATE TABLE p (Id INT PRIMARY KEY, A VEC3, B FLOAT[3], C REAL[2], D VEC4, E VECTOR(3))");
        IReadOnlyList<Column> columns = db.Engine.Catalog.GetTableColumns("p", "Space");
        Assert.All(columns.Skip(1), static column => Assert.Equal("VECTOR", column.Type, ignoreCase: true));
        Assert.Equal([3, 3, 2, 4, 3], columns.Skip(1).Select(static column => column.Length));

        AssertOk(db, "INSERT INTO p (Id, A, B, C, D, E) VALUES (1, '[1,2,3]', '[4,5,6]', '[7,8]', '[1,2,3,4]', '[0,0,1]')");
        Assert.True(db.Execute("INSERT INTO p (Id, A) VALUES (2, '[1,2]')")[^1].IsError);

        Assert.True(db.Execute("CREATE TABLE q (Id INT PRIMARY KEY, A FLOAT[0])")[^1].IsError);
        Assert.True(db.Execute("CREATE TABLE q (Id INT PRIMARY KEY, A FLOAT[3)")[^1].IsError);
    }

    [Fact]
    public void Sql_ValidatesIndexOptions()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        AssertOk(db, "CREATE TABLE p (Id INT PRIMARY KEY, A VEC3, N INT)");

        Assert.True(db.Execute("CREATE INDEX i1 ON p (A) USING HNSW WITH (metric = 'manhattan')")[^1].IsError);
        Assert.True(db.Execute("CREATE INDEX i2 ON p (A) USING HNSW WITH (size = 3)")[^1].IsError);
        Assert.True(db.Execute("CREATE INDEX i3 ON p (N) WITH (metric = 'l2')")[^1].IsError);
        Assert.Empty(db.Engine.Catalog.GetTableIndexes("p", "Space").Where(static index => index.IndexFileName.StartsWith('i')));

        AssertOk(db, "CREATE INDEX i4 ON p (A) USING HNSW WITH (metric = euclidean)");
        Assert.Equal(IndexFile.EuclideanMetric, db.Engine.Catalog.GetTableIndexes("p", "Space").Single(static i => i.IndexFileName == "i4").Metric);
    }

    [Fact]
    public void SpatialQueries_ExplainWhatIsWrongWithTheirArguments()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        db.EnsureShipTable();
        db.EnsureBeaconTable();
        db.EnsurePlainRowTable();
        DataVoTable<Ship> ships = db.Table<Ship>();

        Assert.Throws<ArgumentOutOfRangeException>(() => ships.WithinRadius(Vector3.Zero, -1f));
        Assert.Throws<ArgumentOutOfRangeException>(() => ships.WithinRadius(Vector3.Zero, float.NaN));
        Assert.Throws<ArgumentException>(() => ships.WithinRadius(new Vector3(float.NaN, 0, 0), 1f));
        Assert.Throws<ArgumentException>(() => ships.WithinBoundingBox(new Vector3(float.NaN), Vector3.One));
        Assert.Throws<ArgumentOutOfRangeException>(() => ships.NearestNeighbors(Vector3.Zero, -1));
        Assert.Throws<ArgumentException>(() => ships.NearestNeighbors(new Vector3(float.PositiveInfinity), 1));
        Assert.Throws<ArgumentException>(() => ships.WithinRadius(Vector3.Zero, 1f, "Name"));
        Assert.Empty(ships.NearestNeighbors(Vector3.Zero, 0));

        var ambiguous = Assert.Throws<InvalidOperationException>(() => db.Table<Beacon>().WithinRadius(Vector3.Zero, 1f));
        Assert.Contains("Origin", ambiguous.Message);
        var none = Assert.Throws<InvalidOperationException>(() => db.Table<PlainRow>().NearestNeighbors(Vector3.Zero, 1));
        Assert.Contains("no 3D position column", none.Message);
    }

    [Fact]
    public void LambdaOverAPosition_FallsBackToAScanAndStaysCorrect()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        List<Ship> all = SeedShips(db, 100, seed: 8);
        Vector3 wanted = all[42].Position;

        var exact = db.Table<Ship>().Where(wanted, static (s, p) => s.Position == p);
        Assert.False(exact.IsCompiled);
        Assert.Equal([all[42].Id], exact.Select(static s => s.Id));

        var byAxis = db.Table<Ship>().Where(0f, static (s, x) => s.Position.X > x);
        Assert.Equal(all.Count(static s => s.Position.X > 0f), byAxis.Count());
    }

    [Fact]
    public void Spatial3_ComputesDistancesPrecisely()
    {
        Assert.Equal(5f, Spatial3.Distance(Vector3.Zero, new Vector3(3, 4, 0)));
        Assert.Equal(25f, Spatial3.DistanceSquared(new Vector3(1, 1, 1), new Vector3(4, 5, 1)));
        Assert.Equal(1e-14, Spatial3.DistanceSquaredPrecise(new Vector3(1e-7f, 0, 0), Vector3.Zero), 1e-20);

        // Two points 1 apart far from the origin: single precision squares lose them, double does not.
        var a = new Vector3(16_777_216f, 0, 0);
        var b = new Vector3(16_777_218f, 0, 0);
        Assert.Equal(4d, Spatial3.DistanceSquaredPrecise(a, b));
        Assert.True(Spatial3.IsWithinRadius(a, b, 2f));
        Assert.False(Spatial3.IsWithinRadius(a, b, 1.9999999f));

        Assert.True(Spatial3.IsWithinRadius([3f, 4f, 0f], Vector3.Zero, 5f));
        Assert.Throws<ArgumentException>(() => Spatial3.IsWithinRadius([1f, 2f], Vector3.Zero, 5f));
        Assert.Equal(new Vector3(1, 2, 3), Spatial3.ToVector3([1f, 2f, 3f]));

        Span<float> written = stackalloc float[3];
        Spatial3.CopyTo(new Vector3(7, 8, 9), written);
        Assert.Equal([7f, 8f, 9f], written.ToArray());
    }

    [Fact]
    public void BoundingBox3_NormalizesCornersAndTestsContainment()
    {
        var box = new BoundingBox3(new Vector3(5, -1, 2), new Vector3(-5, 1, -2));
        Assert.Equal(new Vector3(-5, -1, -2), box.Min);
        Assert.Equal(new Vector3(5, 1, 2), box.Max);
        Assert.Equal(Vector3.Zero, box.Center);
        Assert.Equal(new Vector3(10, 2, 4), box.Size);

        Assert.True(box.Contains(new Vector3(5, 1, 2)));
        Assert.False(box.Contains(new Vector3(5.0001f, 0, 0)));
        Assert.True(box.Contains(new BoundingBox3(Vector3.Zero, Vector3.One)));
        Assert.False(box.Contains(new BoundingBox3(Vector3.Zero, new Vector3(6, 0, 0))));

        Assert.True(box.Intersects(new BoundingBox3(new Vector3(5, 1, 2), new Vector3(9))));
        Assert.False(box.Intersects(new BoundingBox3(new Vector3(6), new Vector3(9))));
        Assert.True(box.Intersects(new Vector3(8, 0, 0), 3f));
        Assert.False(box.Intersects(new Vector3(8, 0, 0), 2.9f));

        Assert.Equal(new Vector3(5, 0, 0), box.ClosestPoint(new Vector3(9, 0, 0)));
        Assert.Equal(16f, box.DistanceSquared(new Vector3(9, 0, 0)));
        Assert.Equal(0f, box.DistanceSquared(Vector3.Zero));
        Assert.Equal(new Vector3(9, 1, 2), box.Encapsulate(new Vector3(9, 0, 0)).Max);

        Assert.Equal(new BoundingBox3(new Vector3(-2), new Vector3(2)), BoundingBox3.AroundSphere(Vector3.Zero, 2f));
        Assert.Equal(new BoundingBox3(new Vector3(0, 0, 0), new Vector3(2, 4, 6)), BoundingBox3.FromCenter(new Vector3(1, 2, 3), new Vector3(-1, 2, -3)));
        Assert.Throws<ArgumentException>(() => new BoundingBox3(new Vector3(float.NaN), Vector3.One));
    }

    [Fact]
    public void DistanceAndBoxMath_AllocatesNothing()
    {
        var box = new BoundingBox3(new Vector3(-10), new Vector3(10));
        float[] stored = [1f, 2f, 3f];
        var center = new Vector3(0.5f, 0.25f, -0.75f);
        RunMath(box, stored, center, 1_000); // JIT the path first

        long before = GC.GetAllocatedBytesForCurrentThread();
        double sink = RunMath(box, stored, center, 200_000);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(0, allocated);
        Assert.NotEqual(0d, sink);
    }

    [Fact]
    public void RejectedRows_AreNeverMaterialized()
    {
        using DataVoContext db = Open(StorageMode.Lsm);
        SeedShips(db, 4_000, seed: 9);
        var nowhere = new Vector3(1e6f);
        var everywhere = Vector3.Zero;

        Assert.Empty(db.Table<Ship>().WithinRadius(nowhere, 1f)); // warm both paths
        Assert.Equal(4_000, db.Table<Ship>().WithinRadius(everywhere, 1e5f).Count());

        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.Empty(db.Table<Ship>().WithinRadius(nowhere, 1f));
        long rejectAll = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(4_000, db.Table<Ship>().WithinRadius(everywhere, 1e5f).Count());
        long acceptAll = GC.GetAllocatedBytesForCurrentThread() - before;

        // An accepted row costs a Ship (header, Id, Name reference, 12-byte Vector3: about 40 B); a rejected
        // row is dropped before one is built, so it costs nothing beyond the scan itself.
        long perRowSaving = (acceptAll - rejectAll) / 4_000;
        Assert.True(perRowSaving >= 32, $"rejecting rows saved only {perRowSaving} B per row ({rejectAll} B vs {acceptAll} B)");
    }

    [Fact]
    public void WithinRadius_KeepsTheLiveHeapFlatWhileEnumerating()
    {
        const int rows = 20_000;
        using DataVoContext db = Open(StorageMode.Lsm);
        db.EnsureShipTable();
        for (int start = 1; start <= rows; start += 500)
        {
            IEnumerable<string> values = Enumerable.Range(start, 500)
                .Select(static id => $"({id}, 'ship number {id}', '[{id % 100},{id % 37},{id % 11}]')");
            AssertOk(db, $"INSERT INTO ships (Id, Name, Position) VALUES {string.Join(", ", values)}");
        }

        IEnumerable<Ship> query = db.Table<Ship>().WithinRadius(Vector3.Zero, 1_000f);
        Assert.Equal(rows, query.Count());

        long baseline = LiveBytes();
        long peak = 0;
        int seen = 0;
        foreach (Ship _ in query)
        {
            if (++seen % 4_000 == 0)
            {
                peak = Math.Max(peak, LiveBytes() - baseline);
            }
        }

        Assert.Equal(rows, seen);
        Assert.True(peak < 1_000_000, $"live heap grew by {peak} bytes while enumerating");
    }

    private static double RunMath(BoundingBox3 box, float[] stored, Vector3 center, int iterations)
    {
        double sum = 0;
        for (int i = 0; i < iterations; i++)
        {
            var point = new Vector3(i * 0.001f, -i * 0.002f, 1f);
            sum += Spatial3.Distance(point, center);
            sum += Spatial3.DistanceSquared(point, center);
            sum += Spatial3.DistanceSquaredPrecise(point, center);
            sum += Spatial3.DistanceSquaredPrecise(stored, point);
            sum += Spatial3.IsWithinRadius(stored, center, 4f) ? 1 : 0;
            sum += Spatial3.IsWithinRadius(point, center, 4f) ? 1 : 0;
            sum += Spatial3.IsInside(stored, box) ? 1 : 0;
            sum += box.Contains(point) ? 1 : 0;
            sum += box.Intersects(point, 2f) ? 1 : 0;
            sum += box.DistanceSquared(point);
            sum += Spatial3.ToVector3(stored).X;
        }

        return sum;
    }

    private static List<Ship> SeedShips(DataVoContext db, int count, int seed)
    {
        db.EnsureShipTable();
        var random = new Random(seed);
        var ships = new List<Ship>(count);
        for (int id = 1; id <= count; id++)
        {
            ships.Add(new Ship { Id = id, Name = $"ship-{id}", Position = RandomPoint(random, 100) });
        }

        // One statement per 500 rows: a durable insert per row would make the seeding dominate the test.
        foreach (Ship[] batch in ships.Chunk(500))
        {
            IEnumerable<string> values = batch.Select(static s => $"({s.Id}, '{s.Name}', '{VectorLiteral(s.Position)}')");
            AssertOk(db, $"INSERT INTO ships (Id, Name, Position) VALUES {string.Join(", ", values)}");
        }

        return ships;
    }

    private static string VectorLiteral(Vector3 point) => string.Create(
        System.Globalization.CultureInfo.InvariantCulture,
        $"[{point.X:R},{point.Y:R},{point.Z:R}]");

    private static Vector3 RandomPoint(Random random, float extent) => new(
        (float)((random.NextDouble() * 2 - 1) * extent),
        (float)((random.NextDouble() * 2 - 1) * extent),
        (float)((random.NextDouble() * 2 - 1) * extent));

    private static IndexFile SpatialIndex(DataVoContext db, string table) =>
        db.Engine.Catalog.GetTableIndexes(table, "Space").Single(static index => index.IndexKind == "HNSW");

    private static void AssertSameBits(Vector3 expected, Vector3 actual)
    {
        Assert.Equal(BitConverter.SingleToInt32Bits(expected.X), BitConverter.SingleToInt32Bits(actual.X));
        Assert.Equal(BitConverter.SingleToInt32Bits(expected.Y), BitConverter.SingleToInt32Bits(actual.Y));
        Assert.Equal(BitConverter.SingleToInt32Bits(expected.Z), BitConverter.SingleToInt32Bits(actual.Z));
    }

    private static void AssertOk(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }

    private static long LiveBytes()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GCMemoryInfo info = GC.GetGCMemoryInfo(GCKind.FullBlocking);
        return info.HeapSizeBytes - info.FragmentedBytes;
    }

    private DataVoContext Open(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
        });
        db.Execute("CREATE DATABASE Space");
        db.Execute("USE Space");
        return db;
    }
}
