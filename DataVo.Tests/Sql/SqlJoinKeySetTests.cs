using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Utils;

namespace DataVo.Tests.Sql;

/// <summary>
/// A joined table that supplies nothing but its join key (an anti-join, a semi-join, <c>COUNT(c.Id)</c>) is kept
/// as a set of keys. When the keys are unique and close together, as integer primary keys are, the set is one
/// bit per key of their range instead of a hash table; otherwise key counts are hashed as before. These tests
/// run the same joins over keys of every kind (consecutive, with gaps, negative, far apart, at the ends of
/// BIGINT, repeated) and compare the answers with LINQ over the seed data, on InMemory and Lsm.
/// </summary>
public sealed class SqlJoinKeySetTests
{
    public static TheoryData<StorageMode, string> ModesAndKeys
    {
        get
        {
            var data = new TheoryData<StorageMode, string>();
            foreach (StorageMode mode in new[] { StorageMode.InMemory, StorageMode.Lsm })
            {
                foreach (string keys in KeyKinds.Keys)
                {
                    data.Add(mode, keys);
                }
            }

            return data;
        }
    }

    // The customer keys of each kind; orders refer to these, to keys next to them, and to no customer.
    private static readonly Dictionary<string, long[]> KeyKinds = new()
    {
        ["consecutive"] = [.. Enumerable.Range(1, 300).Select(i => (long)i)],
        ["gaps"] = [.. Enumerable.Range(1, 300).Where(i => i % 7 != 0 && i % 11 != 0).Select(i => (long)i)],
        ["negative"] = [.. Enumerable.Range(-150, 300).Where(i => i % 5 != 0).Select(i => (long)i)],
        ["far apart"] = [.. Enumerable.Range(1, 120).Select(i => i * 1_000_003L)],
        ["bigint ends"] = [long.MinValue + 1, long.MinValue + 2, -3, 0, 4, long.MaxValue - 1, long.MaxValue],
        ["one"] = [42],
        ["none"] = [],
    };

    private sealed record Order(int Id, long? CustomerId, double Amount);

    [Theory]
    [MemberData(nameof(ModesAndKeys))]
    public void KeyOnlyJoins_MatchLinqForEveryKindOfKey(StorageMode mode, string kind)
    {
        long[] customers = KeyKinds[kind];
        Order[] orders = OrdersFor(customers);
        HashSet<long> known = [.. customers];
        bool Matched(Order o) => o.CustomerId is { } id && known.Contains(id);

        WithSchema(mode, customers, orders, context =>
        {
            const string left = " FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id";
            const string inner = " FROM Orders o JOIN Customers c ON o.CustomerId = c.Id";

            // The anti-join and its complement.
            AssertTotals(context, "SELECT COUNT(*)" + left + " WHERE c.Id IS NULL", orders.Count(o => !Matched(o)));
            AssertTotals(context, "SELECT COUNT(*)" + left + " WHERE c.Id IS NOT NULL", orders.Count(Matched));
            AssertTotals(context, "SELECT COUNT(*), COUNT(c.Id)" + left, orders.Length, orders.Count(Matched));
            AssertTotals(context, "SELECT COUNT(*)" + left + " WHERE c.Id IS NULL AND o.Amount >= 50", orders.Count(o => !Matched(o) && o.Amount >= 50));

            // The semi-join as an INNER JOIN, with an aggregate over the streamed table.
            AssertTotals(context, "SELECT COUNT(*)" + inner, orders.Count(Matched));
            if (orders.Any(Matched))
            {
                AssertTotals(context, "SELECT COUNT(*), SUM(o.Amount)" + inner, orders.Count(Matched), orders.Where(Matched).Sum(o => o.Amount));
            }

            // Rows of the anti-join, in key order.
            QueryResult rows = Run(context, "SELECT o.Id" + left + " WHERE c.Id IS NULL ORDER BY o.Id LIMIT 25");
            Assert.Equal(
                orders.Where(o => !Matched(o)).OrderBy(o => o.Id).Take(25).Select(o => o.Id),
                rows.Data.Select(row => Convert.ToInt32(row.Values.Single(), CultureInfo.InvariantCulture)));

            // The key itself read back from the key-only side.
            if (orders.Any(Matched))
            {
                QueryResult extremes = Run(context, "SELECT MIN(c.Id), MAX(c.Id)" + inner);
                Assert.Equal(
                    [orders.Where(Matched).Min(o => o.CustomerId!.Value), orders.Where(Matched).Max(o => o.CustomerId!.Value)],
                    Assert.Single(extremes.Data).Values.Select(value => Convert.ToInt64(value, CultureInfo.InvariantCulture)));
            }
        });
    }

    /// <summary>A key column with repeated values is not a set: every matching row still counts.</summary>
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void RepeatedKeys_KeepTheirMultiplicity(StorageMode mode)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_join_key_set_repeat") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE KeySetRepeat");
        Execute(context, "USE KeySetRepeat");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, Amount DOUBLE)");
        Execute(context, "CREATE TABLE Payments (Id INT PRIMARY KEY, OrderId INT)");
        Execute(context, "INSERT INTO Orders (Id, Amount) VALUES " + string.Join(", ", Enumerable.Range(1, 200).Select(i => $"({i}, {i}.5)")));

        // Orders 1-150 have (Id % 3) payments each: none, one or two; payment 999 has no order.
        (int Id, int? OrderId)[] payments = [.. Enumerable.Range(1, 150).SelectMany(order => Enumerable.Range(0, order % 3).Select(n => (order * 10 + n, (int?)order))), (9_990, 999), (9_991, null)];
        Execute(context, "INSERT INTO Payments (Id, OrderId) VALUES " + string.Join(", ", payments.Select(p => $"({p.Id}, {p.OrderId?.ToString(CultureInfo.InvariantCulture) ?? "NULL"})")));

        context.Diagnostics.Enabled = true;
        int paid(int order) => payments.Count(p => p.OrderId == order);
        AssertTotals(context, "SELECT COUNT(*) FROM Orders o JOIN Payments p ON o.Id = p.OrderId", Enumerable.Range(1, 200).Sum(paid));
        AssertTotals(context, "SELECT COUNT(*), COUNT(p.OrderId) FROM Orders o LEFT JOIN Payments p ON o.Id = p.OrderId",
            Enumerable.Range(1, 200).Sum(order => Math.Max(1, paid(order))), Enumerable.Range(1, 200).Sum(paid));
        AssertTotals(context, "SELECT COUNT(*) FROM Orders o LEFT JOIN Payments p ON o.Id = p.OrderId WHERE p.OrderId IS NULL",
            Enumerable.Range(1, 200).Count(order => paid(order) == 0));
    }

    /// <summary>
    /// The anti-join over consecutive integer keys keeps one bit per key (2.5 KB for 20,000 customers) where it
    /// used to hash every key (about 500 KB). Measured against the same join with one far-away key added, which
    /// makes the keys too sparse for a bit set, so they are hashed as before.
    /// </summary>
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void AntiJoinOverDenseKeys_DoesNotHashTheKeys(StorageMode mode)
    {
        long[] dense = [.. Enumerable.Range(1, 20_000).Select(i => (long)i)];
        long[] sparse = [.. dense, 1_000_000_000_000L];
        Order[] orders = [.. Enumerable.Range(1, 30_000).Select(i => new Order(i, i % 9 == 0 ? null : i * 7919L % 26_000 + 1, i % 100))];
        int orphans = orders.Count(o => o.CustomerId is not { } id || id > 20_000);

        long denseBytes = AntiJoinBytes(mode, dense, orders, orphans);
        long hashedBytes = AntiJoinBytes(mode, sparse, orders, orphans);
        Assert.True(hashedBytes - denseBytes > 300_000, $"{mode}: dense keys {denseBytes:N0} B, hashed keys {hashedBytes:N0} B");
    }

    /// <summary>The bytes one run of the anti-join allocates over the given customers, after checking its answer.</summary>
    private static long AntiJoinBytes(StorageMode mode, long[] customers, Order[] orders, int orphans)
    {
        long bytes = 0;
        WithSchema(mode, customers, orders, context =>
        {
            context.Diagnostics.Enabled = false;
            DataVoPreparedStatement statement = context.Prepare("SELECT COUNT(*) FROM Orders o LEFT JOIN Customers c ON o.CustomerId = c.Id WHERE c.Id IS NULL");
            Assert.False(statement.Execute().IsError);
            long before = GC.GetAllocatedBytesForCurrentThread();
            QueryResult result = statement.Execute();
            bytes = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.False(result.IsError, string.Join("; ", result.Messages));
            Assert.Equal(orphans, Convert.ToInt32(Assert.Single(result.Data).Values.Single(), CultureInfo.InvariantCulture));
        });

        return bytes;
    }

    [Fact]
    public void DenseInt64Set_HoldsDistinctNearbyKeysAndDeclinesTheRest()
    {
        long[] keys = [5, 9, -3, 64, 63, 1_000];
        DenseInt64Set set = Assert.IsType<DenseInt64Set>(DenseInt64Set.TryCreate(keys));
        Assert.Equal(6, set.Count);
        Assert.All(keys, key => Assert.True(set.Contains(key)));
        Assert.All(new long[] { -4, 0, 6, 62, 65, 999, 1_001, long.MinValue, long.MaxValue }, key => Assert.False(set.Contains(key)));
        Assert.Equal(keys.Order(), set.Keys());

        // The ends of the range, and an empty set.
        DenseInt64Set ends = Assert.IsType<DenseInt64Set>(DenseInt64Set.TryCreate([long.MaxValue, long.MaxValue - 100]));
        Assert.True(ends.Contains(long.MaxValue) && ends.Contains(long.MaxValue - 100));
        Assert.False(ends.Contains(long.MaxValue - 1) || ends.Contains(long.MinValue) || ends.Contains(0));
        DenseInt64Set empty = Assert.IsType<DenseInt64Set>(DenseInt64Set.TryCreate([]));
        Assert.False(empty.Contains(0));
        Assert.Empty(empty.Keys());

        // A repeated key, keys too far apart, and a range wider than a long can measure are declined.
        Assert.Null(DenseInt64Set.TryCreate([1, 2, 2]));
        Assert.Null(DenseInt64Set.TryCreate([1, 5_000]));
        Assert.Null(DenseInt64Set.TryCreate([long.MinValue, long.MaxValue]));
        Assert.NotNull(DenseInt64Set.TryCreate([.. Enumerable.Range(0, 100).Select(i => i * 60L)]));
        Assert.Null(DenseInt64Set.TryCreate([.. Enumerable.Range(0, 100).Select(i => i * 70L)]));
    }

    /// <summary>Orders that refer to each customer key, to the keys just beside it, and to no customer.</summary>
    private static Order[] OrdersFor(long[] customers)
    {
        var orders = new List<Order>();
        int id = 0;
        foreach (long key in customers)
        {
            orders.Add(new Order(++id, key, id % 100));
            if (key < long.MaxValue)
            {
                orders.Add(new Order(++id, key + 1, id % 100));
            }

            if (key > long.MinValue)
            {
                orders.Add(new Order(++id, key - 1, id % 100));
            }
        }

        foreach (long? other in new long?[] { null, null, 0, -1, 1, long.MinValue, long.MaxValue, 123_456_789_012L })
        {
            orders.Add(new Order(++id, other, id % 100));
        }

        return [.. orders];
    }

    private static void AssertTotals(DataVoContext context, string sql, params double[] want)
    {
        QueryResult result = Run(context, sql);
        double[] got = [.. Assert.Single(result.Data).Values.Select(value => Convert.ToDouble(value ?? double.NaN, CultureInfo.InvariantCulture))];
        Assert.True(want.Length == got.Length && want.Zip(got).All(pair => Math.Abs(pair.First - pair.Second) < 1e-6),
            $"{sql}: expected [{string.Join(", ", want)}], got [{string.Join(", ", got)}]");

        // Answered by the typed join: the general join would have recorded an execution.
        Assert.Equal(0, context.Diagnostics.LastQuery!.JoinExecutions);
    }

    private static QueryResult Run(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    private static void WithSchema(StorageMode mode, long[] customers, Order[] orders, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_join_key_set") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        Execute(context, "CREATE DATABASE JoinKeySet");
        Execute(context, "USE JoinKeySet");
        Execute(context, "CREATE TABLE Customers (Id BIGINT PRIMARY KEY, Name VARCHAR(8))");
        Execute(context, "CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId BIGINT, Amount DOUBLE)");
        foreach (long[] chunk in customers.Chunk(2_000))
        {
            Execute(context, "INSERT INTO Customers (Id, Name) VALUES " + string.Join(", ", chunk.Select(id => $"({id}, 'c')")));
        }

        foreach (Order[] chunk in orders.Chunk(2_000))
        {
            Execute(context, "INSERT INTO Orders (Id, CustomerId, Amount) VALUES " + string.Join(", ", chunk.Select(o =>
                $"({o.Id}, {o.CustomerId?.ToString(CultureInfo.InvariantCulture) ?? "NULL"}, {o.Amount.ToString(CultureInfo.InvariantCulture)})")));
        }

        context.Diagnostics.Enabled = true;
        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql[..Math.Min(sql.Length, 140)] + ": " + string.Join("; ", result.Messages));
    }
}
