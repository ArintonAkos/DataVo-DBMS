namespace Research.Benchmark.Runners.SqlMatrix;

/// <summary>
/// The deterministic data set of the SQL capability matrix: orders, customers and products whose every
/// column is a pure function of the row id. Engines load it, and the oracle answers every query from the same
/// arrays, so each engine's answer is checked against an exact expected result and not only against the
/// other engines.
/// </summary>
/// <remarks>
/// Every floating-point value is a multiple of 0.25 and below 2^20, so any sum over at most a few million of
/// them is exact in a double. Sums therefore compare bit for bit across engines, whatever order they add in.
/// About 25% of orders reference a customer id that does not exist, for the outer-join scenarios.
/// </remarks>
public sealed class SqlMatrixData
{
    /// <summary>The order statuses, upper case so SQLite's case-insensitive LIKE matches DataVo's.</summary>
    public static readonly string[] Statuses = ["NEW", "PAID", "SHIPPED", "CANCELLED"];

    /// <summary>Number of distinct <c>Bucket</c> values (medium-cardinality GROUP BY).</summary>
    public const int Buckets = 1_000;

    /// <summary>Number of distinct customer segments.</summary>
    public const int Segments = 1_000;

    /// <summary>Number of distinct product categories.</summary>
    public const int Categories = 50;

    /// <summary>Number of distinct customer regions.</summary>
    public const int Regions = 8;

    public SqlMatrixData(int orders, int customers, int products)
    {
        if (orders < 1_000 || customers < 100 || products < Categories)
        {
            throw new ArgumentException("The SQL matrix needs at least 1,000 orders, 100 customers and 50 products.");
        }

        Orders = orders;
        Customers = customers;
        Products = products;
        UnmatchedCustomers = Math.Max(1, customers / 4);

        CustomerId = new int[orders + 1];
        ProductId = new int[orders + 1];
        StatusIndex = new byte[orders + 1];
        Amount = new double[orders + 1];
        Qty = new int[orders + 1];
        Discount = new double?[orders + 1];
        Bucket = new int[orders + 1];
        Sku = new string[orders + 1];
        for (int i = 1; i <= orders; i++)
        {
            uint h = Mix((uint)i, 0x9E3779B9u);
            CustomerId[i] = h % 4 == 0
                ? customers + 1 + (int)((h >> 2) % (uint)UnmatchedCustomers)
                : 1 + (int)((h >> 2) % (uint)customers);
            uint p = Mix((uint)i, 0x85EBCA6Bu);
            ProductId[i] = 1 + (int)(p % (uint)products);
            uint s = Mix((uint)i, 0xC2B2AE35u);
            StatusIndex[i] = (byte)(s % 4);
            Amount[i] = ((s >> 2) % 4_000) * 0.25d;
            uint q = Mix((uint)i, 0x27D4EB2Fu);
            Qty[i] = 1 + (int)(q % 10);
            Discount[i] = (q >> 4) % 10 == 0 ? null : ((q >> 8) % 40) * 0.25d;
            Bucket[i] = (int)((q >> 12) % Buckets);
            Sku[i] = "S" + (i % 10_000).ToString("D4", System.Globalization.CultureInfo.InvariantCulture) + "-" + Statuses[StatusIndex[i]];
        }
    }

    public int Orders { get; }

    public int Customers { get; }

    public int Products { get; }

    /// <summary>Size of the id range above <see cref="Customers"/> that orders reference but no customer has.</summary>
    public int UnmatchedCustomers { get; }

    // Per-order columns, indexed by order id (index 0 unused).
    public int[] CustomerId { get; }

    public int[] ProductId { get; }

    public byte[] StatusIndex { get; }

    public double[] Amount { get; }

    public int[] Qty { get; }

    public double?[] Discount { get; }

    public int[] Bucket { get; }

    public string[] Sku { get; }

    public string Status(int orderId) => Statuses[StatusIndex[orderId]];

    public bool CustomerExists(int customerId) => customerId >= 1 && customerId <= Customers;

    public static string CustomerName(int id) => "C" + id.ToString(System.Globalization.CultureInfo.InvariantCulture);

    public static int CustomerSegment(int id) => (int)(Mix((uint)id, 0x165667B1u) % Segments);

    public static string CustomerRegion(int id) => "R" + (id % Regions).ToString(System.Globalization.CultureInfo.InvariantCulture);

    public static string ProductName(int id) => "P" + id.ToString(System.Globalization.CultureInfo.InvariantCulture);

    public static int ProductCategory(int id) => 1 + (id % Categories);

    public static double ProductPrice(int id) => (id % 400) * 0.25d;

    /// <summary>A 32-bit integer hash (the murmur3 finalizer), so columns look random but are reproducible.</summary>
    public static uint Mix(uint value, uint salt)
    {
        uint h = value * 0x9E3779B1u ^ salt;
        h ^= h >> 16;
        h *= 0x85EBCA6Bu;
        h ^= h >> 13;
        h *= 0xC2B2AE35u;
        h ^= h >> 16;
        return h;
    }
}
