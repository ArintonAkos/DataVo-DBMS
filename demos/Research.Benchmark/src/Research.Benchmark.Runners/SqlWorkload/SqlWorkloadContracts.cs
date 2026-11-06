namespace Research.Benchmark.Runners.SqlWorkload;

/// <summary>
/// One embedded database driven through the same relational workload: an orders table with a
/// customer foreign key and a secondary index, a customers table to join against, and a table keyed
/// by random GUIDs. Every query returns a number the runner compares across engines, so a fast but
/// wrong answer shows up as a mismatch instead of a win.
/// </summary>
public interface ISqlWorkloadEngine : IDisposable
{
    string Name { get; }

    /// <summary>The durability setting and API path used, printed under the results.</summary>
    string Notes { get; }

    void Create(string directory);

    void LoadOrders(SqlWorkloadData data);

    void LoadGuids(SqlWorkloadData data);

    /// <summary>Returns the order's Qty, or -1 when the row does not exist.</summary>
    long PointSelect(int id);

    /// <summary>Returns 1 when the GUID-keyed row exists.</summary>
    long GuidPointSelect(Guid key);

    /// <summary>Rows for one customer, via the secondary index. Returns the row count.</summary>
    long SecondaryLookup(int customerId);

    /// <summary>Rows with <c>fromId &lt;= Id &lt; fromId + width</c>. Returns the row count.</summary>
    long RangeScan(int fromId, int width);

    /// <summary><c>COUNT(*)</c> of paid orders (full scan, no index on Status).</summary>
    long FilteredCount();

    /// <summary>GROUP BY Status. Returns the sum of the group counts.</summary>
    long GroupByStatus();

    /// <summary>Orders joined to customers, grouped by region. Returns the number of regions.</summary>
    long JoinByRegion();

    /// <summary>Top 10 orders by Amount. Returns the number of rows.</summary>
    long TopTen();

    void UpdateAmount(int id, double amount);

    void Delete(int id);

    void Close();

    void Reopen();

    long SizeOnDiskBytes();
}

public sealed class SqlWorkloadData
{
    public static readonly string[] Statuses = ["new", "paid", "shipped", "cancelled"];

    public SqlWorkloadData(int orders, int seed = 42)
    {
        Orders = orders;
        Customers = Math.Max(100, orders / 100);
        Guids = new Guid[orders];
        var random = new Random(seed);
        Span<byte> bytes = stackalloc byte[16];
        for (int i = 0; i < Guids.Length; i++)
        {
            random.NextBytes(bytes);
            bytes[7] = (byte)((bytes[7] & 0x0F) | 0x40); // version 4, like Guid.NewGuid()
            Guids[i] = new Guid(bytes);
        }
    }

    public int Orders { get; }

    public int Customers { get; }

    public Guid[] Guids { get; }

    public static int CustomerOf(int orderId, int customers) => (int)((orderId * 7919L) % customers) + 1;

    public static string StatusOf(int orderId) => Statuses[orderId % Statuses.Length];

    public static double AmountOf(int orderId) => (orderId % 1000) * 1.25d;

    public static int QtyOf(int orderId) => orderId % 10;

    public static string RegionOf(int customerId) => "R" + (customerId % 8);
}
