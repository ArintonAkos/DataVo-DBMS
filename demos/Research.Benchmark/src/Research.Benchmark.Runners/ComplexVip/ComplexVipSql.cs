namespace Research.Benchmark.Runners.ComplexVip;

internal static class ComplexVipSql
{
    public const string Query = """
        SELECT m.Category, SUM(o.Stake) AS TotalExposure
        FROM Orders o
        JOIN Accounts a ON o.AccountId = a.Id
        JOIN Markets m ON o.MarketId = m.Id
        WHERE a.IsVip = true
        GROUP BY m.Category
        """;
}
