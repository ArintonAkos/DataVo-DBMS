namespace Research.Benchmark.Runners.ArenaLiveViews;

public static class ArenaLiveViewsSql
{
    public const string CreatePlayers = "CREATE TABLE Players (Id INT PRIMARY KEY, Team VARCHAR(10), Zone VARCHAR(20), Health INT, Score INT)";
    public const string CreateInventory = "CREATE TABLE Inventory (InventoryId INT PRIMARY KEY, PlayerId INT, ItemId INT, Slot INT)";
    public const string Leaderboard = "SELECT Id, Score FROM Players ORDER BY Score DESC, Id ASC LIMIT 20";
    public const string AliveByTeam = "SELECT Team, COUNT(*) AS Alive FROM Players WHERE Health > 0 GROUP BY Team ORDER BY Team ASC";
    public const string ZoneInventory = "SELECT p.Id, i.ItemId FROM Players p JOIN Inventory i ON p.Id = i.PlayerId WHERE p.Zone = 'arena-7' ORDER BY p.Id ASC, i.ItemId ASC";

    public static readonly IReadOnlyList<string> All = [Leaderboard, AliveByTeam, ZoneInventory];
}
