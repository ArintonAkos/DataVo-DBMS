using System.Globalization;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Sql;

/// <summary>
/// A BIT column compared with <c>TRUE</c>/<c>FALSE</c> (any case), with <c>1</c>/<c>0</c>, or with
/// <c>'true'</c>/<c>'false'</c>, which INSERT also accepts. Before the fix, <c>= true</c> failed to bind
/// ("cannot resolve column 'true'") and <c>= 1</c> or <c>= 'true'</c> silently matched no row. The expected
/// rows come from LINQ over the seed data under SQL's rules: a NULL flag matches neither value, and any other
/// integer or text matches nothing.
/// </summary>
public sealed class SqlBooleanLiteralTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    private sealed record User(int Id, bool? IsActive, int DepartmentId);

    private static readonly User[] Users = [.. Enumerable.Range(1, 210).Select(i => new User(
        i,
        i % 7 == 0 ? null : i % 3 != 0,
        i % 4 + 1))];

    private static int[] Ids(Func<User, bool> predicate) => [.. Users.Where(predicate).Select(u => u.Id).Order()];

    [Theory]
    [MemberData(nameof(Modes))]
    public void EveryBooleanSpelling_FiltersByTheFlag(StorageMode mode)
    {
        WithUsers(mode, context =>
        {
            int[] active = Ids(u => u.IsActive == true);
            int[] inactive = Ids(u => u.IsActive == false);

            foreach (string value in new[] { "true", "TRUE", "True", "1", "'true'", "'TRUE'" })
            {
                AssertIds(context, $"SELECT Id FROM Users WHERE IsActive = {value}", active);
                AssertIds(context, $"SELECT Id FROM Users WHERE {value} = IsActive", active);
            }

            foreach (string value in new[] { "false", "FALSE", "0", "'false'" })
            {
                AssertIds(context, $"SELECT Id FROM Users WHERE IsActive = {value}", inactive);
            }

            // A NULL flag is neither: <> leaves it out as well.
            AssertIds(context, "SELECT Id FROM Users WHERE IsActive <> true", inactive);
            AssertIds(context, "SELECT Id FROM Users WHERE NOT IsActive = true", inactive);
            AssertIds(context, "SELECT Id FROM Users WHERE IsActive IS NULL", Ids(u => u.IsActive is null));

            // Values that are not booleans match no row rather than being read as one.
            AssertIds(context, "SELECT Id FROM Users WHERE IsActive = 2", []);
            AssertIds(context, "SELECT Id FROM Users WHERE IsActive = 'yes'", []);
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BooleanLiterals_CombineWithOtherConditionsAndAggregates(StorageMode mode)
    {
        WithUsers(mode, context =>
        {
            AssertIds(context, "SELECT Id FROM Users WHERE Id > 100 AND IsActive = true", Ids(u => u.Id > 100 && u.IsActive == true));
            AssertIds(context, "SELECT Id FROM Users WHERE IsActive = false OR DepartmentId = 2", Ids(u => u.IsActive == false || u.DepartmentId == 2));
            AssertIds(context, "SELECT Id FROM Users WHERE IsActive IN (true)", Ids(u => u.IsActive == true));
            AssertIds(context, "SELECT Id FROM Users WHERE Id BETWEEN 20 AND 80 AND IsActive = FALSE", Ids(u => u.Id is >= 20 and <= 80 && u.IsActive == false));

            int activeCount = Users.Count(u => u.IsActive == true);
            AssertScalar(context, "SELECT COUNT(*) FROM Users WHERE IsActive = true", activeCount);
            AssertScalar(context, $"SELECT COUNT(*) FROM Users WHERE IsActive = true HAVING COUNT(*) >= {activeCount}", activeCount);
            AssertNoRows(context, $"SELECT COUNT(*) FROM Users WHERE IsActive = true HAVING COUNT(*) > {activeCount}");

            // Grouped by department, active users only.
            QueryResult grouped = Run(context, "SELECT DepartmentId, COUNT(*) AS N FROM Users WHERE IsActive = true GROUP BY DepartmentId HAVING COUNT(*) > 0");
            var expected = Users.Where(u => u.IsActive == true).GroupBy(u => u.DepartmentId).ToDictionary(g => g.Key, g => g.Count());
            Assert.Equal(expected.Count, grouped.Data.Count);
            foreach (var row in grouped.Data)
            {
                Assert.Equal(expected[ToInt(row["DepartmentId"])], ToInt(row["N"]));
            }

            // DISTINCT and an explicit transaction take the general path.
            QueryResult distinct = Run(context, "SELECT DISTINCT DepartmentId FROM Users WHERE IsActive = false");
            Assert.Equal(Users.Where(u => u.IsActive == false).Select(u => u.DepartmentId).Distinct().Count(), distinct.Data.Count);

            QueryResult inTransaction = context.Execute("BEGIN TRANSACTION; SELECT Id FROM Users WHERE IsActive = true; COMMIT;")
                .Single(r => r.Fields.Count > 0);
            Assert.False(inTransaction.IsError, string.Join("; ", inTransaction.Messages));
            Assert.Equal(Ids(u => u.IsActive == true), inTransaction.Data.Select(r => ToInt(r["Id"])).Order());
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BooleanLiterals_FilterJoinedTables(StorageMode mode)
    {
        WithUsers(mode, context =>
        {
            Execute(context, "CREATE TABLE Departments (Id INT PRIMARY KEY, IsOpen BIT)");
            Execute(context, "INSERT INTO Departments (Id, IsOpen) VALUES (1, true), (2, false), (3, TRUE), (5, true)");
            HashSet<int> open = [1, 3, 5];

            AssertIds(context, "SELECT u.Id FROM Users u JOIN Departments d ON u.DepartmentId = d.Id WHERE d.IsOpen = true",
                Ids(u => open.Contains(u.DepartmentId)));
            AssertIds(context, "SELECT u.Id FROM Users u JOIN Departments d ON u.DepartmentId = d.Id WHERE d.IsOpen = true AND u.IsActive = false",
                Ids(u => open.Contains(u.DepartmentId) && u.IsActive == false));
            AssertScalar(context, "SELECT COUNT(*) FROM Users u LEFT JOIN Departments d ON u.DepartmentId = d.Id WHERE d.IsOpen = false",
                Users.Count(u => u.DepartmentId == 2));
        });
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BooleanLiterals_InUpdateDeleteAndBoundParameters(StorageMode mode)
    {
        WithUsers(mode, context =>
        {
            var prepared = context.Prepare("SELECT Id FROM Users WHERE IsActive = @flag");
            QueryResult bound = prepared.Execute(DataVoQueryParameter.FromObject("flag", true));
            Assert.False(bound.IsError, string.Join("; ", bound.Messages));
            Assert.Equal(Ids(u => u.IsActive == true), bound.Data.Select(r => ToInt(r["Id"])).Order());

            Execute(context, "UPDATE Users SET IsActive = false WHERE Id <= 30 AND IsActive = true");
            Func<User, bool?> afterUpdate = u => u.Id <= 30 && u.IsActive == true ? false : u.IsActive;
            AssertIds(context, "SELECT Id FROM Users WHERE IsActive = true", Ids(u => afterUpdate(u) == true));

            Execute(context, "DELETE FROM Users WHERE IsActive = FALSE");
            AssertIds(context, "SELECT Id FROM Users", Ids(u => afterUpdate(u) != false));
        });
    }

    private static void AssertIds(DataVoContext context, string sql, int[] expected)
    {
        QueryResult result = Run(context, sql);
        int[] actual = [.. result.Data.Select(r => ToInt(r.Values.First())).Order()];
        Assert.True(expected.SequenceEqual(actual), $"{sql}: expected {expected.Length} rows, got {actual.Length}");
    }

    private static void AssertScalar(DataVoContext context, string sql, long expected)
    {
        QueryResult result = Run(context, sql);
        Assert.Equal(expected, Convert.ToInt64(Assert.Single(result.Data).Values.Single(), CultureInfo.InvariantCulture));
    }

    private static void AssertNoRows(DataVoContext context, string sql) => Assert.Empty(Run(context, sql).Data);

    private static QueryResult Run(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
        return result;
    }

    private static int ToInt(object? value) => Convert.ToInt32(value, CultureInfo.InvariantCulture);

    private static void WithUsers(StorageMode mode, Action<DataVoContext> test)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("sql_boolean_literals") : null;
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DataDirectory = directory });
        Execute(context, "CREATE DATABASE BooleanLiterals");
        Execute(context, "USE BooleanLiterals");
        Execute(context, "CREATE TABLE Users (Id INT PRIMARY KEY, IsActive BIT, DepartmentId INT)");
        foreach (User[] chunk in Users.Chunk(100))
        {
            // Mix the spellings INSERT accepts.
            Execute(context, "INSERT INTO Users (Id, IsActive, DepartmentId) VALUES " + string.Join(", ", chunk.Select(u =>
                $"({u.Id}, {u.IsActive switch { null => "NULL", true => u.Id % 2 == 0 ? "true" : "TRUE", false => "false" }}, {u.DepartmentId})")));
        }

        test(context);
    }

    private static void Execute(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        Assert.False(result.IsError, sql + ": " + string.Join("; ", result.Messages));
    }
}
