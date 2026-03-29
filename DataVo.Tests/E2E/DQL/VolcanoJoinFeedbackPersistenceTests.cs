using DataVo.Core.StorageEngine.Config;
using System.Text.Json;

namespace DataVo.Tests.E2E.DQL;

public class VolcanoJoinFeedbackPersistenceTests : SqlExecutionTestsBase
{
    private readonly string _feedbackPath;

    public VolcanoJoinFeedbackPersistenceTests()
        : base(BuildConfig(out var feedbackPath), "VolcanoJoinFeedbackDB")
    {
        _feedbackPath = feedbackPath;
    }

    [Fact]
    public void Select_Join_WithFeedbackPersistence_WritesFeedbackFile()
    {
        try
        {
            Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT)");
            Execute("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR)");

            Execute("INSERT INTO Orders (Id, CustomerId) VALUES (1, 10)");
            Execute("INSERT INTO Orders (Id, CustomerId) VALUES (2, 11)");
            Execute("INSERT INTO Orders (Id, CustomerId) VALUES (3, 10)");

            Execute("INSERT INTO Customers (Id, Name) VALUES (10, 'Alice')");
            Execute("INSERT INTO Customers (Id, Name) VALUES (11, 'Bob')");

            var result = ExecuteAndReturn(@"
                SELECT o.Id, c.Name
                FROM Orders o
                JOIN Customers c ON o.CustomerId = c.Id
                ORDER BY o.Id ASC");

            Assert.False(result.IsError, string.Join(" | ", result.Messages));
            Assert.Equal(3, result.Data.Count);

            Assert.True(File.Exists(_feedbackPath));
            string json = File.ReadAllText(_feedbackPath);
            Assert.Contains("Customers.Id=Orders.CustomerId", json, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (File.Exists(_feedbackPath))
            {
                File.Delete(_feedbackPath);
            }
        }
    }

    [Fact]
    public void Select_Join_FeedbackPersistence_TrimsToConfiguredMaxEntries()
    {
        string cappedPath = Path.Combine(Path.GetTempPath(), $"datavo-join-feedback-capped-{Guid.NewGuid():N}.json");

        ReinitializeEngine(new DataVoConfig
        {
            StorageMode = StorageMode.InMemory,
            EnableVolcanoExecution = true,
            EnableVolcanoJoinCardinalityFeedback = true,
            EnableVolcanoJoinCardinalityFeedbackPersistence = true,
            VolcanoJoinCardinalityFeedbackPersistenceFile = cappedPath,
            VolcanoJoinCardinalityFeedbackMaxEntries = 16
        });

        try
        {
            Execute($"CREATE DATABASE {TestDb}");
            Execute($"USE {TestDb}");

            Execute("CREATE TABLE Hub (Id INT PRIMARY KEY)");
            Execute("INSERT INTO Hub (Id) VALUES (1)");

            for (int i = 1; i <= 24; i++)
            {
                string tableName = $"Leaf{i}";
                Execute($"CREATE TABLE {tableName} (Id INT PRIMARY KEY)");
                Execute($"INSERT INTO {tableName} (Id) VALUES (1)");
                Execute($"SELECT h.Id FROM Hub h JOIN {tableName} l ON h.Id = l.Id");
            }

            Assert.True(File.Exists(cappedPath));
            string json = File.ReadAllText(cappedPath);
            var persisted = JsonSerializer.Deserialize<Dictionary<string, double>>(json);

            Assert.NotNull(persisted);
            Assert.True(persisted!.Count <= 16, $"Expected <= 16 feedback entries but found {persisted.Count}.");
        }
        finally
        {
            if (File.Exists(cappedPath))
            {
                File.Delete(cappedPath);
            }
        }
    }

    private static DataVoConfig BuildConfig(out string feedbackPath)
    {
        feedbackPath = Path.Combine(Path.GetTempPath(), $"datavo-join-feedback-{Guid.NewGuid():N}.json");

        return new DataVoConfig
        {
            StorageMode = StorageMode.InMemory,
            EnableVolcanoExecution = true,
            EnableVolcanoJoinCardinalityFeedback = true,
            EnableVolcanoJoinCardinalityFeedbackPersistence = true,
            VolcanoJoinCardinalityFeedbackPersistenceFile = feedbackPath,
            VolcanoJoinCardinalityFeedbackMaxEntries = 256
        };
    }
}
