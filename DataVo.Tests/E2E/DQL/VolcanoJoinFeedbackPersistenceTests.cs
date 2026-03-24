using DataVo.Core.StorageEngine.Config;

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
