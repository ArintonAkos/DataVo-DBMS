using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;

/// <summary>A transaction racing an autocommit write to the same row must never leave two rows with one key.</summary>
public class TransactionAutocommitRaceTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    public void SequentialInterleaving_KeepsOneRowPerKey(StorageMode mode)
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = mode });
        Guid writer = Guid.NewGuid();
        Guid other = Guid.NewGuid();
        Run(context, "CREATE DATABASE Race", writer);
        Run(context, "USE Race", writer);
        Run(context, "USE Race", other);
        Run(context, "CREATE TABLE T (Id INT PRIMARY KEY, V INT)", writer);
        Run(context, "INSERT INTO T (Id, V) VALUES (1, 0)", writer);

        Run(context, "BEGIN", writer);
        Run(context, "UPDATE T SET V = 1 WHERE Id = 1", writer);
        QueryResult concurrent = context.Execute("UPDATE T SET V = 2 WHERE Id = 1", other)[^1];
        QueryResult commit = context.Execute("COMMIT", writer)[^1];

        List<Dictionary<string, object?>> rows = context.Execute("SELECT Id, V FROM T", other)[^1].Data;
        Assert.True(rows.Count == 1, $"concurrent error={concurrent.IsError} ({string.Join(" | ", concurrent.Messages)}), commit error={commit.IsError} ({string.Join(" | ", commit.Messages)}), rows={rows.Count}");
    }

    [Fact(Skip = "Known engine bug: a row replaced by an autocommit update after BEGIN vanishes from the open transaction's snapshot.")]
    public void RowReplacedAfterBegin_StaysVisibleAndCannotBeInsertedAgain()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        Guid tx = Guid.NewGuid();
        Guid other = Guid.NewGuid();
        Run(context, "CREATE DATABASE Race", tx);
        Run(context, "USE Race", tx);
        Run(context, "USE Race", other);
        Run(context, "CREATE TABLE T (Id INT PRIMARY KEY, V INT)", tx);
        Run(context, "INSERT INTO T (Id, V) VALUES (1, 0)", tx);

        Run(context, "BEGIN", tx);
        Run(context, "UPDATE T SET V = 5 WHERE Id = 1", other);
        int visibleInTransaction = context.Execute("SELECT Id FROM T WHERE Id = 1", tx)[^1].Data.Count;
        QueryResult insert = context.Execute("INSERT INTO T (Id, V) VALUES (1, 9)", tx)[^1];
        QueryResult commit = context.Execute("COMMIT", tx)[^1];

        int rows = context.Execute("SELECT Id, V FROM T", other)[^1].Data.Count;
        Assert.True(rows == 1 && visibleInTransaction == 1, $"visible={visibleInTransaction}, insertError={insert.IsError} ({string.Join("|", insert.Messages)}), commitError={commit.IsError} ({string.Join("|", commit.Messages)}), rows={rows}");
    }

    [Fact]
    public async Task ConcurrentTransactionsAndAutocommits_KeepOneRowPerKey()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        Guid setup = Guid.NewGuid();
        Run(context, "CREATE DATABASE Race", setup);
        Run(context, "USE Race", setup);
        Run(context, "CREATE TABLE T (Id INT PRIMARY KEY, V INT)", setup);
        for (int id = 1; id <= 3; id++)
        {
            Run(context, $"INSERT INTO T (Id, V) VALUES ({id}, 0)", setup);
        }

        Task Worker(bool transactional, int seed) => Task.Run(() =>
        {
            Guid session = Guid.NewGuid();
            context.Execute("USE Race", session);
            var random = new Random(seed);
            for (int i = 0; i < 400; i++)
            {
                string update = $"UPDATE T SET V = {i} WHERE Id = {random.Next(1, 4)}";
                if (transactional)
                {
                    context.Execute("BEGIN", session);
                    context.Execute(update, session);
                    if (context.Execute("COMMIT", session)[^1].IsError)
                    {
                        context.Execute("ROLLBACK", session);
                    }
                }
                else
                {
                    context.Execute(update, session);
                }
            }
        });

        await Task.WhenAll(Worker(true, 1), Worker(false, 2), Worker(false, 3));

        List<object?> ids = [.. context.Execute("SELECT Id FROM T", setup)[^1].Data.Select(row => row["Id"])];
        Assert.Equal(3, ids.Count);
        Assert.Equal(3, ids.Distinct().Count());
    }

    private static void Run(DataVoContext context, string sql, Guid session)
    {
        QueryResult result = context.Execute(sql, session)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
    }
}
