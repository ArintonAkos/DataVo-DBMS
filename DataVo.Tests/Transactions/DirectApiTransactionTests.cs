using DataVo.Core;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Tests.E2E;

namespace DataVo.Tests.Transactions;

/// <summary>
/// The direct insert APIs (<c>BulkInsert</c>, <c>InsertTyped</c>, <c>InsertTypedBatch</c>, <c>Insert&lt;T&gt;</c>) inside an
/// explicit transaction: rows are validated and buffered like SQL <c>INSERT</c>, hidden from other sessions until
/// COMMIT, discarded by ROLLBACK, and published to subscribers only when committed.
/// </summary>
public class DirectApiTransactionTests
{
    private static readonly ReactiveRowSchema PlayerSchema = new("Id", "Name", "Level");

    public static TheoryData<StorageMode> Modes => [StorageMode.InMemory, StorageMode.Lsm];

    public sealed class Player
    {
        public int Id { get; set; }

        public string? Name { get; set; }

        public int Level { get; set; }
    }

    private static DataVoContext Open(StorageMode mode, out Guid observer)
    {
        DataVoContext context = ChangeCaptureIntegrationTests.NewContext(mode, out string database);
        Ok(context, "CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(20), Level INT)");
        observer = Guid.NewGuid();
        context.Engine.Sessions.Set(observer, database);
        return context;
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void DirectInserts_AreBufferedUntilCommit(StorageMode mode)
    {
        using DataVoContext context = Open(mode, out Guid observer);
        Ok(context, "BEGIN TRANSACTION");

        IReadOnlyList<RowKey> bulk = context.BulkInsert("Players", [new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ann", ["Level"] = 1 }]);
        RowKey typed = context.InsertTyped("Players", PlayerSchema, [CellValue.From(2), CellValue.From("Bob"), CellValue.From(2)]);
        IReadOnlyList<RowKey> batch = context.InsertTypedBatch("Players", PlayerSchema,
        [
            [CellValue.From(3), CellValue.From("Cid"), CellValue.From(3)],
            [CellValue.From(4), CellValue.From("Dee"), CellValue.From(4)],
        ]);
        RowKey model = context.Insert("Players", new Player { Id = 5, Name = "Eve", Level = 5 });

        // Provisional keys are distinct handles for the buffered rows.
        RowKey[] keys = [.. bulk, typed, .. batch, model];
        Assert.Equal(5, keys.Distinct().Count());

        Assert.Equal("1,2,3,4,5", Ids(context, context.SessionId));
        Assert.Equal(string.Empty, Ids(context, observer));

        Ok(context, "COMMIT");
        Assert.Equal("1,2,3,4,5", Ids(context, observer));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void DirectInserts_AreDiscardedByRollback(StorageMode mode)
    {
        using DataVoContext context = Open(mode, out _);
        Ok(context, "BEGIN TRANSACTION");
        context.InsertTyped("Players", PlayerSchema, [CellValue.From(1), CellValue.From("Ann"), CellValue.From(1)]);
        context.BulkInsert("Players", [new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Bob", ["Level"] = 2 }]);

        Ok(context, "ROLLBACK");
        Assert.Equal(string.Empty, Ids(context, context.SessionId));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void DirectInserts_AreCheckedAgainstTheTransactionsOwnRows(StorageMode mode)
    {
        using DataVoContext context = Open(mode, out _);
        Ok(context, "INSERT INTO Players VALUES (1, 'Ann', 1)");
        Ok(context, "BEGIN TRANSACTION");
        context.InsertTyped("Players", PlayerSchema, [CellValue.From(2), CellValue.From("Bob"), CellValue.From(2)]);

        Assert.Throws<ConstraintViolationException>(() =>
            context.InsertTyped("Players", PlayerSchema, [CellValue.From(1), CellValue.From("Dup"), CellValue.From(0)]));
        Assert.Throws<ConstraintViolationException>(() =>
            context.InsertTypedBatch("Players", PlayerSchema, [[CellValue.From(3), CellValue.From("Cid"), CellValue.From(3)], [CellValue.From(2), CellValue.From("Dup"), CellValue.From(0)]]));

        // The rejected batch buffered nothing; the transaction still commits its valid row.
        Ok(context, "COMMIT");
        Assert.Equal("1,2", Ids(context, context.SessionId));
    }

    [Fact]
    public void DirectInserts_ReachSubscribersOnlyOnCommit()
    {
        using DataVoContext context = Open(StorageMode.InMemory, out _);
        var added = new List<int>();
        using IDisposable subscription = context.Subscribe("SELECT Id FROM Players", change =>
            added.AddRange(change.Added.Select(row => Convert.ToInt32(row["Id"]))));

        Ok(context, "BEGIN TRANSACTION");
        context.InsertTyped("Players", PlayerSchema, [CellValue.From(7), CellValue.From("Ann"), CellValue.From(1)]);
        context.DispatchPendingNotifications();
        Assert.Empty(added);

        Ok(context, "COMMIT");
        context.DispatchPendingNotifications();
        Assert.Equal([7], added);
    }

    private static string Ids(DataVoContext context, Guid session) =>
        string.Join(",", context.Execute("SELECT Id FROM Players ORDER BY Id", session)[^1].Data.Select(row => row["Id"]));

    private static void Ok(DataVoContext context, string sql)
    {
        foreach (var result in context.Execute(sql))
        {
            Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        }
    }
}
