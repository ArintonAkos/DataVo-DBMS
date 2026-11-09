using DataVo.Core;
using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Security;
using DataVo.Core.StorageEngine.Config;
using DataVo.Data;

namespace DataVo.Tests.E2E.DQL;

/// <summary>
/// Pins the behaviour prepared statements must share with plain SQL: results a caller can keep, permission
/// checks, plan invalidation, parameter positions, and index maintenance on the fast paths.
/// </summary>
public class PreparedStatementCorrectnessTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_prep_correct_{Guid.NewGuid():N}");

    public PreparedStatementCorrectnessTests() => Directory.CreateDirectory(_directory);

    public void Dispose()
    {
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch (IOException)
        {
            // Best-effort cleanup of the temporary database directory.
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Execute_ReturnedResult_StaysValidAfterLaterExecutions(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        Seed(context);
        DataVoPreparedStatement lookup = context.Prepare("SELECT Id, Qty, Price, Name FROM Items WHERE Id = @id");

        QueryResult first = lookup.Execute(("id", 40_001));
        object boxedQty = first.Data[0]["Qty"]!;
        object boxedPrice = first.Data[0]["Price"]!;

        for (int i = 40_002; i < 40_050; i++)
        {
            lookup.Execute(("id", i));
        }

        Assert.Equal(40_001, first.Data[0]["Id"]);
        Assert.Equal("item-40001", first.Data[0]["Name"]);
        Assert.Equal(40_001 * 2, boxedQty);
        Assert.Equal(40_001 * 0.5f, Convert.ToSingle(boxedPrice));
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Execute_MutatingAResult_DoesNotLeakIntoLaterResults(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        Seed(context);
        DataVoPreparedStatement lookup = context.Prepare("SELECT Id FROM Items WHERE Id = @id");
        DataVoPreparedStatement update = context.Prepare("UPDATE Items SET Qty = @qty WHERE Id = @id");

        QueryResult missing = lookup.Execute(("id", 999_999));
        missing.Messages.Add("caller note");
        missing.Fields.Add("caller field");
        missing.Data.Add([]);
        QueryResult missingAgain = lookup.Execute(("id", 999_998));
        Assert.Empty(missingAgain.Data);
        Assert.DoesNotContain("caller note", missingAgain.Messages);
        Assert.DoesNotContain("caller field", missingAgain.Fields);

        QueryResult updated = update.Execute(("qty", 3), ("id", 40_001));
        updated.Messages.Clear();
        QueryResult updatedAgain = update.Execute(("qty", 4), ("id", 40_001));
        Assert.Contains("Rows affected: 1", updatedAgain.Messages);
    }

    [Fact]
    public void ExecuteBorrowed_LsmPointLookup_AllocatesNothingAfterWarmUp()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        Seed(context);
        DataVoPreparedStatement lookup = context.Prepare("SELECT Id, Qty, Name FROM Items WHERE Id = @id");
        for (int i = 0; i < 2_000; i++)
        {
            lookup.ExecuteBorrowed(("id", 40_001 + (i % 10)));
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        long sum = 0;
        for (int i = 0; i < 1_000; i++)
        {
            sum += (int)lookup.ExecuteBorrowed(("id", 40_001 + (i % 10))).Data[0]["Qty"]!;
        }

        Assert.True(sum > 0);
        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    [Theory]
    [InlineData("SELECT Id FROM Items WHERE Id = @id", false)]
    [InlineData("UPDATE Items SET Qty = @id WHERE Id = @id", true)]
    [InlineData("DELETE FROM Items WHERE Id = @id", true)]
    public void Prepared_FastPaths_EnforceSessionPermissions(string sql, bool allowReadOnlyAnonymous)
    {
        var config = new DataVoConfig
        {
            StorageMode = StorageMode.Lsm,
            DiskStoragePath = _directory,
            LsmStrictFsync = false,
            EnableAuthorization = true,
            AllowAnonymousSession = allowReadOnlyAnonymous,
            AnonymousRole = DatabaseRole.ReadOnly,
            AuthorizationUsers = [new DataVoAuthUser { Username = "admin", Password = "adminpw", Role = DatabaseRole.Admin }],
        };

        using var context = new DataVoContext(config);
        Assert.True(context.Login("admin", "adminpw"));
        context.Execute("CREATE DATABASE AuthDb");
        context.Execute("USE AuthDb");
        context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Qty INT)");
        context.Execute("INSERT INTO Items (Id, Qty) VALUES (1, 10)");
        context.Logout();

        QueryResult denied = context.Prepare(sql).Execute(("id", 1));

        Assert.True(denied.IsError, string.Join(" | ", denied.Messages));
        Assert.Equal(DataVoErrorCodes.PermissionDenied, denied.ErrorCode);
    }

    [Fact]
    public void Prepared_Plans_SeeSchemaChanges()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        Seed(context);
        DataVoPreparedStatement lookup = context.Prepare("SELECT * FROM Items WHERE Id = @id");
        Assert.DoesNotContain("Extra", lookup.Execute(("id", 40_001)).Fields);

        Assert.False(context.Execute("ALTER TABLE Items ADD COLUMN Extra INT")[^1].IsError);

        QueryResult afterAlter = lookup.Execute(("id", 40_001));
        Assert.False(afterAlter.IsError, string.Join(" | ", afterAlter.Messages));
        Assert.Contains("Extra", afterAlter.Fields);
    }

    [Fact]
    public void SchemaVersion_IsSharedByEverySpellingOfATableName()
    {
        using DataVoContext context = Open(StorageMode.InMemory);
        Seed(context);
        int before = context.Engine.Catalog.GetTableSchemaVersion("Items", "Prep");

        context.Execute("CREATE INDEX ix_qty ON items (Qty)");

        Assert.NotEqual(before, context.Engine.Catalog.GetTableSchemaVersion("ITEMS", "prep"));
    }

    [Fact]
    public void Prepared_Select_AfterDropTable_ReportsTheMissingTable()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        Seed(context);
        DataVoPreparedStatement lookup = context.Prepare("SELECT Id FROM Items WHERE Id = @id");
        Assert.Single(lookup.Execute(("id", 40_001)).Data);

        context.Execute("DROP TABLE Items");

        Assert.True(lookup.Execute(("id", 40_001)).IsError);
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Prepared_FastPathResults_MatchPlainSqlForEveryColumnType(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        context.Execute("CREATE DATABASE Types");
        context.Execute("USE Types");
        context.Execute("CREATE TABLE T (Id INT PRIMARY KEY, I INT, F FLOAT, B BIT, D DATE, S VARCHAR(40), G GUID, V VECTOR(3), Tail INT)");
        const string guid = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
        context.Execute($"INSERT INTO T (Id, I, F, B, D, S, G, V, Tail) VALUES (1, 70000, 1.5, 1, '2024-02-29', 'alpha', '{guid}', '[1, 2, 3]', 9)");
        context.Execute("INSERT INTO T (Id, I, F, B, D, S, G, V, Tail) VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 8)");

        foreach (int id in new[] { 1, 2, 3 })
        {
            AssertSameAsPlainSql(context, "SELECT * FROM T WHERE Id = @id", $"SELECT * FROM T WHERE Id = {id}", ("id", id));
            AssertSameAsPlainSql(context, "SELECT Tail, S AS Label, id FROM T WHERE id = @id", $"SELECT Tail, S AS Label, id FROM T WHERE id = {id}", ("id", id));
            AssertSameAsPlainSql(context, "SELECT t.I, t.V FROM T t WHERE t.Id = @id", $"SELECT t.I, t.V FROM T t WHERE t.Id = {id}", ("id", id));
        }

        AssertSameAsPlainSql(context, "SELECT Id FROM T WHERE Id >= @a AND Id < @b LIMIT 1 OFFSET 1", "SELECT Id FROM T WHERE Id >= 1 AND Id < 3 LIMIT 1 OFFSET 1", ("a", 1), ("b", 3));
        AssertSameAsPlainSql(context, "SELECT Id FROM T WHERE F > @f", "SELECT Id FROM T WHERE F > 1.25", ("f", 1.25));
        AssertSameAsPlainSql(context, "SELECT Id FROM T WHERE F = @f", "SELECT Id FROM T WHERE F = 1.5", ("f", 1.5));
        AssertSameAsPlainSql(context, "SELECT Id FROM T WHERE I <> @i", "SELECT Id FROM T WHERE I <> 5", ("i", 5));
        AssertSameAsPlainSql(context, "SELECT Id FROM T WHERE S = @s", "SELECT Id FROM T WHERE S = 'alpha'", ("s", "alpha"));
        AssertSameAsPlainSql(context, "SELECT Id FROM T WHERE S <> @s", "SELECT Id FROM T WHERE S <> 'alpha'", ("s", "alpha"));
        AssertSameAsPlainSql(context, "SELECT Id FROM T WHERE B = @b", "SELECT Id FROM T WHERE B = 1", ("b", 1));
        AssertSameAsPlainSql(context, "SELECT Id FROM T WHERE D = @d", "SELECT Id FROM T WHERE D = '2024-02-29'", ("d", "2024-02-29"));
        AssertSameAsPlainSql(context, "SELECT Id FROM T WHERE G = @g", $"SELECT Id FROM T WHERE G = '{guid}'", ("g", Guid.Parse(guid)));
        AssertSameAsPlainSql(context, "SELECT Id FROM T WHERE Id = @id", "SELECT Id FROM T WHERE Id = 1.5", ("id", 1.5));
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Prepared_ParametersWork_WhereverAValueWorks(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        Seed(context);

        Assert.Equal(2, Rows(context.Prepare("SELECT Id FROM Items LIMIT @n").Execute(("n", 2))));
        Assert.Equal(1, Rows(context.Prepare("SELECT Id FROM Items ORDER BY Id LIMIT @n OFFSET @skip").Execute(("n", 1), ("skip", 2))));
        Assert.Equal(2, Rows(context.Prepare("SELECT Id FROM Items WHERE Id IN (@a, @b)").Execute(("a", 40_001), ("b", 40_002))));
        Assert.Equal(3, Rows(context.Prepare("SELECT Id FROM Items WHERE Id BETWEEN @a AND @b").Execute(("a", 40_001), ("b", 40_003))));
        Assert.Equal(1, Rows(context.Prepare("SELECT Id FROM Items WHERE Name LIKE @p").Execute(("p", "item-40001"))));
        Assert.Equal(1, Rows(context.Prepare("SELECT Id FROM Items WHERE Id = @a OR Id = @a").Execute(("a", 40_001))));
        Assert.Equal(1, Rows(context.Prepare("SELECT Id FROM Items WHERE Id IN (SELECT Id FROM Items WHERE Id = @a)").Execute(("a", 40_001))));
        Assert.Equal(1, Rows(context.Prepare("SELECT Name, COUNT(*) AS N FROM Items WHERE Id = @a GROUP BY Name HAVING COUNT(*) >= @min").Execute(("a", 40_001), ("min", 1))));
        Assert.Equal(1, Rows(context.Prepare("SELECT UPPER(Name) AS U FROM Items WHERE Id = @a").Execute(("a", 40_001))));

        QueryResult bumped = context.Prepare("UPDATE Items SET Qty = Qty + @d WHERE Id = @id").Execute(("d", 5), ("id", 40_001));
        Assert.False(bumped.IsError, string.Join(" | ", bumped.Messages));
        Assert.Equal(40_001 * 2 + 5, context.Execute("SELECT Qty FROM Items WHERE Id = 40001")[^1].Data[0]["Qty"]);

        QueryResult dollar = context.Prepare("SELECT Id FROM Items WHERE Id = $a").Execute(("a", 40_001));
        Assert.False(dollar.IsError, string.Join(" | ", dollar.Messages));
        Assert.Single(dollar.Data);
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Prepared_Scan_HandlesOffsetWithoutLimit(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        Seed(context);

        Assert.Equal(58, Rows(context.Execute("SELECT Id FROM Items WHERE Id >= 40001 OFFSET 2")[^1]));
        Assert.Equal(58, Rows(context.Prepare("SELECT Id FROM Items WHERE Id >= @a OFFSET 2").Execute(("a", 40_001))));
    }

    [Fact]
    public void Prepared_UnboundLimit_IsAnErrorInsteadOfAnEmptyResult()
    {
        using DataVoContext context = Open(StorageMode.InMemory);
        Seed(context);

        QueryResult missing = context.Prepare("SELECT Id FROM Items LIMIT @n").Execute();
        Assert.True(missing.IsError);
        Assert.Equal(DataVoErrorCodes.Binding, missing.ErrorCode);

        QueryResult negative = context.Prepare("SELECT Id FROM Items LIMIT @n").Execute(("n", -1));
        Assert.True(negative.IsError);
        Assert.Equal(DataVoErrorCodes.Binding, negative.ErrorCode);
    }

    [Fact]
    public void Prepared_SecondaryLookup_NeverSeesATransactionHalfApplied()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        context.Execute("CREATE DATABASE Torn");
        context.Execute("USE Torn");
        context.Execute("CREATE TABLE Accounts (Id INT PRIMARY KEY, Owner INT, Balance INT)");
        context.Execute("CREATE INDEX ix_owner ON Accounts (Owner)");
        context.Execute("INSERT INTO Accounts (Id, Owner, Balance) VALUES (1, 7, 500)");
        context.Execute("INSERT INTO Accounts (Id, Owner, Balance) VALUES (2, 7, 500)");
        DataVoPreparedStatement balances = context.Prepare("SELECT Balance FROM Accounts WHERE Owner = @owner");

        using var stop = new CancellationTokenSource(TimeSpan.FromMilliseconds(1_500));
        Task writer = Task.Run(() =>
        {
            Guid session = Guid.NewGuid();
            context.Execute("USE Torn", session);
            for (int amount = 1; !stop.IsCancellationRequested; amount = (amount % 400) + 1)
            {
                Assert.False(context.Execute("BEGIN", session)[^1].IsError);
                context.Execute($"UPDATE Accounts SET Balance = {500 - amount} WHERE Id = 1", session);
                context.Execute($"UPDATE Accounts SET Balance = {500 + amount} WHERE Id = 2", session);
                QueryResult commit = context.Execute("COMMIT", session)[^1];
                Assert.False(commit.IsError, string.Join(" | ", commit.Messages));
            }
        });

        while (!stop.IsCancellationRequested)
        {
            QueryResult result = balances.Execute(("owner", 7));
            Assert.False(result.IsError, string.Join(" | ", result.Messages));
            Assert.Equal(1_000, result.Data.Sum(row => (int)row["Balance"]!));
        }

        writer.GetAwaiter().GetResult();
    }

    [Fact]
    public void Prepared_GeneralPath_CanBeExecutedRepeatedly()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        Seed(context);
        context.Execute("CREATE TABLE Tags (TagId INT PRIMARY KEY, ItemId INT, Weight INT)");
        context.Execute("INSERT INTO Tags (TagId, ItemId, Weight) VALUES (1, 40001, 5)");
        context.Execute("INSERT INTO Tags (TagId, ItemId, Weight) VALUES (2, 40002, 6)");
        DataVoPreparedStatement grouped = context.Prepare(
            "SELECT i.Name, SUM(t.Weight) AS Total FROM Items i JOIN Tags t ON i.Id = t.ItemId WHERE i.Id >= @a GROUP BY i.Name ORDER BY i.Name LIMIT 2");

        QueryResult first = grouped.Execute(("a", 40_001));
        Assert.False(first.IsError, string.Join(" | ", first.Messages));
        for (int i = 0; i < 3; i++)
        {
            QueryResult again = grouped.Execute(("a", 40_001));
            Assert.False(again.IsError, string.Join(" | ", again.Messages));
            Assert.Equal(first.Fields, again.Fields);
            Assert.Equal(first.Data.Count, again.Data.Count);
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void Prepared_TextParameters_CompareTheExactStoredValue(StorageMode mode)
    {
        using DataVoContext context = Open(mode);
        context.Execute("CREATE DATABASE Quotes");
        context.Execute("USE Quotes");
        context.Execute("CREATE TABLE T (Id INT PRIMARY KEY, S VARCHAR(40))");
        DataVoPreparedStatement insert = context.Prepare("INSERT INTO T (Id, S) VALUES (@id, @s)");
        insert.Execute(("id", 1), ("s", "'wrapped'"));
        insert.Execute(("id", 2), ("s", "wrapped"));

        QueryResult exact = context.Prepare("SELECT Id FROM T WHERE S = @s").Execute(("s", "'wrapped'"));
        Assert.Equal([1], exact.Data.Select(row => (int)row["Id"]!));

        QueryResult bare = context.Prepare("SELECT Id FROM T WHERE S = @s").Execute(("s", "wrapped"));
        Assert.Equal([2], bare.Data.Select(row => (int)row["Id"]!));

        context.Execute("CREATE INDEX ix_s ON T (S)");
        QueryResult exactIndexed = context.Prepare("SELECT Id FROM T WHERE S = @s").Execute(("s", "'wrapped'"));
        Assert.Equal([1], exactIndexed.Data.Select(row => (int)row["Id"]!));
        QueryResult bareIndexed = context.Prepare("SELECT Id FROM T WHERE S = @s").Execute(("s", "wrapped"));
        Assert.Equal([2], bareIndexed.Data.Select(row => (int)row["Id"]!));
    }

    [Fact]
    public void Prepared_Delete_RemovesEntriesOfIndexesThatIncludeTheKey()
    {
        using (DataVoContext context = Open(StorageMode.Lsm))
        {
            context.Execute("CREATE DATABASE Del");
            context.Execute("USE Del");
            context.Execute("CREATE TABLE T (Id INT PRIMARY KEY, A INT, Code VARCHAR(10) UNIQUE)");
            context.Execute("CREATE INDEX ix_id_a ON T (Id, A)");
            context.Execute("CREATE INDEX ix_a ON T (A)");
            context.Execute("INSERT INTO T (Id, A, Code) VALUES (1, 7, 'one')");
            context.Execute("INSERT INTO T (Id, A, Code) VALUES (2, 7, 'two')");

            QueryResult deleted = context.Prepare("DELETE FROM T WHERE Id = @id").Execute(("id", 1));
            Assert.Contains("Rows affected: 1", deleted.Messages);

            Assert.Equal([2], context.Execute("SELECT Id FROM T WHERE A = 7")[^1].Data.Select(row => (int)row["Id"]!));
            Assert.Empty(context.Execute("SELECT Id FROM T WHERE Id = 1 AND A = 7")[^1].Data);

            QueryResult reinserted = context.Execute("INSERT INTO T (Id, A, Code) VALUES (1, 8, 'one')")[^1];
            Assert.False(reinserted.IsError, string.Join(" | ", reinserted.Messages));
        }

        using DataVoContext reopened = Open(StorageMode.Lsm);
        reopened.Execute("USE Del");
        Assert.Equal(8, reopened.Execute("SELECT A FROM T WHERE Id = 1")[^1].Data[0]["A"]);
        Assert.Equal([2], reopened.Execute("SELECT Id FROM T WHERE A = 7")[^1].Data.Select(row => (int)row["Id"]!));
    }

    [Fact]
    public void Prepared_DeleteReinsertUpdate_CycleKeepsWorking()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        context.Execute("CREATE DATABASE Cycle");
        context.Execute("USE Cycle");
        context.Execute("CREATE TABLE T (Id INT PRIMARY KEY, A INT)");
        DataVoPreparedStatement insert = context.Prepare("INSERT INTO T (Id, A) VALUES (@id, @a)");
        DataVoPreparedStatement update = context.Prepare("UPDATE T SET A = @a WHERE Id = @id");
        DataVoPreparedStatement delete = context.Prepare("DELETE FROM T WHERE Id = @id");

        for (int i = 0; i < 5; i++)
        {
            Assert.False(insert.Execute(("id", 1), ("a", i)).IsError);
            QueryResult updated = update.Execute(("a", i + 100), ("id", 1));
            Assert.False(updated.IsError, string.Join(" | ", updated.Messages));
            Assert.Contains("Rows affected: 1", updated.Messages);
            Assert.Equal(i + 100, context.Execute("SELECT A FROM T WHERE Id = 1")[^1].Data[0]["A"]);
            Assert.Contains("Rows affected: 1", delete.Execute(("id", 1)).Messages);
        }
    }

    [Fact]
    public void Prepared_UpdateAndDelete_RespectAnOpenTransaction()
    {
        using DataVoContext context = Open(StorageMode.Lsm);
        Seed(context);
        DataVoPreparedStatement update = context.Prepare("UPDATE Items SET Qty = @qty WHERE Id = @id");
        DataVoPreparedStatement delete = context.Prepare("DELETE FROM Items WHERE Id = @id");

        context.Execute("BEGIN");
        update.Execute(("qty", 1), ("id", 40_001));
        delete.Execute(("id", 40_002));
        context.Execute("ROLLBACK");

        Assert.Equal(80_002, context.Execute("SELECT Qty FROM Items WHERE Id = 40001")[^1].Data[0]["Qty"]);
        Assert.Single(context.Execute("SELECT Id FROM Items WHERE Id = 40002")[^1].Data);
    }

    [Fact]
    public void AdoNet_ParameterValues_MatchLiteralInsertsForCommonClrTypes()
    {
        using var connection = new DataVoConnection("StorageMode=InMemory;DataSource=AdoTypesDb");
        connection.Open();
        Run(connection, "CREATE TABLE T (Id INT PRIMARY KEY, B BIT, D DATE, F FLOAT, S VARCHAR(20));");

        using (DataVoCommand insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO T (Id, B, D, F, S) VALUES (@id, @b, @d, @f, @s);";
            insert.Parameters.AddWithValue("@id", 1);
            insert.Parameters.AddWithValue("@b", true);
            insert.Parameters.AddWithValue("@d", new DateTime(2024, 2, 29));
            insert.Parameters.AddWithValue("@f", 2.5m);
            insert.Parameters.AddWithValue("@s", DBNull.Value);
            Assert.Equal(1, insert.ExecuteNonQuery());
        }

        using DataVoCommand read = connection.CreateCommand();
        read.CommandText = "SELECT B, D, F, S FROM T WHERE Id = @id AND B = @b;";
        read.Parameters.AddWithValue("@id", 1);
        read.Parameters.AddWithValue("@b", true);
        using var reader = read.ExecuteReader();
        Assert.True(reader.Read());
        Assert.True(reader.GetBoolean(0));
        Assert.Equal(new DateTime(2024, 2, 29), reader.GetDateTime(1));
        Assert.Equal(2.5, reader.GetDouble(2), 3);
        Assert.True(reader.IsDBNull(3));
    }

    [Fact]
    public void AdoNet_PreparedCommand_RebindsWhenTheConnectionReopens()
    {
        using var connection = new DataVoConnection($"StorageMode=InMemory;DataSource=AdoReopen_{Guid.NewGuid():N}");
        connection.Open();
        Run(connection, "CREATE TABLE T (Id INT PRIMARY KEY, A INT);");
        Run(connection, "INSERT INTO T (Id, A) VALUES (1, 10);");

        using DataVoCommand read = connection.CreateCommand();
        read.CommandText = "SELECT A FROM T WHERE Id = @id;";
        read.Parameters.AddWithValue("@id", 1);
        read.Prepare();
        Assert.Equal(10, read.ExecuteScalar());

        connection.Close();
        connection.Open();
        Run(connection, "CREATE TABLE T (Id INT PRIMARY KEY, A INT);");
        Run(connection, "INSERT INTO T (Id, A) VALUES (1, 20);");

        Assert.Equal(20, read.ExecuteScalar());
    }

    [Fact]
    public void AdoNet_ParameterizedBatch_RunsDdlBeforeLaterStatements()
    {
        using var connection = new DataVoConnection("StorageMode=InMemory;DataSource=AdoBatchDb");
        connection.Open();

        using DataVoCommand batch = connection.CreateCommand();
        batch.CommandText = "CREATE TABLE T (Id INT PRIMARY KEY, A INT); INSERT INTO T (Id, A) VALUES (@id, @a); SELECT A FROM T WHERE Id = @id;";
        batch.Parameters.AddWithValue("@id", 1);
        batch.Parameters.AddWithValue("@a", 42);

        Assert.Equal(42, batch.ExecuteScalar());
    }

    private DataVoContext Open(StorageMode mode) => new(new DataVoConfig
    {
        StorageMode = mode,
        DiskStoragePath = _directory,
        LsmStrictFsync = false,
    });

    private static void Seed(DataVoContext context)
    {
        context.Execute("CREATE DATABASE Prep");
        context.Execute("USE Prep");
        context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Qty INT, Price FLOAT, Name VARCHAR(30))");
        for (int id = 40_001; id <= 40_060; id++)
        {
            context.Execute($"INSERT INTO Items (Id, Qty, Price, Name) VALUES ({id}, {id * 2}, {(id * 0.5f).ToString(System.Globalization.CultureInfo.InvariantCulture)}, 'item-{id}')");
        }
    }

    private static int Rows(QueryResult result)
    {
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return result.Data.Count;
    }

    private static void Run(DataVoConnection connection, string sql)
    {
        using DataVoCommand command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static void AssertSameAsPlainSql(DataVoContext context, string preparedSql, string plainSql, params DataVoQueryParameter[] parameters)
    {
        QueryResult expected = context.Execute(plainSql)[^1];
        QueryResult actual = context.Prepare(preparedSql).Execute(parameters);

        Assert.True(
            expected.IsError == actual.IsError,
            $"{plainSql}: plain SQL said '{string.Join(" | ", expected.Messages)}', prepared said '{string.Join(" | ", actual.Messages)}'");
        Assert.Equal(expected.Fields, actual.Fields);
        Assert.Equal(expected.Data.Count, actual.Data.Count);
        for (int rowIndex = 0; rowIndex < expected.Data.Count; rowIndex++)
        {
            foreach ((string field, object? expectedValue) in expected.Data[rowIndex])
            {
                object? actualValue = actual.Data[rowIndex][field];
                Assert.Equal(expectedValue?.GetType(), actualValue?.GetType());
                Assert.Equal(expectedValue, actualValue);
            }
        }
    }
}
