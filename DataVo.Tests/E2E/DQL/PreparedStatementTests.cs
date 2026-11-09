using DataVo.Core;
using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Data;

namespace DataVo.Tests.E2E.DQL;

/// <summary>
/// End-to-end tests covering <see cref="DataVoPreparedStatement"/>, parameter binding, SQL injection immunity,
/// multi-threaded concurrent execution, ADO.NET <see cref="DataVoCommand.Prepare"/>, and steady-state zero-allocation point lookups.
/// </summary>
public class PreparedStatementTests
{
    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void PreparedPointSelect_IntAndGuidPrimaryKey_ExecutesAcrossLoopCorrectly(StorageMode mode)
    {
        string dir = CreateTempDir();
        try
        {
            using var context = CreateContext(mode, dir);
            context.Execute("CREATE DATABASE PrepDb");
            context.Execute("USE PrepDb");
            context.Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Status VARCHAR(16), Amount FLOAT, Qty INT)");
            context.Execute("CREATE TABLE GuidItems (Id GUID PRIMARY KEY, Name VARCHAR(40), Value INT)");

            var orderSchema = new ReactiveRowSchema("Id", "CustomerId", "Status", "Amount", "Qty");
            var orderRows = new List<CellValue[]>(200);
            for (int i = 1; i <= 200; i++)
            {
                orderRows.Add(
                [
                    CellValue.From(i),
                    CellValue.From(i % 20),
                    CellValue.From(i % 2 == 0 ? "paid" : "open"),
                    CellValue.From(i * 1.5),
                    CellValue.From(i * 10),
                ]);
            }

            context.InsertTypedBatch("Orders", orderSchema, orderRows);

            var guids = new Guid[100];
            var guidSchema = new ReactiveRowSchema("Id", "Name", "Value");
            var guidRows = new List<CellValue[]>(100);
            for (int i = 0; i < 100; i++)
            {
                guids[i] = Guid.NewGuid();
                guidRows.Add(
                [
                    CellValue.From(guids[i]),
                    CellValue.From($"item-{i}"),
                    CellValue.From(i * 3),
                ]);
            }

            context.InsertTypedBatch("GuidItems", guidSchema, guidRows);

            DataVoPreparedStatement orderLookup = context.Prepare(
                "SELECT Id, CustomerId, Status, Amount, Qty FROM Orders WHERE Id = @id");
            DataVoPreparedStatement guidLookup = context.Prepare(
                "SELECT Name, Value FROM GuidItems WHERE Id = @id");

            for (int i = 1; i <= 200; i++)
            {
                QueryResult res = orderLookup.Execute(("id", i));
                Assert.False(res.IsError);
                Assert.Single(res.Data);
                Assert.Equal(i, res.Data[0]["Id"]);
                Assert.Equal(i * 10, res.Data[0]["Qty"]);
                Assert.Equal(i % 2 == 0 ? "paid" : "open", res.Data[0]["Status"]);
            }

            QueryResult missing = orderLookup.Execute(("id", 999_999));
            Assert.False(missing.IsError);
            Assert.Empty(missing.Data);

            for (int i = 0; i < 100; i++)
            {
                QueryResult res = guidLookup.Execute(("id", guids[i]));
                Assert.False(res.IsError);
                Assert.Single(res.Data);
                Assert.Equal($"item-{i}", res.Data[0]["Name"]);
                Assert.Equal(i * 3, res.Data[0]["Value"]);
            }
        }
        finally
        {
            CleanupTempDir(dir);
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void PreparedSecondaryIndexAndRangeScan_ReturnExpectedRows(StorageMode mode)
    {
        string dir = CreateTempDir();
        try
        {
            using var context = CreateContext(mode, dir);
            context.Execute("CREATE DATABASE PrepIdxDb");
            context.Execute("USE PrepIdxDb");
            context.Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Code VARCHAR(20), Amount FLOAT)");
            context.Execute("CREATE INDEX ix_orders_customer ON Orders (CustomerId)");
            context.Execute("CREATE INDEX ix_orders_code ON Orders (Code)");

            DataVoPreparedStatement insert = context.Prepare(
                "INSERT INTO Orders (Id, CustomerId, Code, Amount) VALUES (@id, @cust, @code, @amount)");

            for (int i = 1; i <= 50; i++)
            {
                QueryResult ins = insert.Execute(
                    ("id", i),
                    ("cust", i % 5),
                    ("code", $"CODE-{i:D3}"),
                    ("amount", i * 2.5));
                Assert.False(ins.IsError);
            }

            DataVoPreparedStatement byCustomer = context.Prepare(
                "SELECT Id, Amount FROM Orders WHERE CustomerId = @customerId");
            for (int cust = 0; cust < 5; cust++)
            {
                QueryResult res = byCustomer.Execute(("customerId", cust));
                Assert.False(res.IsError);
                Assert.Equal(10, res.Data.Count);
            }

            DataVoPreparedStatement byCode = context.Prepare(
                "SELECT Id, CustomerId FROM Orders WHERE Code = @code");
            QueryResult codeRes = byCode.Execute(("code", "CODE-025"));
            Assert.False(codeRes.IsError);
            Assert.Single(codeRes.Data);
            Assert.Equal(25, codeRes.Data[0]["Id"]);

            DataVoPreparedStatement rangeScan = context.Prepare(
                "SELECT Id, Amount FROM Orders WHERE Id >= @fromId AND Id < @toId");
            QueryResult rangeRes = rangeScan.Execute(("fromId", 10), ("toId", 25));
            Assert.False(rangeRes.IsError);
            Assert.Equal(15, rangeRes.Data.Count);
        }
        finally
        {
            CleanupTempDir(dir);
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void PreparedInsertUpdateAndDelete_MutateRowsCorrectly(StorageMode mode)
    {
        string dir = CreateTempDir();
        try
        {
            using var context = CreateContext(mode, dir);
            context.Execute("CREATE DATABASE PrepDmlDb");
            context.Execute("USE PrepDmlDb");
            context.Execute("CREATE TABLE Accounts (Id INT PRIMARY KEY, Owner VARCHAR(40), Balance FLOAT)");
            context.Execute("CREATE INDEX ix_accounts_owner ON Accounts (Owner)");

            DataVoPreparedStatement insert = context.Prepare(
                "INSERT INTO Accounts (Id, Owner, Balance) VALUES (@id, @owner, @balance)");
            DataVoPreparedStatement update = context.Prepare(
                "UPDATE Accounts SET Balance = @balance WHERE Id = @id");
            DataVoPreparedStatement delete = context.Prepare(
                "DELETE FROM Accounts WHERE Id = @id");
            DataVoPreparedStatement selectById = context.Prepare(
                "SELECT Id, Owner, Balance FROM Accounts WHERE Id = @id");

            for (int i = 1; i <= 20; i++)
            {
                Assert.False(insert.Execute(("id", i), ("owner", $"User-{i}"), ("balance", i * 100.0)).IsError);
            }

            for (int i = 1; i <= 20; i++)
            {
                QueryResult upd = update.Execute(("balance", i * 250.5), ("id", i));
                Assert.False(upd.IsError);
                Assert.Contains(upd.Messages, m => m.Contains("Rows affected: 1", StringComparison.Ordinal));

                QueryResult row = selectById.Execute(("id", i));
                Assert.Single(row.Data);
                Assert.Equal(i * 250.5, Convert.ToDouble(row.Data[0]["Balance"]), 3);
            }

            for (int i = 1; i <= 10; i++)
            {
                QueryResult del = delete.Execute(("id", i));
                Assert.False(del.IsError);
                Assert.Contains(del.Messages, m => m.Contains("Rows affected: 1", StringComparison.Ordinal));
                Assert.Empty(selectById.Execute(("id", i)).Data);
            }

            QueryResult delMissing = delete.Execute(("id", 999));
            Assert.False(delMissing.IsError);
            Assert.Contains(delMissing.Messages, m => m.Contains("Rows affected: 0", StringComparison.Ordinal));
        }
        finally
        {
            CleanupTempDir(dir);
        }
    }

    [Fact]
    public void PreparedStatement_SqlInjectionPayloads_AreTreatedStrictlyAsLiterals()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        context.Execute("CREATE DATABASE SecDb");
        context.Execute("USE SecDb");
        context.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(100), Role VARCHAR(30))");
        context.Execute("CREATE INDEX ix_users_name ON Users (Name)");

        DataVoPreparedStatement insert = context.Prepare(
            "INSERT INTO Users (Id, Name, Role) VALUES (@id, @name, @role)");
        DataVoPreparedStatement selectByName = context.Prepare(
            "SELECT Id, Name, Role FROM Users WHERE Name = @name");
        DataVoPreparedStatement deleteByName = context.Prepare(
            "DELETE FROM Users WHERE Name = @name");

        Assert.False(insert.Execute(("id", 1), ("name", "Alice"), ("role", "admin")).IsError);
        Assert.False(insert.Execute(("id", 2), ("name", "Bob"), ("role", "user")).IsError);

        string payload1 = "' OR 1=1 --";
        string payload2 = "Alice' OR '1'='1";
        string payload3 = "'; DROP TABLE Users; --";

        // Attempting SQL injection on SELECT must return 0 rows, not all rows.
        Assert.Empty(selectByName.Execute(("name", payload1)).Data);
        Assert.Empty(selectByName.Execute(("name", payload2)).Data);
        Assert.Empty(selectByName.Execute(("name", payload3)).Data);

        // Attempting SQL injection on DELETE must affect 0 rows.
        QueryResult delAttempt = deleteByName.Execute(("name", payload1));
        Assert.False(delAttempt.IsError);
        Assert.Equal(2, context.Execute("SELECT * FROM Users")[0].Data.Count);

        // Inserting the injection payload stores the exact literal text, including single quotes.
        Assert.False(insert.Execute(("id", 3), ("name", payload1), ("role", "guest")).IsError);
        QueryResult literalMatch = selectByName.Execute(("name", payload1));
        Assert.Single(literalMatch.Data);
        Assert.Equal(3, literalMatch.Data[0]["Id"]);
        Assert.Equal(payload1, literalMatch.Data[0]["Name"]);
    }

    [Fact]
    public void PreparedStatement_ConcurrentExecutionAcrossThreads_IsolatesParameters()
    {
        string dir = CreateTempDir();
        try
        {
            using var context = CreateContext(StorageMode.Lsm, dir);
            context.Execute("CREATE DATABASE ConcDb");
            context.Execute("USE ConcDb");
            context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Label VARCHAR(32), Score INT)");

            var schema = new ReactiveRowSchema("Id", "Label", "Score");
            var rows = new List<CellValue[]>(256);
            for (int i = 1; i <= 256; i++)
            {
                rows.Add([CellValue.From(i), CellValue.From($"label-{i}"), CellValue.From(i * 7)]);
            }

            context.InsertTypedBatch("Items", schema, rows);

            DataVoPreparedStatement pointSelect = context.Prepare(
                "SELECT Id, Label, Score FROM Items WHERE Id = @id");
            DataVoPreparedStatement exprSelect = context.Prepare(
                "SELECT Id, Score FROM Items WHERE Id = @id AND Label = @label");

            Parallel.For(0, 8, worker =>
            {
                var rng = new Random(1000 + worker);
                for (int iter = 0; iter < 250; iter++)
                {
                    int id = rng.Next(1, 257);
                    QueryResult fastRes = pointSelect.Execute(("id", id));
                    Assert.False(fastRes.IsError);
                    Assert.Single(fastRes.Data);
                    Assert.Equal(id, fastRes.Data[0]["Id"]);
                    Assert.Equal($"label-{id}", fastRes.Data[0]["Label"]);
                    Assert.Equal(id * 7, fastRes.Data[0]["Score"]);

                    QueryResult boundRes = exprSelect.Execute(("id", id), ("label", $"label-{id}"));
                    Assert.False(boundRes.IsError);
                    Assert.Single(boundRes.Data);
                    Assert.Equal(id * 7, boundRes.Data[0]["Score"]);
                }
            });
        }
        finally
        {
            CleanupTempDir(dir);
        }
    }

    [Fact]
    public void PreparedStatement_MissingNullAndTypeMismatchParameters_PreserveErrorContracts()
    {
        using var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        context.Execute("CREATE DATABASE ErrDb");
        context.Execute("USE ErrDb");
        context.Execute("CREATE TABLE T (Id INT PRIMARY KEY, Name VARCHAR(30))");
        context.Execute("INSERT INTO T (Id, Name) VALUES (1, 'One')");

        DataVoPreparedStatement byId = context.Prepare("SELECT Id, Name FROM T WHERE Id = @id");

        // Missing parameter returns Binding error code.
        QueryResult missingParam = byId.Execute();
        Assert.True(missingParam.IsError);
        Assert.Equal(DataVoErrorCodes.Binding, missingParam.ErrorCode);

        // Non-numeric text compared against INT column returns TypeMismatch error code.
        QueryResult typeMismatch = byId.Execute(("id", "not-a-number"));
        Assert.True(typeMismatch.IsError);
        Assert.Equal(DataVoErrorCodes.TypeMismatch, typeMismatch.ErrorCode);

        // NULL parameter in '=' predicate returns 0 rows without error.
        QueryResult nullEq = byId.Execute(DataVoQueryParameter.Null("id"));
        Assert.False(nullEq.IsError);
        Assert.Empty(nullEq.Data);
    }

    [Fact]
    public void AdoNetCommand_Prepare_ExecutesParameterizedQueriesWithoutStringSubstitution()
    {
        using var conn = new DataVoConnection("StorageMode=InMemory;DataSource=AdoPrepDb");
        conn.Open();

        using (var setup = conn.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE Products (Id INT PRIMARY KEY, Title VARCHAR(60), Price FLOAT);";
            setup.ExecuteNonQuery();
        }

        using (var insert = conn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO Products (Id, Title, Price) VALUES (@id, @title, @price);";
            var pId = insert.Parameters.AddWithValue("@id", 0);
            var pTitle = insert.Parameters.AddWithValue("@title", "");
            var pPrice = insert.Parameters.AddWithValue("@price", 0.0);
            insert.Prepare();

            for (int i = 1; i <= 10; i++)
            {
                pId.Value = i;
                pTitle.Value = i == 5 ? "' OR 1=1 --" : $"Product-{i}";
                pPrice.Value = i * 9.99;
                Assert.Equal(1, insert.ExecuteNonQuery());
            }
        }

        using (var lookup = conn.CreateCommand())
        {
            lookup.CommandText = "SELECT Title, Price FROM Products WHERE Id = @id;";
            var pId = lookup.Parameters.AddWithValue("@id", 5);
            lookup.Prepare();

            using var reader = lookup.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal("' OR 1=1 --", reader.GetString(0));
            Assert.False(reader.Read());
        }
    }

    [Fact]
    public void PreparedPointLookup_LsmSteadyState_AllocatesZeroBytes()
    {
        string dir = CreateTempDir();
        try
        {
            using (var seedContext = CreateContext(StorageMode.Lsm, dir))
            {
                seedContext.Execute("CREATE DATABASE AllocDb");
                seedContext.Execute("USE AllocDb");
                seedContext.Execute("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Status VARCHAR(12), Amount FLOAT, Qty INT)");

                var schema = new ReactiveRowSchema("Id", "CustomerId", "Status", "Amount", "Qty");
                var rows = new List<CellValue[]>(1000);
                for (int i = 1; i <= 1000; i++)
                {
                    rows.Add(
                    [
                        CellValue.From(i),
                        CellValue.From(i % 50),
                        CellValue.From(i % 2 == 0 ? "paid" : "open"),
                        CellValue.From(i * 1.25),
                        CellValue.From(i % 20),
                    ]);
                }

                seedContext.InsertTypedBatch("Orders", schema, rows);
            }

            // Reopen so rows are served from SSTable on disk (matching the benchmark steady state).
            using var context = CreateContext(StorageMode.Lsm, dir);
            context.Execute("USE AllocDb");

            DataVoPreparedStatement lookup = context.Prepare(
                "SELECT Id, CustomerId, Status, Amount, Qty FROM Orders WHERE Id = @id");

            // Warm up thread-local workspace, integer cache, and string cache.
            for (int i = 1; i <= 1000; i++)
            {
                QueryResult warm = lookup.ExecuteBorrowed(("id", i));
                Assert.Single(warm.Data);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            long sum = 0;
            for (int i = 1; i <= 1000; i++)
            {
                QueryResult res = lookup.ExecuteBorrowed(("id", i));
                sum += (int)res.Data[0]["Qty"]!;
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.True(sum > 0);
            Assert.Equal(0, allocated);
        }
        finally
        {
            CleanupTempDir(dir);
        }
    }

    private static DataVoContext CreateContext(StorageMode mode, string directory) =>
        new(new DataVoConfig
        {
            StorageMode = mode,
            DiskStoragePath = directory,
            LsmStrictFsync = false,
        });

    private static string CreateTempDir()
    {
        string path = Path.Combine(Path.GetTempPath(), $"datavo_prep_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void CleanupTempDir(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
            }
        }
        catch
        {
            // Ignore best-effort cleanup errors.
        }
    }
}
