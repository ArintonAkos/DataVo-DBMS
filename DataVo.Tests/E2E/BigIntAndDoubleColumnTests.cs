using DataVo.Core;
using DataVo.Core.Annotations;
using DataVo.Core.Contracts.Results;
using DataVo.Core.PreparedStatements;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.ColumnTypes;

[DataVoTable("ledger")]
public sealed class LedgerEntry
{
    [PrimaryKey] public long Id { get; set; }

    [Indexed] public long Account { get; set; }

    public double Amount { get; set; }

    public long? Parent { get; set; }

    public double? Rate { get; set; }
}

/// <summary>
/// BIGINT and DOUBLE on every read and write path. The values around 2^53 are chosen because a double
/// cannot tell them apart: anything that routes a BIGINT through a double fails these tests.
/// </summary>
public sealed class BigIntAndDoubleColumnTests : IDisposable
{
    private const long Exact = 9007199254740992; // 2^53
    private const long NextToExact = 9007199254740993; // has no double of its own

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_types_{Guid.NewGuid():N}");

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Disk, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [InlineData("LONG", "BIGINT")]
    [InlineData("DECIMAL(10,2)", "DOUBLE")]
    [InlineData("NUMERIC", "DOUBLE")]
    [InlineData("MONEY", "DOUBLE")]
    [InlineData("DATETIME", "DATE")]
    [InlineData("TIMESTAMP", "DATE")]
    [InlineData("BLOB", "Supported types")]
    [InlineData("POINT", "Supported types")]
    [InlineData("HABIT", "Supported types")]
    public void UnsupportedType_IsRejected_WithTheTypeNamed(string type, string advice)
    {
        using DataVoContext db = Open(StorageMode.InMemory);

        QueryResult result = db.Execute($"CREATE TABLE T (Id INT PRIMARY KEY, V {type})")[^1];

        Assert.True(result.IsError);
        string message = string.Join(" ", result.Messages);
        Assert.Contains(type.Split('(')[0], message);
        Assert.Contains(advice, message);
        Assert.True(db.Execute("SELECT * FROM T")[^1].IsError);
    }

    [Fact]
    public void UnsupportedType_IsRejected_ByAlterTable()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY)");

        Assert.True(db.Execute("ALTER TABLE T ADD COLUMN V MONEY")[^1].IsError);
    }

    [Theory]
    [InlineData("INTEGER", "5", 5)]
    [InlineData("SMALLINT", "5", 5)]
    [InlineData("BIGINT", "5", 5L)]
    [InlineData("REAL", "1.5", 1.5f)]
    [InlineData("FLOAT", "1.5", 1.5f)]
    [InlineData("DOUBLE", "1.5", 1.5d)]
    [InlineData("DOUBLE PRECISION", "1.5", 1.5d)]
    [InlineData("BOOLEAN", "true", true)]
    [InlineData("TEXT", "'x'", "x")]
    [InlineData("CHAR(4)", "'x'", "x")]
    public void SupportedName_StoresTheDeclaredType(string type, string literal, object expected)
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        Ok(db, $"CREATE TABLE T (Id INT PRIMARY KEY, V {type})");
        Ok(db, $"INSERT INTO T (Id, V) VALUES (1, {literal})");

        object? value = Select(db, "SELECT V FROM T").Single()["V"];

        Assert.Equal(expected, value);
        Assert.IsType(expected.GetType(), value);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BigInt_RoundTripsItsWholeRange(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V BIGINT)");
        long[] values = [long.MaxValue, long.MinValue, NextToExact, -NextToExact, 5_000_000_000, 0];
        for (int i = 0; i < values.Length; i++)
        {
            Ok(db, $"INSERT INTO T (Id, V) VALUES ({i}, {values[i]})");
        }

        Ok(db, "INSERT INTO T (Id, V) VALUES (100, NULL)");

        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(values[i], Assert.IsType<long>(Select(db, $"SELECT V FROM T WHERE Id = {i}").Single()["V"]));
        }

        Assert.Null(Select(db, "SELECT V FROM T WHERE Id = 100").Single()["V"]);
    }

    [Theory]
    [InlineData("9223372036854775808")]
    [InlineData("1.5")]
    [InlineData("'abc'")]
    public void BigInt_RejectsWhatItCannotHold(string literal)
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V BIGINT)");

        Assert.True(db.Execute($"INSERT INTO T (Id, V) VALUES (1, {literal})")[^1].IsError);
    }

    [Fact]
    public void Int_StillRejectsSixtyFourBitValues()
    {
        using DataVoContext db = Open(StorageMode.InMemory);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V INT)");

        Assert.True(db.Execute("INSERT INTO T (Id, V) VALUES (1, 5000000000)")[^1].IsError);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BigInt_ComparesExactly_AboveWhatADoubleCanHold(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V BIGINT)");
        Ok(db, $"INSERT INTO T (Id, V) VALUES (1, {Exact})");
        Ok(db, $"INSERT INTO T (Id, V) VALUES (2, {NextToExact})");

        Assert.Equal(2, Ids(db, $"SELECT Id FROM T WHERE V = {NextToExact}").Single());
        Assert.Equal(1, Ids(db, $"SELECT Id FROM T WHERE V = {Exact}").Single());
        Assert.Equal(2, Ids(db, $"SELECT Id FROM T WHERE V > {Exact}").Single());
        Assert.Equal(1, Ids(db, $"SELECT Id FROM T WHERE V < {NextToExact}").Single());
        Assert.Equal(1, Ids(db, $"SELECT Id FROM T WHERE V <> {NextToExact}").Single());

        DataVoPreparedStatement equal = db.Prepare("SELECT Id FROM T WHERE V = @v");
        Assert.Equal(2, equal.Execute(new DataVoQueryParameter("v", NextToExact)).Data.Single()["Id"]);
        DataVoPreparedStatement greater = db.Prepare("SELECT Id FROM T WHERE V > @v");
        Assert.Equal(2, greater.Execute(new DataVoQueryParameter("v", Exact)).Data.Single()["Id"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BigInt_PrimaryKey_HoldsKeysADoubleCannotTellApart(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "CREATE TABLE T (Id BIGINT PRIMARY KEY, V INT)");
        Ok(db, $"INSERT INTO T (Id, V) VALUES ({Exact}, 1)");
        Ok(db, $"INSERT INTO T (Id, V) VALUES ({NextToExact}, 2)");
        Ok(db, $"INSERT INTO T (Id, V) VALUES ({long.MaxValue}, 3)");
        Ok(db, $"INSERT INTO T (Id, V) VALUES ({long.MinValue + 1}, 4)");

        Assert.True(db.Execute($"INSERT INTO T (Id, V) VALUES ({NextToExact}, 9)")[^1].IsError);

        Assert.Equal(2, Select(db, $"SELECT V FROM T WHERE Id = {NextToExact}").Single()["V"]);
        Assert.Equal(NextToExact, Assert.IsType<long>(Select(db, $"SELECT Id FROM T WHERE Id = {NextToExact}").Single()["Id"]));
        Assert.Equal(3, Select(db, $"SELECT V FROM T WHERE Id = {long.MaxValue}").Single()["V"]);

        Assert.Equal(new long[] { NextToExact, long.MaxValue }, Keys(db, $"SELECT Id FROM T WHERE Id > {Exact}"));
        Assert.Equal(new long[] { long.MinValue + 1, Exact }, Keys(db, $"SELECT Id FROM T WHERE Id < {NextToExact}"));
        Assert.Equal(4, Keys(db, $"SELECT Id FROM T WHERE Id <= {long.MaxValue}").Length);
        Assert.Empty(Keys(db, $"SELECT Id FROM T WHERE Id > {long.MaxValue}"));

        DataVoPreparedStatement point = db.Prepare("SELECT Id, V FROM T WHERE Id = @id");
        var row = point.Execute(new DataVoQueryParameter("id", NextToExact)).Data.Single();
        Assert.Equal(NextToExact, row["Id"]);
        Assert.Equal(2, row["V"]);
        Assert.Empty(point.Execute(new DataVoQueryParameter("id", NextToExact + 1)).Data);

        DataVoPreparedStatement range = db.Prepare("SELECT Id FROM T WHERE Id >= @id");
        Assert.Equal(2, range.Execute(new DataVoQueryParameter("id", NextToExact)).Data.Count);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BigInt_Update_And_Delete_ReachTheRightRow(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "CREATE TABLE T (Id BIGINT PRIMARY KEY, A BIGINT, B DOUBLE, C VARCHAR)");
        Ok(db, $"INSERT INTO T (Id, A, B, C) VALUES ({Exact}, 1, 1.5, 'first')");
        Ok(db, $"INSERT INTO T (Id, A, B, C) VALUES ({NextToExact}, 2, 2.5, 'second')");

        DataVoPreparedStatement update = db.Prepare("UPDATE T SET A = @a, B = @b WHERE Id = @id");
        update.Execute(
            new DataVoQueryParameter("a", long.MaxValue),
            new DataVoQueryParameter("b", Math.PI),
            new DataVoQueryParameter("id", NextToExact));

        var changed = Select(db, $"SELECT A, B, C FROM T WHERE Id = {NextToExact}").Single();
        Assert.Equal(long.MaxValue, changed["A"]);
        Assert.Equal(Math.PI, changed["B"]);
        Assert.Equal("second", changed["C"]);
        Assert.Equal(1L, Select(db, $"SELECT A FROM T WHERE Id = {Exact}").Single()["A"]);

        Ok(db, $"UPDATE T SET A = {NextToExact} WHERE Id = {Exact}");
        Assert.Equal(NextToExact, Select(db, $"SELECT A FROM T WHERE Id = {Exact}").Single()["A"]);

        DataVoPreparedStatement delete = db.Prepare("DELETE FROM T WHERE Id = @id");
        delete.Execute(new DataVoQueryParameter("id", NextToExact));

        Assert.Empty(Select(db, $"SELECT Id FROM T WHERE Id = {NextToExact}"));
        Assert.Single(Select(db, $"SELECT Id FROM T WHERE Id = {Exact}"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BigInt_Index_FindsExactRows(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V BIGINT)");
        Ok(db, $"INSERT INTO T (Id, V) VALUES (1, {Exact})");
        Ok(db, $"INSERT INTO T (Id, V) VALUES (2, {NextToExact})");
        Ok(db, "CREATE INDEX IX_V ON T (V)");
        Ok(db, $"INSERT INTO T (Id, V) VALUES (3, {NextToExact})");

        Assert.Equal(new[] { 2, 3 }, Ids(db, $"SELECT Id FROM T WHERE V = {NextToExact}"));
        Assert.Equal(new[] { 1 }, Ids(db, $"SELECT Id FROM T WHERE V = {Exact}"));

        DataVoPreparedStatement lookup = db.Prepare("SELECT Id FROM T WHERE V = @v");
        var found = lookup.Execute(new DataVoQueryParameter("v", NextToExact)).Data.Select(row => (int)row["Id"]!).OrderBy(id => id);
        Assert.Equal(new[] { 2, 3 }, found);

        Ok(db, $"UPDATE T SET V = 7 WHERE Id = 3");
        Assert.Equal(new[] { 2 }, Ids(db, $"SELECT Id FROM T WHERE V = {NextToExact}"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void BigInt_Join_GroupBy_OrderBy_AndAggregates(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "CREATE TABLE A (Id INT PRIMARY KEY, K BIGINT)");
        Ok(db, "CREATE TABLE B (Id INT PRIMARY KEY, K BIGINT, N INT)");
        Ok(db, $"INSERT INTO A (Id, K) VALUES (1, {Exact})");
        Ok(db, $"INSERT INTO A (Id, K) VALUES (2, {NextToExact})");
        Ok(db, $"INSERT INTO B (Id, K, N) VALUES (1, {NextToExact}, 20)");
        Ok(db, $"INSERT INTO B (Id, K, N) VALUES (2, {NextToExact}, 30)");

        var joined = Select(db, "SELECT A.Id AS L, B.N AS R FROM A JOIN B ON A.K = B.K");
        Assert.Equal(2, joined.Count);
        Assert.All(joined, row => Assert.Equal(2, row["L"]));

        var groups = Select(db, "SELECT K, COUNT(*) AS C FROM B GROUP BY K");
        Assert.Equal(NextToExact, groups.Single()["K"]);

        Assert.Equal(new long[] { NextToExact, Exact }, Select(db, "SELECT K FROM A ORDER BY K DESC").Select(row => (long)row["K"]!));
        Assert.Equal(NextToExact, Select(db, "SELECT MAX(K) AS M FROM A").Single()["M"]);
        Assert.Equal(Exact, Select(db, "SELECT MIN(K) AS M FROM A").Single()["M"]);
        Assert.Equal(checked(Exact + NextToExact), Convert.ToInt64(Select(db, "SELECT SUM(K) AS S FROM A").Single()["S"]));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Double_KeepsFullPrecision(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V DOUBLE, F FLOAT)");
        Ok(db, "INSERT INTO T (Id, V, F) VALUES (1, 3.141592653589793, 1.5)");
        Ok(db, "INSERT INTO T (Id, V, F) VALUES (2, 1e300, 2.5)");
        Ok(db, "INSERT INTO T (Id, V, F) VALUES (3, 0.1, 3.5)");

        var first = Select(db, "SELECT V, F FROM T WHERE Id = 1").Single();
        Assert.Equal(Math.PI, Assert.IsType<double>(first["V"]));
        Assert.IsType<float>(first["F"]);
        Assert.Equal(1e300, Select(db, "SELECT V FROM T WHERE Id = 2").Single()["V"]);

        Assert.Equal(new[] { 3 }, Ids(db, "SELECT Id FROM T WHERE V = 0.1"));
        Assert.Equal(new[] { 1 }, Ids(db, "SELECT Id FROM T WHERE V = 3.141592653589793"));
        Assert.Equal(new[] { 2 }, Ids(db, "SELECT Id FROM T WHERE V > 1e299"));

        DataVoPreparedStatement prepared = db.Prepare("SELECT V FROM T WHERE V = @v");
        Assert.Equal(0.1d, prepared.Execute(new DataVoQueryParameter("v", 0.1d)).Data.Single()["V"]);

        Assert.Equal(1e300, Select(db, "SELECT MAX(V) AS M FROM T").Single()["M"]);
        double sum = Convert.ToDouble(Select(db, "SELECT SUM(V) AS S FROM T WHERE Id <> 2").Single()["S"]);
        Assert.Equal(Math.PI + 0.1, sum, 12);
        double average = Convert.ToDouble(Select(db, "SELECT AVG(V) AS A FROM T WHERE Id <> 2").Single()["A"]);
        Assert.Equal((Math.PI + 0.1) / 2, average, 12);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Double_Index_FindsExactRows(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V DOUBLE)");
        Ok(db, "INSERT INTO T (Id, V) VALUES (1, 0.1)");
        Ok(db, "CREATE INDEX IX_V ON T (V)");
        Ok(db, "INSERT INTO T (Id, V) VALUES (2, 3.141592653589793)");

        Assert.Equal(new[] { 1 }, Ids(db, "SELECT Id FROM T WHERE V = 0.1"));
        Assert.Equal(new[] { 2 }, Ids(db, "SELECT Id FROM T WHERE V = 3.141592653589793"));
    }

    [Theory]
    [InlineData(StorageMode.Disk)]
    [InlineData(StorageMode.Lsm)]
    public void Values_SurviveReopen(StorageMode mode)
    {
        using (DataVoContext db = Open(mode))
        {
            Ok(db, "CREATE TABLE T (Id BIGINT PRIMARY KEY, V BIGINT, D DOUBLE, S VARCHAR)");
            Ok(db, $"INSERT INTO T (Id, V, D, S) VALUES ({NextToExact}, {long.MaxValue}, 0.1, 'after')");
        }

        using var reopened = new DataVoContext(Config(mode));
        Ok(reopened, "USE G");
        var row = Select(reopened, $"SELECT Id, V, D, S FROM T WHERE Id = {NextToExact}").Single();
        Assert.Equal(NextToExact, row["Id"]);
        Assert.Equal(long.MaxValue, row["V"]);
        Assert.Equal(0.1d, row["D"]);
        Assert.Equal("after", row["S"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void Transaction_SeesAndRollsBackItsOwnValues(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "CREATE TABLE T (Id BIGINT PRIMARY KEY, V BIGINT, D DOUBLE)");
        Ok(db, "BEGIN");
        Ok(db, $"INSERT INTO T (Id, V, D) VALUES ({NextToExact}, {NextToExact}, 0.1)");

        var row = Select(db, $"SELECT Id, V, D FROM T WHERE V = {NextToExact}").Single();
        Assert.Equal(NextToExact, row["Id"]);
        Assert.Equal(0.1d, row["D"]);
        Assert.Empty(Select(db, $"SELECT Id FROM T WHERE V = {Exact}"));

        Ok(db, "ROLLBACK");
        Assert.Empty(Select(db, $"SELECT Id FROM T WHERE V = {NextToExact}"));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void AlterColumn_WidensIntToBigInt_AndFloatToDouble(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "CREATE TABLE T (Id INT PRIMARY KEY, V INT, F FLOAT)");
        Ok(db, "INSERT INTO T (Id, V, F) VALUES (1, 7, 1.5)");

        Ok(db, "ALTER TABLE T MODIFY COLUMN V BIGINT");
        Ok(db, "ALTER TABLE T MODIFY COLUMN F DOUBLE");
        Ok(db, $"INSERT INTO T (Id, V, F) VALUES (2, {NextToExact}, 0.1)");

        var first = Select(db, "SELECT V, F FROM T WHERE Id = 1").Single();
        Assert.Equal(7L, first["V"]);
        Assert.Equal(1.5d, first["F"]);
        Assert.Equal(NextToExact, Select(db, "SELECT V FROM T WHERE Id = 2").Single()["V"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void IntegerParameters_WidenIntoBigIntAndDouble(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "CREATE TABLE T (Id BIGINT PRIMARY KEY, V BIGINT, D DOUBLE)");

        DataVoPreparedStatement insert = db.Prepare("INSERT INTO T (Id, V, D) VALUES (@id, @v, @d)");
        insert.Execute(new DataVoQueryParameter("id", 1), new DataVoQueryParameter("v", 2), new DataVoQueryParameter("d", 3));

        var row = Select(db, "SELECT Id, V, D FROM T").Single();
        Assert.Equal(1L, row["Id"]);
        Assert.Equal(2L, row["V"]);
        Assert.Equal(3d, row["D"]);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void GeneratedModel_UsesLongAndDouble(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        db.EnsureLedgerEntryTable();

        db.Insert(new LedgerEntry { Id = NextToExact, Account = long.MaxValue, Amount = Math.PI, Parent = null, Rate = 0.1 });
        LedgerEntry loaded = db.GetLedgerEntryById(NextToExact)!;
        Assert.Equal(long.MaxValue, loaded.Account);
        Assert.Equal(Math.PI, loaded.Amount);
        Assert.Null(loaded.Parent);
        Assert.Equal(0.1, loaded.Rate);
        Assert.Null(db.GetLedgerEntryById(Exact));

        Assert.True(db.Update(new LedgerEntry { Id = NextToExact, Account = 1, Amount = 2, Parent = Exact, Rate = null }));
        loaded = db.GetLedgerEntryById(NextToExact)!;
        Assert.Equal(1, loaded.Account);
        Assert.Equal(Exact, loaded.Parent);
        Assert.Null(loaded.Rate);

        db.EnsureLedgerEntryTable();
    }

    private DataVoContext Open(StorageMode mode)
    {
        var db = new DataVoContext(Config(mode));
        Ok(db, "CREATE DATABASE G");
        Ok(db, "USE G");
        return db;
    }

    private DataVoConfig Config(StorageMode mode) => new()
    {
        StorageMode = mode,
        DiskStoragePath = mode == StorageMode.InMemory ? null! : _directory,
    };

    private static void Ok(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
    }

    private static List<Dictionary<string, object?>> Select(DataVoContext db, string sql)
    {
        QueryResult result = db.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql}: {string.Join(" | ", result.Messages)}");
        return [.. result.Data];
    }

    private static int[] Ids(DataVoContext db, string sql) =>
        [.. Select(db, sql).Select(row => (int)row["Id"]!).OrderBy(id => id)];

    private static long[] Keys(DataVoContext db, string sql) =>
        [.. Select(db, sql).Select(row => (long)row["Id"]!).OrderBy(id => id)];
}
