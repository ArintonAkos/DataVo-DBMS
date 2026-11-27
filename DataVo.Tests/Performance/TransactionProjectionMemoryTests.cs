using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Performance;

/// <summary>
/// A SQL <c>SELECT</c> inside an open transaction reads the transaction's snapshot with its own writes overlaid,
/// and still decodes only the columns the statement reads: a wide text column it leaves out is never turned into
/// strings, and the rows it returns are the snapshot's.
/// </summary>
public sealed class TransactionProjectionMemoryTests : IDisposable
{
    private const int Rows = 2_000;
    private const int BodyLength = 2_048;

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"datavo_tx_projection_{Guid.NewGuid():N}");
    private readonly Guid _tx = Guid.NewGuid();
    private readonly Guid _other = Guid.NewGuid();

    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SqlProjectionInsideATransaction_NeverDecodesTheUnselectedText(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "BEGIN", _tx);
        try
        {
            Ok(db, "UPDATE D SET Kind = 9 WHERE Kind = 0", _tx);
            Ok(db, "DELETE FROM D WHERE Id = 1995", _tx);
            Ok(db, "INSERT INTO D (Id, Kind, Body) VALUES (2001, 9, 'inserted')", _tx);

            Assert.Equal([2001, 1988, 1981], Ok(db, "SELECT Id FROM D WHERE Kind > 8 ORDER BY Id DESC LIMIT 3", _tx).Data.Select(static row => (int)row["Id"]!));
            Assert.Equal(Rows / 7, Convert.ToInt64(Assert.Single(Ok(db, "SELECT COUNT(*) AS N FROM D WHERE Kind = 9", _tx).Data)["N"]));
            Assert.Equal(Rows, Ok(db, "SELECT Id FROM D WHERE Kind >= 0", _tx).Data.Count);

            long whole = Measure(db, "SELECT * FROM D WHERE Kind >= 0");
            long ids = Measure(db, "SELECT Id FROM D WHERE Kind >= 0");
            long bodies = Rows * BodyLength * 2L;
            Assert.True(whole > bodies, $"SELECT * allocated {whole:N0} B");
            Assert.True(whole - ids > bodies * 9 / 10, $"SELECT Id allocated {ids:N0} B against {whole:N0} B for SELECT *: the text column was decoded");
        }
        finally
        {
            Ok(db, "ROLLBACK", _tx);
        }

        Assert.Empty(Ok(db, "SELECT Id FROM D WHERE Kind = 9", _tx).Data);
        Assert.Equal(Rows, Ok(db, "SELECT Id FROM D", _tx).Data.Count);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SqlProjectionInsideATransaction_ReadsTheSnapshotFromBegin(StorageMode mode)
    {
        using DataVoContext db = Open(mode);
        Ok(db, "BEGIN", _tx);
        try
        {
            Ok(db, "UPDATE D SET Kind = 3 WHERE Id = 1999", _tx);
            Ok(db, "UPDATE D SET Kind = 5 WHERE Id = 2000", _other);
            Ok(db, "DELETE FROM D WHERE Id = 1998", _other);
            Ok(db, "INSERT INTO D (Id, Kind, Body) VALUES (2002, 1, 'other')", _other);

            QueryResult tail = Ok(db, "SELECT Id, Kind FROM D WHERE Id > 1996 ORDER BY Id", _tx);
            Assert.Equal([(1997, 2), (1998, 3), (1999, 3), (2000, 5)], tail.Data.Select(static row => ((int)row["Id"]!, (int)row["Kind"]!)));

            QueryResult groups = Ok(db, "SELECT Kind, COUNT(*) AS N FROM D GROUP BY Kind ORDER BY Kind", _tx);
            Assert.Equal(Enumerable.Range(0, 7).Select(kind => (kind, (long)Enumerable.Range(1, Rows).Count(id => (id == 1999 ? 3 : id % 7) == kind))),
                groups.Data.Select(static row => ((int)row["Kind"]!, Convert.ToInt64(row["N"]))));
        }
        finally
        {
            Ok(db, "ROLLBACK", _tx);
        }

        QueryResult after = Ok(db, "SELECT Id, Kind FROM D WHERE Id > 1996 ORDER BY Id", _tx);
        Assert.Equal([(1997, 2), (1999, 4), (2000, 5), (2002, 1)], after.Data.Select(static row => ((int)row["Id"]!, (int)row["Kind"]!)));
    }

    private long Measure(DataVoContext db, string sql)
    {
        Ok(db, sql, _tx); // prepare and warm the path
        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.NotEmpty(Ok(db, sql, _tx).Data);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private DataVoContext Open(StorageMode mode)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = mode,
            DataDirectory = mode == StorageMode.InMemory ? null! : _directory,
        });
        Ok(db, "CREATE DATABASE P", _tx);
        Ok(db, "USE P", _tx);
        Ok(db, "USE P", _other);
        Ok(db, "CREATE TABLE D (Id INT PRIMARY KEY, Kind INT, Body VARCHAR)", _tx);
        string body = new('x', BodyLength);
        for (int id = 1; id <= Rows; id++)
        {
            Ok(db, $"INSERT INTO D (Id, Kind, Body) VALUES ({id}, {id % 7}, '{body}')", _tx);
        }

        return db;
    }

    private static QueryResult Ok(DataVoContext db, string sql, Guid session)
    {
        QueryResult result = db.Execute(sql, session)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
