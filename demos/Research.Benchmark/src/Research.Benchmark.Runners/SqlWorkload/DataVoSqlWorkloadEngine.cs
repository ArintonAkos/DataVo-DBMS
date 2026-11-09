using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace Research.Benchmark.Runners.SqlWorkload;

/// <summary>
/// DataVo in durable LSM mode. Rows are loaded through the typed batch API (the bulk path an
/// application would use); every query is prepared once and executed with bound parameters.
/// </summary>
public sealed class DataVoSqlWorkloadEngine : ISqlWorkloadEngine
{
    private const int BatchSize = 65_536;
    private DataVoContext? _context;
    private string? _directory;
    private DataVoPreparedStatement? _pointSelect;
    private DataVoPreparedStatement? _guidPointSelect;
    private DataVoPreparedStatement? _secondaryLookup;
    private DataVoPreparedStatement? _rangeScan;
    private DataVoPreparedStatement? _filteredCount;
    private DataVoPreparedStatement? _groupByStatus;
    private DataVoPreparedStatement? _joinByRegion;
    private DataVoPreparedStatement? _topTen;
    private DataVoPreparedStatement? _updateAmount;
    private DataVoPreparedStatement? _delete;

    public string Name => DataVoBenchmarkName.Format("DataVo (LSM)");

    public string Notes => "LSM mode, commit log without fsync per commit; prepared statements with bound parameters, bulk load via InsertTypedBatch.";

    public void Create(string directory)
    {
        _directory = directory;
        _context = Open(directory);
        Ok("CREATE DATABASE Bench");
        Ok("USE Bench");
        Ok("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR(40), Region VARCHAR(10))");
        Ok("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, Status VARCHAR(12), Amount FLOAT, Qty INT)");
        Ok("CREATE INDEX ix_orders_customer ON Orders (CustomerId)");
        Ok("CREATE TABLE GuidItems (Id GUID PRIMARY KEY, Name VARCHAR(40), Value INT)");
    }

    public void LoadOrders(SqlWorkloadData data)
    {
        InsertBatched("Customers", new ReactiveRowSchema("Id", "Name", "Region"), data.Customers, static (row, i) =>
        {
            row[0] = CellValue.From(i);
            row[1] = CellValue.From("customer-" + i);
            row[2] = CellValue.From(SqlWorkloadData.RegionOf(i));
        }, 3);

        int customers = data.Customers;
        InsertBatched("Orders", new ReactiveRowSchema("Id", "CustomerId", "Status", "Amount", "Qty"), data.Orders, (row, i) =>
        {
            row[0] = CellValue.From(i);
            row[1] = CellValue.From(SqlWorkloadData.CustomerOf(i, customers));
            row[2] = CellValue.From(SqlWorkloadData.StatusOf(i));
            row[3] = CellValue.From(SqlWorkloadData.AmountOf(i));
            row[4] = CellValue.From(SqlWorkloadData.QtyOf(i));
        }, 5);
    }

    public void LoadGuids(SqlWorkloadData data)
    {
        Guid[] keys = data.Guids;
        InsertBatched("GuidItems", new ReactiveRowSchema("Id", "Name", "Value"), keys.Length, (row, i) =>
        {
            row[0] = CellValue.From(keys[i - 1]);
            row[1] = CellValue.From("item-" + (i % 1000));
            row[2] = CellValue.From(i % 97);
        }, 3);
    }

    public long PointSelect(int id)
    {
        DataVoPreparedStatement stmt = _pointSelect ??= Ctx.Prepare("SELECT Id, CustomerId, Status, Amount, Qty FROM Orders WHERE Id = @id");
        QueryResult result = Check(stmt.ExecuteBorrowed(("id", id)));
        return result.Data.Count == 0 ? -1 : (int)result.Data[0]["Qty"]!;
    }

    public long GuidPointSelect(Guid key)
    {
        DataVoPreparedStatement stmt = _guidPointSelect ??= Ctx.Prepare("SELECT Name, Value FROM GuidItems WHERE Id = @id");
        return Check(stmt.ExecuteBorrowed(("id", key))).Data.Count;
    }

    public long SecondaryLookup(int customerId)
    {
        DataVoPreparedStatement stmt = _secondaryLookup ??= Ctx.Prepare("SELECT Id, Amount FROM Orders WHERE CustomerId = @customerId");
        return Check(stmt.Execute(("customerId", customerId))).Data.Count;
    }

    public long RangeScan(int fromId, int width)
    {
        DataVoPreparedStatement stmt = _rangeScan ??= Ctx.Prepare("SELECT Id, Amount FROM Orders WHERE Id >= @fromId AND Id < @toId");
        return Check(stmt.Execute(("fromId", fromId), ("toId", fromId + width))).Data.Count;
    }

    public long FilteredCount()
    {
        DataVoPreparedStatement stmt = _filteredCount ??= Ctx.Prepare("SELECT COUNT(*) AS N FROM Orders WHERE Status = 'paid'");
        QueryResult result = Check(stmt.Execute());
        return Convert.ToInt64(result.Data[0].Values.First());
    }

    public long GroupByStatus()
    {
        DataVoPreparedStatement stmt = _groupByStatus ??= Ctx.Prepare("SELECT Status, COUNT(*) AS N, AVG(Amount) AS A FROM Orders GROUP BY Status");
        QueryResult result = Check(stmt.Execute());
        return result.Data.Sum(static row => Convert.ToInt64(row["N"]));
    }

    public long JoinByRegion()
    {
        DataVoPreparedStatement stmt = _joinByRegion ??= Ctx.Prepare("SELECT c.Region, SUM(o.Amount) AS Total FROM Orders o JOIN Customers c ON o.CustomerId = c.Id GROUP BY c.Region");
        return Check(stmt.Execute()).Data.Count;
    }

    public long TopTen()
    {
        DataVoPreparedStatement stmt = _topTen ??= Ctx.Prepare("SELECT Id, Amount FROM Orders ORDER BY Amount DESC LIMIT 10");
        return Check(stmt.Execute()).Data.Count;
    }

    public void UpdateAmount(int id, double amount)
    {
        DataVoPreparedStatement stmt = _updateAmount ??= Ctx.Prepare("UPDATE Orders SET Amount = @amount WHERE Id = @id");
        Check(stmt.Execute(("amount", amount), ("id", id)));
    }

    public void Delete(int id)
    {
        DataVoPreparedStatement stmt = _delete ??= Ctx.Prepare("DELETE FROM Orders WHERE Id = @id");
        Check(stmt.Execute(("id", id)));
    }

    public void Close()
    {
        _pointSelect = null;
        _guidPointSelect = null;
        _secondaryLookup = null;
        _rangeScan = null;
        _filteredCount = null;
        _groupByStatus = null;
        _joinByRegion = null;
        _topTen = null;
        _updateAmount = null;
        _delete = null;
        _context?.Dispose();
        _context = null;
    }

    public void Reopen()
    {
        Close();
        _context = Open(_directory!);
        Ok("USE Bench");
    }

    public long SizeOnDiskBytes() => SqlWorkloadRunner.DirectorySize(_directory!);

    public void Dispose() => Close();

    private static DataVoContext Open(string directory) => new(new DataVoConfig
    {
        StorageMode = StorageMode.Lsm,
        DiskStoragePath = directory,
        LsmStrictFsync = false,
    });

    private void InsertBatched(string table, ReactiveRowSchema schema, int count, Action<CellValue[], int> fill, int width)
    {
        int capacity = Math.Min(count, BatchSize);
        var buffers = new CellValue[capacity][];
        for (int i = 0; i < capacity; i++)
        {
            buffers[i] = new CellValue[width];
        }

        var rows = new List<CellValue[]>(capacity);
        for (int i = 1; i <= count; i++)
        {
            CellValue[] row = buffers[rows.Count];
            fill(row, i);
            rows.Add(row);
            if (rows.Count == capacity)
            {
                Ctx.InsertTypedBatch(table, schema, rows, callerOwnsRowBuffers: true);
                rows.Clear();
            }
        }

        if (rows.Count > 0)
        {
            Ctx.InsertTypedBatch(table, schema, rows, callerOwnsRowBuffers: true);
        }
    }

    private DataVoContext Ctx => _context ?? throw new InvalidOperationException("DataVo is not open.");

    private void Ok(string sql) => Query(sql);

    private QueryResult Query(string sql) => Check(Ctx.Execute(sql).Last());

    private static QueryResult Check(QueryResult result)
    {
        if (result.IsError)
        {
            throw new InvalidOperationException($"DataVo failed: {string.Join("; ", result.Messages)}");
        }

        return result;
    }
}
