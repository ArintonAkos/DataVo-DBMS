using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace Research.Benchmark.Runners.SqlMatrix;

/// <summary>
/// DataVo through SQL text: prepared statements with bound parameters. The LSM variant runs without a flush
/// per commit (Relaxed) and is reopened after the load so queries read from SSTables; the in-memory variant
/// keeps everything in the managed heap.
/// </summary>
public sealed class DataVoSqlMatrixEngine(StorageMode mode) : ISqlMatrixEngine
{
    private const string Database = "Bench";
    private const int BatchSize = 65_536;
    private readonly Dictionary<string, DataVoPreparedStatement> _prepared = new(StringComparer.Ordinal);
    private DataVoContext? _context;
    private string? _directory;

    public string Name => DataVoBenchmarkName.Format(mode == StorageMode.Lsm ? "DataVo (LSM)" : "DataVo (InMemory)");

    public string Notes => mode == StorageMode.Lsm
        ? "LSM, LsmStrictFsync=false (Relaxed: no flush per commit); bulk load via InsertTypedBatch, then close and reopen; prepared statements with bound parameters."
        : "InMemory engine; bulk load via InsertTypedBatch; prepared statements with bound parameters.";

    public bool SupportsColumnarIndex => true;

    public long? ColumnarScans => _context?.Engine.Columnar.Find(Database, "Orders")?.Scans;

    public void Create(string directory)
    {
        _directory = directory;
        _context = Open();
        Ok($"CREATE DATABASE {Database}");
        Ok($"USE {Database}");
        Ok("CREATE TABLE Customers (Id INT PRIMARY KEY, Name VARCHAR(16), Segment INT, Region VARCHAR(4))");
        Ok("CREATE TABLE Products (Id INT PRIMARY KEY, Name VARCHAR(16), CategoryId INT, Price DOUBLE)");
        Ok("CREATE TABLE Orders (Id INT PRIMARY KEY, CustomerId INT, ProductId INT, ProductTag INT, Status VARCHAR(12), Amount DOUBLE, Qty INT, Discount DOUBLE, Bucket INT, Sku VARCHAR(16))");
        Ok("CREATE INDEX ix_orders_customer ON Orders (CustomerId)");
        Ok("CREATE INDEX ix_orders_product ON Orders (ProductId)");
    }

    public void Load(SqlMatrixData data)
    {
        InsertBatched("Customers", new ReactiveRowSchema("Id", "Name", "Segment", "Region"), data.Customers, 4, static (row, i) =>
        {
            row[0] = CellValue.From(i);
            row[1] = CellValue.From(SqlMatrixData.CustomerName(i));
            row[2] = CellValue.From(SqlMatrixData.CustomerSegment(i));
            row[3] = CellValue.From(SqlMatrixData.CustomerRegion(i));
        });
        InsertBatched("Products", new ReactiveRowSchema("Id", "Name", "CategoryId", "Price"), data.Products, 4, static (row, i) =>
        {
            row[0] = CellValue.From(i);
            row[1] = CellValue.From(SqlMatrixData.ProductName(i));
            row[2] = CellValue.From(SqlMatrixData.ProductCategory(i));
            row[3] = CellValue.From(SqlMatrixData.ProductPrice(i));
        });
        var orders = new ReactiveRowSchema("Id", "CustomerId", "ProductId", "ProductTag", "Status", "Amount", "Qty", "Discount", "Bucket", "Sku");
        InsertBatched("Orders", orders, data.Orders, 10, (row, i) =>
        {
            row[0] = CellValue.From(i);
            row[1] = CellValue.From(data.CustomerId[i]);
            row[2] = CellValue.From(data.ProductId[i]);
            row[3] = CellValue.From(data.ProductId[i]);
            row[4] = CellValue.From(data.Status(i));
            row[5] = CellValue.From(data.Amount[i]);
            row[6] = CellValue.From(data.Qty[i]);
            row[7] = data.Discount[i] is double discount ? CellValue.From(discount) : CellValue.Null;
            row[8] = CellValue.From(data.Bucket[i]);
            row[9] = CellValue.From(data.Sku[i]);
        });
    }

    public void Reopen()
    {
        if (mode != StorageMode.Lsm)
        {
            return;
        }

        CloseContext();
        _context = Open();
        Ok($"USE {Database}");
    }

    public List<object?[]> Query(SqlMatrixQuery query, SqlMatrixParameter[] parameters)
    {
        QueryResult result = Run(query, parameters);
        var rows = new List<object?[]>(result.Data.Count);
        foreach (Dictionary<string, object?> row in result.Data)
        {
            rows.Add([.. row.Values]);
        }

        return rows;
    }

    public long Execute(SqlMatrixQuery query, SqlMatrixParameter[] parameters)
    {
        QueryResult result = Run(query, parameters);
        long sink = 0;
        foreach (Dictionary<string, object?> row in result.Data)
        {
            foreach (object? value in row.Values)
            {
                if (value is not null) sink++;
            }
        }

        return sink >= 0 ? result.Data.Count : -1;
    }

    public void CreateColumnarIndex()
    {
        Ok("CREATE INDEX cx_orders ON Orders (Amount, Qty, Bucket, Discount, ProductTag) USING COLUMNAR");
        _prepared.Clear(); // statements prepared before the index existed are planned again
    }

    public long SizeOnDiskBytes() => mode == StorageMode.Lsm ? SqlMatrixRunner.DirectorySize(_directory!) : 0;

    public void Dispose() => CloseContext();

    private QueryResult Run(SqlMatrixQuery query, SqlMatrixParameter[] parameters)
    {
        if (!_prepared.TryGetValue(query.Sql, out DataVoPreparedStatement? statement))
        {
            statement = Ctx.Prepare(query.Sql);
            _prepared[query.Sql] = statement;
        }

        DataVoQueryParameter[] values = new DataVoQueryParameter[parameters.Length];
        for (int i = 0; i < parameters.Length; i++)
        {
            values[i] = DataVoQueryParameter.FromObject(parameters[i].Name, parameters[i].Value);
        }

        return Check(statement.Execute(values));
    }

    private DataVoContext Open() => new(new DataVoConfig
    {
        StorageMode = mode,
        DataDirectory = _directory!,
        LsmStrictFsync = false,
    });

    private void CloseContext()
    {
        _prepared.Clear();
        _context?.Dispose();
        _context = null;
    }

    private void InsertBatched(string table, ReactiveRowSchema schema, int count, int width, Action<CellValue[], int> fill)
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

    private void Ok(string sql) => Check(Ctx.Execute(sql)[^1]);

    private static QueryResult Check(QueryResult result) => result.IsError
        ? throw new InvalidOperationException($"DataVo failed: {string.Join("; ", result.Messages)}")
        : result;
}
