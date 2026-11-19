using System.Diagnostics;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Xunit.Abstractions;

namespace DataVo.Tests.Performance;

/// <summary>
/// A prepared aggregate over a 100-row range of a 100,000-row table's integer primary key
/// (<c>SELECT COUNT(*), SUM(Score) FROM Items WHERE Id &gt;= @lo AND Id &lt; @hi</c> through
/// <see cref="DataVoPreparedStatement.ExecuteBorrowed(DataVoQueryParameter, DataVoQueryParameter)"/>) must cost
/// in proportion to the range, not the table, and allocate no per-row objects. Allocation is asserted because it is
/// deterministic on a loaded machine; latency is reported only.
/// </summary>
public sealed class PrimaryKeyRangeAggregateAllocationTests
{
    private const int TableRows = 100_000;
    private const int RangeWidth = 100;
    private const int Iterations = 20_000;

    private readonly ITestOutputHelper _output;

    /// <summary>Creates the test with xUnit's output sink for the measured numbers.</summary>
    public PrimaryKeyRangeAggregateAllocationTests(ITestOutputHelper output) => _output = output;

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void PreparedRangeAggregate_AllocatesIndependentlyOfRowCount(StorageMode mode)
    {
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("pk_range_aggregate") : null;
        try
        {
            using var context = new DataVoContext(new DataVoConfig
            {
                StorageMode = mode,
                DiskStoragePath = directory ?? string.Empty,
                LsmStrictFsync = false,
            });
            Seed(context);

            DataVoPreparedStatement statement = context.Prepare(
                "SELECT COUNT(*), SUM(Score) FROM Items WHERE Id >= @lo AND Id < @hi");
            for (int i = 0; i < 100; i++)
            {
                Check(statement.ExecuteBorrowed(("lo", (long)(i * 7)), ("hi", (long)(i * 7 + RangeWidth))), i * 7);
            }

            (double micros, long bytes) = Measure(statement, context.SessionId);
            _output.WriteLine($"{mode}: {micros:F2} µs/op, {bytes:N0} B/op");

            // Inside a transaction the general path reads the snapshot: reported, not asserted.
            context.Execute("BEGIN TRANSACTION");
            context.Execute("INSERT INTO Items (Id, Score, Name) VALUES (-1, 1, 'n')");
            Check(statement.ExecuteBorrowed(("lo", 0L), ("hi", (long)RangeWidth)), 0);
            (double txMicros, long txBytes) = Measure(statement, context.SessionId, iterations: 10_000);
            context.Execute("ROLLBACK");
            _output.WriteLine($"{mode} in transaction: {txMicros:F2} µs/op, {txBytes:N0} B/op");

            // A per-row object (a row dictionary, a boxed cell, a RowKey list entry) costs well over 8 B per row.
            Assert.True(bytes < 2_048, $"{mode} range aggregate allocated {bytes:N0} B/op");
        }
        finally
        {
            if (directory is not null && Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Times the statement over sliding 100-row ranges after a warm-up long enough for tiered JIT to settle.</summary>
    private static (double MicrosPerOp, long BytesPerOp) Measure(DataVoPreparedStatement statement, Guid sessionId, int iterations = Iterations)
    {
        for (int i = 0; i < iterations; i++)
        {
            long lo = (i * 13L) % (TableRows - RangeWidth);
            statement.ExecuteBorrowed([("lo", lo), ("hi", lo + RangeWidth)], sessionId);
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        var watch = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            long lo = (i * 13L) % (TableRows - RangeWidth);
            statement.ExecuteBorrowed([("lo", lo), ("hi", lo + RangeWidth)], sessionId);
        }

        watch.Stop();
        return (watch.Elapsed.TotalMilliseconds * 1000 / iterations, (GC.GetAllocatedBytesForCurrentThread() - before) / iterations);
    }

    private static void Check(QueryResult result, long lo)
    {
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        Assert.Single(result.Data);
        long expectedSum = 0;
        for (long id = lo; id < lo + RangeWidth; id++)
        {
            expectedSum += id % 1_000;
        }

        object?[] cells = [.. result.Data[0].Values];
        Assert.Equal(RangeWidth, Convert.ToInt64(cells[0]));
        Assert.Equal(expectedSum, Convert.ToInt64(cells[1]));
    }

    private static void Seed(DataVoContext context)
    {
        context.Execute("CREATE DATABASE RangeDb");
        context.Execute("USE RangeDb");
        context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Score INT, Name VARCHAR(32))");
        var schema = new ReactiveRowSchema("Id", "Score", "Name");
        var rows = new List<CellValue[]>(TableRows);
        for (int i = 0; i < TableRows; i++)
        {
            rows.Add([CellValue.From(i), CellValue.From(i % 1_000), CellValue.From("n")]);
        }

        context.InsertTypedBatch("Items", schema, rows);
    }
}
