using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using Xunit.Abstractions;

namespace DataVo.Tests.Performance;

public sealed class CompiledQueryReadAllocationGuardTests
{
    private const int RowCount = 1_000;
    private const int Iterations = 20_000;
    private const long PreparedPointLookupCeilingBytes = 160;
    private const long SelectSingleTypedPointLookupCeilingBytes = 192;

    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Value", "Score");
    private static readonly DataVoCompiledQueryPlan TaggedPrimaryKeyPlan = DataVoCompiledQueryPlan.SelectSingle(
        "Records",
        ["Id", "Name", "Value", "Score"],
        "Id",
        "id",
        CompiledAccessPath.SingleColumnIndex,
        "_PK_Records");

    private readonly ITestOutputHelper _output;

    public CompiledQueryReadAllocationGuardTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public void PreparedSelectSingleTyped_WarmPointLookup_StaysNearMaterializationFloor()
    {
        using DataVoContext context = CreateContext();
        Seed(context);

        DataVoPreparedSelectSingle<Hit> prepared =
            DataVoCompiledQuery.PrepareSelectSingleTyped(context, TaggedPrimaryKeyPlan, MapHit);

        AllocationSample sample = Measure(id => prepared.Execute(id));
        _output.WriteLine($"PreparedSelectSingleTyped warm point lookup: {sample.BytesPerCall:N1} B/call");

        Assert.True(sample.BytesPerCall <= PreparedPointLookupCeilingBytes,
            $"prepared point lookup {sample.BytesPerCall:N1} B/call exceeds {PreparedPointLookupCeilingBytes} B/call");
    }

    [Fact]
    public void SelectSingleTyped_TaggedWarmPointLookup_UsesPreparedProjectionCache()
    {
        using DataVoContext context = CreateContext();
        Seed(context);

        var parameters = new DataVoCompiledQueryParameter[1];
        AllocationSample sample = Measure(id =>
        {
            parameters[0] = new DataVoCompiledQueryParameter("id", id);
            return DataVoCompiledQuery.SelectSingleTyped(context, TaggedPrimaryKeyPlan, parameters, MapHit);
        });
        _output.WriteLine($"SelectSingleTyped tagged warm point lookup: {sample.BytesPerCall:N1} B/call");

        Assert.True(sample.BytesPerCall <= SelectSingleTypedPointLookupCeilingBytes,
            $"SelectSingleTyped point lookup {sample.BytesPerCall:N1} B/call exceeds {SelectSingleTypedPointLookupCeilingBytes} B/call");
    }

    private static AllocationSample Measure(Func<int, object?> action)
    {
        object? sink = null;
        for (int i = 0; i < 1_000; i++)
        {
            sink = action((i % RowCount) + 1);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < Iterations; i++)
        {
            sink = action((i % RowCount) + 1);
        }

        long bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        GC.KeepAlive(sink);
        return new AllocationSample(bytes, bytes / (double)Iterations);
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk(context, "CREATE DATABASE CompiledReadGuard");
        ExecuteOk(context, "USE CompiledReadGuard");
        ExecuteOk(context, "CREATE TABLE Records (Id INT PRIMARY KEY, Name VARCHAR(40), Value INT, Score FLOAT)");
        return context;
    }

    private static void Seed(DataVoContext context)
    {
        var cells = new CellValue[4];
        for (int i = 1; i <= RowCount; i++)
        {
            cells[0] = CellValue.From(i);
            cells[1] = CellValue.From($"Name {i}");
            cells[2] = CellValue.From(i * 3);
            cells[3] = CellValue.From(i * 0.25d);
            context.InsertTyped("Records", Schema, cells);
        }
    }

    private static void ExecuteOk(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }

    private static Hit MapHit(CompiledRowReader row) => new(
        row.GetInt32("Id"),
        row.GetString("Name") ?? string.Empty,
        row.GetInt32("Value"),
        row.GetDouble("Score"));

    private sealed record Hit(int Id, string Name, int Value, double Score);

    private readonly record struct AllocationSample(long TotalBytes, double BytesPerCall);
}
