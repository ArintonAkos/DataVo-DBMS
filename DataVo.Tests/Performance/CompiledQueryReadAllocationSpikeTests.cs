using System.Globalization;
using System.Runtime.CompilerServices;
using DataVo.Core;
using DataVo.Core.BTree;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Serialization;
using Xunit.Abstractions;

namespace DataVo.Tests.Performance;

public sealed class CompiledQueryReadAllocationSpikeTests
{
    private const int RowCount = 1_000;
    private const int Iterations = 20_000;
    private static readonly ReactiveRowSchema Schema = new("Id", "Name", "Value", "Score");
    private static readonly DataVoCompiledQueryPlan RuntimePlan = DataVoCompiledQueryPlan.SelectSingle(
        "Records",
        ["Id", "Name", "Value", "Score"],
        "Id",
        "id");
    private static readonly DataVoCompiledQueryPlan TaggedPrimaryKeyPlan = DataVoCompiledQueryPlan.SelectSingle(
        "Records",
        ["Id", "Name", "Value", "Score"],
        "Id",
        "id",
        CompiledAccessPath.SingleColumnIndex,
        "_PK_Records");

    private readonly ITestOutputHelper _output;

    public CompiledQueryReadAllocationSpikeTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public void Spike_PointLookupAllocationBreakdown()
    {
        using DataVoContext context = CreateContext();
        Seed(context);

        var cachedRows = new StoredRow[RowCount + 1];
        var rowIdsById = new List<long>[RowCount + 1];
        var keysById = new string[RowCount + 1];
        for (int id = 1; id <= RowCount; id++)
        {
            keysById[id] = BuildIdKey(id);
            IReadOnlyList<long> rowIds = context.Engine.IndexManager.FilterUsingIndex(
                keysById[id],
                "_PK_Records",
                "Records",
                "FlatCrudSpike");
            rowIdsById[id] = [rowIds[0]];
            cachedRows[id] = context.Engine.StorageContext.GetTypedTableContents(rowIdsById[id], "Records", "FlatCrudSpike")[rowIds[0]];
        }

        IReadOnlyList<Column> cachedColumns = context.Engine.Catalog.GetTableColumns("Records", "FlatCrudSpike");
        bool[] cachedIsProjected = [true, true, true, true];
        var cachedProjectedSchema = new ReactiveRowSchema("Id", "Name", "Value", "Score");
        var cachedProjectionBuffer = new CellValue[4];
        var runtimeParameterBuffer = new DataVoCompiledQueryParameter[1];
        var taggedParameterBuffer = new DataVoCompiledQueryParameter[1];
        var typedRuntimeParameterBuffer = new DataVoCompiledQueryParameter[1];
        var typedTaggedParameterBuffer = new DataVoCompiledQueryParameter[1];

        AllocationSample buildKeyOnly = Measure(id => BuildIdKey(id));
        AllocationSample indexOnly = Measure(id => context.Engine.IndexManager.FilterUsingIndex(
            keysById[id],
            "_PK_Records",
            "Records",
            "FlatCrudSpike"));
        AllocationSample storageOnly = Measure(id => context.Engine.StorageContext.GetTypedTableContents(
            rowIdsById[id],
            "Records",
            "FlatCrudSpike"));
        AllocationSample materializeOnly = Measure(id => Materialize(cachedRows[id]));
        AllocationSample legacyPostStorage = Measure(id =>
        {
            Dictionary<string, object?> row = Materialize(cachedRows[id]);
            Dictionary<string, object?> projected = Project(row);
            return MapDictionary(projected);
        });
        AllocationSample typedPostIndexFreshSetup = Measure(id => DecodeProjectedFreshSetup(context, rowIdsById[id][0]));
        AllocationSample typedPostIndexCachedSetup = Measure(id => DecodeProjectedCachedSetup(
            context,
            rowIdsById[id][0],
            cachedColumns,
            cachedIsProjected,
            cachedProjectedSchema,
            cachedProjectionBuffer));
        AllocationSample selectSingleRuntime = Measure(id => DataVoCompiledQuery.SelectSingle(
            context,
            RuntimePlan,
            [new DataVoCompiledQueryParameter("id", id)],
            MapDictionary));
        AllocationSample selectSingleTagged = Measure(id => DataVoCompiledQuery.SelectSingle(
            context,
            TaggedPrimaryKeyPlan,
            [new DataVoCompiledQueryParameter("id", id)],
            MapDictionary));
        AllocationSample selectSingleRuntimeReusedParameterArray = Measure(id =>
        {
            runtimeParameterBuffer[0] = new DataVoCompiledQueryParameter("id", id);
            return DataVoCompiledQuery.SelectSingle(context, RuntimePlan, runtimeParameterBuffer, MapDictionary);
        });
        AllocationSample selectSingleTaggedReusedParameterArray = Measure(id =>
        {
            taggedParameterBuffer[0] = new DataVoCompiledQueryParameter("id", id);
            return DataVoCompiledQuery.SelectSingle(context, TaggedPrimaryKeyPlan, taggedParameterBuffer, MapDictionary);
        });
        AllocationSample typedRuntime = Measure(id => DataVoCompiledQuery.SelectSingleTyped(
            context,
            RuntimePlan,
            [new DataVoCompiledQueryParameter("id", id)],
            MapTyped));
        AllocationSample typedTagged = Measure(id => DataVoCompiledQuery.SelectSingleTyped(
            context,
            TaggedPrimaryKeyPlan,
            [new DataVoCompiledQueryParameter("id", id)],
            MapTyped));
        AllocationSample typedRuntimeReusedParameterArray = Measure(id =>
        {
            typedRuntimeParameterBuffer[0] = new DataVoCompiledQueryParameter("id", id);
            return DataVoCompiledQuery.SelectSingleTyped(context, RuntimePlan, typedRuntimeParameterBuffer, MapTyped);
        });
        AllocationSample typedTaggedReusedParameterArray = Measure(id =>
        {
            typedTaggedParameterBuffer[0] = new DataVoCompiledQueryParameter("id", id);
            return DataVoCompiledQuery.SelectSingleTyped(context, TaggedPrimaryKeyPlan, typedTaggedParameterBuffer, MapTyped);
        });

        string report = string.Join(Environment.NewLine,
            "Compiled query read allocation spike",
            $"Rows: {RowCount}, Iterations: {Iterations}",
            Format("build comparison key only", buildKeyOnly),
            Format("index only", indexOnly),
            Format("storage only, precomputed row id", storageOnly),
            Format("legacy materialize cached StoredRow", materializeOnly),
            Format("legacy post-storage materialize+project+map", legacyPostStorage),
            Format("typed post-index fresh projection setup", typedPostIndexFreshSetup),
            Format("typed post-index cached projection setup", typedPostIndexCachedSetup),
            Format("SelectSingle runtime legacy dictionary", selectSingleRuntime),
            Format("SelectSingle tagged legacy dictionary", selectSingleTagged),
            Format("SelectSingle runtime reused parameter array", selectSingleRuntimeReusedParameterArray),
            Format("SelectSingle tagged reused parameter array", selectSingleTaggedReusedParameterArray),
            Format("SelectSingleTyped runtime", typedRuntime),
            Format("SelectSingleTyped tagged", typedTagged),
            Format("SelectSingleTyped runtime reused parameter array", typedRuntimeReusedParameterArray),
            Format("SelectSingleTyped tagged reused parameter array", typedTaggedReusedParameterArray));

        _output.WriteLine(report);
        Assert.True(typedPostIndexCachedSetup.BytesPerCall <= 1.0,
            $"Cached projected decode should be allocation-free.{Environment.NewLine}{report}");
        Assert.True(typedRuntime.BytesPerCall <= 192.0,
            $"Runtime typed point lookup allocation regressed.{Environment.NewLine}{report}");
        Assert.True(typedTaggedReusedParameterArray.BytesPerCall <= 128.0,
            $"Tagged typed point lookup with reused parameters allocation regressed.{Environment.NewLine}{report}");
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

    private static string Format(string label, AllocationSample sample) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{label,-46} {sample.TotalBytes,12:N0} B total {sample.BytesPerCall,10:N1} B/call");

    private static string BuildIdKey(int id) =>
        IndexKeyEncoder.BuildKeyString(
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["Id"] = id
            },
            ["Id"]);

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk(context, "CREATE DATABASE FlatCrudSpike");
        ExecuteOk(context, "USE FlatCrudSpike");
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

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static Dictionary<string, object?> Materialize(StoredRow row)
    {
        StoredRowView view = row.AsView();
        var result = new Dictionary<string, object?>(view.Count, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < view.Count; i++)
        {
            result[view.Schema.ColumnAt(i)] = view[i].ToObject();
        }

        return result;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static Dictionary<string, object?> Project(Dictionary<string, object?> row)
    {
        var projected = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        projected["Id"] = row.TryGetValue("Id", out object? id) ? id : null;
        projected["Name"] = row.TryGetValue("Name", out object? name) ? name : null;
        projected["Value"] = row.TryGetValue("Value", out object? value) ? value : null;
        projected["Score"] = row.TryGetValue("Score", out object? score) ? score : null;
        return projected;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static FlatRecord? DecodeProjectedFreshSetup(DataVoContext context, long rowId)
    {
        IReadOnlyList<Column> columns = context.Engine.Catalog.GetTableColumns("Records", "FlatCrudSpike");
        bool[] isProjected = new bool[columns.Count];
        var projectedNames = new List<string>(RuntimePlan.ProjectedColumns.Count);
        for (int i = 0; i < columns.Count; i++)
        {
            if (ContainsIgnoreCase(RuntimePlan.ProjectedColumns, columns[i].Name))
            {
                isProjected[i] = true;
                projectedNames.Add(columns[i].Name);
            }
        }

        var projectedSchema = new ReactiveRowSchema(projectedNames);
        var buffer = new CellValue[projectedNames.Count];
        return DecodeProjectedCachedSetup(context, rowId, columns, isProjected, projectedSchema, buffer);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static FlatRecord? DecodeProjectedCachedSetup(
        DataVoContext context,
        long rowId,
        IReadOnlyList<Column> columns,
        bool[] isProjected,
        ReactiveRowSchema projectedSchema,
        CellValue[] buffer)
    {
        if (!context.Engine.StorageContext.IsRowVisible("Records", "FlatCrudSpike", rowId))
        {
            return null;
        }

        byte[]? bytes = context.Engine.StorageContext.TryReadRowBytes("Records", "FlatCrudSpike", rowId);
        if (bytes is null)
        {
            return null;
        }

        RowSerializer.DecodeProjectedCells(bytes, columns, isProjected, buffer);
        return MapTyped(new CompiledRowReader(new StoredRowView(projectedSchema, buffer)));
    }

    private static bool ContainsIgnoreCase(IReadOnlyList<string> values, string candidate)
    {
        for (int i = 0; i < values.Count; i++)
        {
            if (string.Equals(values[i], candidate, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static FlatRecord MapDictionary(Dictionary<string, object?> row) => new(
        Convert.ToInt64(row["Id"], CultureInfo.InvariantCulture),
        Convert.ToString(row["Name"], CultureInfo.InvariantCulture) ?? string.Empty,
        Convert.ToInt32(row["Value"], CultureInfo.InvariantCulture),
        Convert.ToDouble(row["Score"], CultureInfo.InvariantCulture));

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static FlatRecord MapTyped(CompiledRowReader row) => new(
        row.GetInt32("Id"),
        row.GetString("Name") ?? string.Empty,
        row.GetInt32("Value"),
        row.GetDouble("Score"));

    private sealed record FlatRecord(long Id, string Name, int Value, double Score);

    private readonly record struct AllocationSample(long TotalBytes, double BytesPerCall);
}
