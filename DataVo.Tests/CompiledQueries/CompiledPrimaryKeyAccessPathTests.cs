using System.Numerics;
using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.CompiledQueries;

/// <summary>
/// The compile-time <see cref="CompiledAccessPath.PrimaryKey"/> access path (emitted by the query generator for a
/// manifest primary key) and the <see cref="CompiledRowReader"/> getters added for float, short, byte, Vector3 and
/// ReadOnlyMemory&lt;float&gt; projections.
/// </summary>
public sealed class CompiledPrimaryKeyAccessPathTests
{
    public static TheoryData<StorageMode> Modes => new() { StorageMode.InMemory, StorageMode.Lsm };

    [Theory]
    [MemberData(nameof(Modes))]
    public void PrimaryKeyAccessPath_FindsRowsLikeRuntimeResolution(StorageMode mode)
    {
        string? directory = mode == StorageMode.Lsm ? Path.Combine(Path.GetTempPath(), $"datavo_pkpath_{Guid.NewGuid():N}") : null;
        try
        {
            using var context = CreateContext(mode, directory);
            context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(20))");
            context.Execute("INSERT INTO Items VALUES (1, 'one'), (2, 'two')");

            DataVoCompiledQueryPlan tagged = DataVoCompiledQueryPlan.SelectSingle("Items", ["Id", "Name"], "Id", "id", accessPath: CompiledAccessPath.PrimaryKey);
            Assert.Equal(CompiledAccessPath.PrimaryKey, tagged.AccessPath);
            Assert.Equal("_PK_Items", tagged.ResolvedIndexName);

            Assert.Equal("two", Name(context, tagged, 2));
            Assert.Null(Name(context, tagged, 99));

            // Declared with other casing: the plan follows the stored table name, and its index name with it.
            DataVoCompiledQueryPlan lower = DataVoCompiledQueryPlan.SelectMany("items", ["Id", "Name"], "Id", "id", accessPath: CompiledAccessPath.PrimaryKey);
            IReadOnlyList<string?> names = DataVoCompiledQuery.SelectManyTyped<string?>(
                context, lower, [new DataVoCompiledQueryParameter("id", 1)], static r => r.GetString("Name"));
            Assert.Equal(["one"], names);
        }
        finally
        {
            if (directory is not null && Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Fact]
    public void PrimaryKeyAccessPath_OnANonKeyColumn_FallsBackSafely()
    {
        using var context = CreateContext(StorageMode.InMemory, null);
        context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(20))");
        context.Execute("INSERT INTO Items VALUES (1, 'one'), (2, 'two')");

        // A stale manifest claimed Name was the key: the runtime must still answer correctly.
        DataVoCompiledQueryPlan wrong = DataVoCompiledQueryPlan.SelectSingle("Items", ["Id", "Name"], "Name", "name", accessPath: CompiledAccessPath.PrimaryKey);
        int? id = DataVoCompiledQuery.SelectSingleTyped<int?>(context, wrong, [new DataVoCompiledQueryParameter("name", "two")], static r => r.GetInt32("Id"));

        Assert.Equal(2, id);
    }

    [Fact]
    public void RowReader_ReadsFloatShortByteVector3AndMemory()
    {
        using var context = CreateContext(StorageMode.InMemory, null);
        context.Execute("CREATE TABLE Samples (Id INT PRIMARY KEY, Ratio FLOAT, Small INT, Tiny INT, Position VECTOR(3), Embedding VECTOR(2), Maybe FLOAT)");
        context.BulkInsert(
            "Samples",
            [
                new Dictionary<string, object?>
                {
                    ["Id"] = 1, ["Ratio"] = 0.25, ["Small"] = -12, ["Tiny"] = 250,
                    ["Position"] = new float[] { 1, 2, 3 }, ["Embedding"] = new float[] { 0.5f, 1.5f }, ["Maybe"] = null,
                },
            ]);

        var row = DataVoCompiledQuery.SelectSingleTyped<(float, short, byte, Vector3, float[], float?, Vector3?)>(
            context,
            DataVoCompiledQueryPlan.SelectSingle("Samples", ["Id", "Ratio", "Small", "Tiny", "Position", "Embedding", "Maybe"], "Id", "id"),
            [new DataVoCompiledQueryParameter("id", 1)],
            static r => (r.GetSingle("Ratio"), r.GetInt16("Small"), r.GetByte("Tiny"), r.GetVector3("Position"),
                r.GetVectorMemory("Embedding").ToArray(), r.GetSingleOrNull("Maybe"), r.GetVector3OrNull("Position")));

        Assert.Equal((0.25f, (short)-12, (byte)250, new Vector3(1, 2, 3)), (row.Item1, row.Item2, row.Item3, row.Item4));
        Assert.Equal([0.5f, 1.5f], row.Item5);
        Assert.Null(row.Item6);
        Assert.Equal(new Vector3(1, 2, 3), row.Item7);
    }

    private static string? Name(DataVoContext context, DataVoCompiledQueryPlan plan, int id) =>
        DataVoCompiledQuery.SelectSingleTyped<string?>(context, plan, [new DataVoCompiledQueryParameter("id", id)], static r => r.GetString("Name"));

    private static DataVoContext CreateContext(StorageMode mode, string? directory)
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
        string databaseName = $"PkPath_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }
}
