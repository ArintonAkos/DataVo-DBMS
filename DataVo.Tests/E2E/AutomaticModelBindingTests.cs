using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.ValueConversion;

namespace DataVo.Tests.E2E;

public sealed class AutomaticModelBindingTests
{
    [Fact]
    public void Map_ConvertsRegisteredVectorAndIgnoresPropertiesMissingFromProjection()
    {
        using var context = CreateContext();
        context.RegisterValueConverters(CreateTestVectorConverter());

        TestTransformRow transform = context.Map<TestTransformRow>(new Dictionary<string, object?>
        {
            ["Id"] = 7,
            ["Position"] = new[] { 1.25f, -2f },
            ["Label"] = "spawn"
        });

        Assert.Equal(7, transform.Id);
        Assert.Equal(new TestVector(1.25f, -2f), transform.Position);
        Assert.Equal("spawn", transform.Label);

        ProjectionRow projection = context.Map<ProjectionRow>(new Dictionary<string, object?>
        {
            ["Position"] = new[] { 3f, 4f }
        });

        Assert.Equal(new TestVector(3f, 4f), projection.Position);
        Assert.Equal(0, projection.NotSelected);
    }

    [Fact]
    public void Map_PropagatesConverterDimensionFailures()
    {
        using var context = CreateContext();
        context.RegisterValueConverters(CreateTestVectorConverter());

        ArgumentException exception = Assert.Throws<ArgumentException>(() => context.Map<TestTransformRow>(
            new Dictionary<string, object?>
            {
                ["Id"] = 1,
                ["Position"] = new[] { 1f, 2f, 3f },
                ["Label"] = "wrong-dimension"
            }));

        Assert.Contains("VECTOR(2)", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Insert_WritesAllCatalogColumnsThroughTheRegisteredConverter()
    {
        using var context = CreateContext();
        context.RegisterValueConverters(CreateTestVectorConverter());
        context.Execute("CREATE TABLE Transforms (Id INT PRIMARY KEY, Position VECTOR(2), Label VARCHAR(20))");

        RowKey rowId = context.Insert("Transforms", new TestTransformRow
        {
            Id = 1,
            Position = new TestVector(0.5f, -8f),
            Label = "checkpoint"
        });

        Assert.Equal(1L, rowId);
        Dictionary<string, object?> row = Select(context, "SELECT Id, Position, Label FROM Transforms").Single();
        TestTransformRow materialized = context.Map<TestTransformRow>(row);
        Assert.Equal(1, materialized.Id);
        Assert.Equal(new TestVector(0.5f, -8f), materialized.Position);
        Assert.Equal("checkpoint", materialized.Label);
    }

    [Fact]
    public void Insert_WhenAModelPropertyForACatalogColumnIsMissing_ThrowsBeforeWriting()
    {
        using var context = CreateContext();
        context.RegisterValueConverters(CreateTestVectorConverter());
        context.Execute("CREATE TABLE Transforms (Id INT PRIMARY KEY, Position VECTOR(2), Label VARCHAR(20))");

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() =>
            context.Insert("Transforms", new MissingLabelRow { Id = 1, Position = new TestVector(1f, 2f) }));

        Assert.Contains("Label", exception.Message, StringComparison.Ordinal);
        Assert.Empty(Select(context, "SELECT Id FROM Transforms"));
    }

    [Fact]
    public void BulkInsert_ConvertsRegisteredVectorWithoutMakingCallersUseCells()
    {
        using var context = CreateContext();
        context.RegisterValueConverters(CreateTestVectorConverter());
        context.Execute("CREATE TABLE Samples (Id INT PRIMARY KEY, Position VECTOR(2))");
        context.Execute("CREATE INDEX idx_samples_position ON Samples (Position) USING HNSW");

        IReadOnlyList<RowKey> rowIds = context.BulkInsert("Samples",
        [
            new Dictionary<string, object?> { ["Id"] = 1, ["Position"] = new TestVector(6f, -3f) }
        ]);

        Assert.Equal([1L], rowIds);
        Dictionary<string, object?> row = Select(context, "SELECT Position FROM Samples").Single();
        Assert.Equal(new TestVector(6f, -3f), context.Get<TestVector>(row, "Position"));
        Assert.Equal(1, context.SearchNearest("Samples", "idx_samples_position", [6f, -3f], topK: 1).Single()["Id"]);
    }

    private static DataVoValueConverter<TestVector> CreateTestVectorConverter() => new(
        "DataVo.Tests.AutomaticModelBinding.TestVector.v1",
        value => CellValue.FromVector([value.X, value.Y]),
        value => value is float[] vector && vector.Length == 2
            ? new TestVector(vector[0], vector[1])
            : throw new ArgumentException("Expected VECTOR(2).", nameof(value)));

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string databaseName = $"AutomaticModelBinding_{Guid.NewGuid():N}";
        context.Execute($"CREATE DATABASE {databaseName}");
        context.Execute($"USE {databaseName}");
        return context;
    }

    private static List<Dictionary<string, object?>> Select(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Single();
        Assert.False(result.IsError, string.Join(" | ", result.Messages));
        return result.Data;
    }

    public readonly record struct TestVector(float X, float Y);

    public sealed class TestTransformRow
    {
        public int Id { get; set; }
        public TestVector Position { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    public sealed class ProjectionRow
    {
        public TestVector Position { get; set; }
        public int NotSelected { get; set; }
    }

    public sealed class MissingLabelRow
    {
        public int Id { get; set; }
        public TestVector Position { get; set; }
    }
}
