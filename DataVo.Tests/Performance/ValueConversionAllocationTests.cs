using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.ValueConversion;

namespace DataVo.Tests.Performance;

public sealed class ValueConversionAllocationTests
{
    [Fact]
    public void RegisteredVectorModelBinding_UsesOneOwnedWriteVector_AndDoesNotAllocateASecondReadVector()
    {
        using DataVoContext context = CreateContext();
        context.RegisterValueConverters(TestVectorConverter);
        context.Execute("CREATE TABLE Samples (Id INT PRIMARY KEY, Position VECTOR(2))");

        context.Insert("Samples", new Sample { Id = 1, Position = new TestVector(1f, 2f) });
        Dictionary<string, object?> row = Select(context, "SELECT Id, Position FROM Samples").Single();
        _ = context.Map<Sample>(row);
        _ = context.Get<TestVector>(row, "Position");

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long beforeWrite = GC.GetAllocatedBytesForCurrentThread();
        context.Insert("Samples", new Sample { Id = 2, Position = new TestVector(3f, 4f) });
        long writeAllocated = GC.GetAllocatedBytesForCurrentThread() - beforeWrite;

        TestVector sink = default;
        const int readCount = 2_000;
        long beforeRead = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < readCount; i++)
        {
            sink = context.Get<TestVector>(row, "Position");
        }

        long readAllocated = GC.GetAllocatedBytesForCurrentThread() - beforeRead;

        Assert.Equal(new TestVector(1f, 2f), sink);
        Assert.True(
            writeAllocated is > 0 and <= 8_192,
            $"Direct Insert<T> allocated {writeAllocated} bytes after warm-up. The bound includes its one owned VECTOR cell and normal typed-insert work; this is not a zero-allocation mapper claim.");
        Assert.True(
            readAllocated <= 1_024,
            $"{readCount} Get<T> calls allocated {readAllocated} bytes. This guard rejects a second managed VECTOR array per read; it is not a Unity player-performance claim.");
    }

    private static readonly DataVoValueConverter<TestVector> TestVectorConverter = new(
        "DataVo.Tests.ValueConversionAllocation.TestVector.v1",
        value => CellValue.FromVector([value.X, value.Y]),
        value => value is float[] vector && vector.Length == 2
            ? new TestVector(vector[0], vector[1])
            : throw new ArgumentException("Expected VECTOR(2).", nameof(value)));

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        string databaseName = $"ValueConversionAllocation_{Guid.NewGuid():N}";
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

    public sealed class Sample
    {
        public int Id { get; set; }
        public TestVector Position { get; set; }
    }
}
