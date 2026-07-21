using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.ValueConversion;

namespace DataVo.Tests.E2E;

public sealed class ValueConversionRegistryTests
{
    private readonly record struct TestVector(float X, float Y);

    private readonly record struct UnregisteredVector(float X, float Y);

    [Fact]
    public void Get_UsesRegisteredConverterWithoutExposingFloatArray()
    {
        using var context = NewContext();
        context.RegisterValueConverters(CreateTestVectorConverter("DataVo.Tests.TestVector.v1"));
        var row = new Dictionary<string, object?> { ["Position"] = new[] { 1.25f, -2f } };

        Assert.Equal(new TestVector(1.25f, -2f), context.Get<TestVector>(row, "Position"));
    }

    [Fact]
    public void Get_ReturnsCompatibleBuiltInValueDirectly()
    {
        using var context = NewContext();
        var row = new Dictionary<string, object?> { ["Id"] = 7 };

        Assert.Equal(7, context.Get<int>(row, "Id"));
    }

    [Fact]
    public void Get_WhenColumnIsMissing_ThrowsKeyNotFoundException()
    {
        using var context = NewContext();

        KeyNotFoundException exception = Assert.Throws<KeyNotFoundException>(() =>
            context.Get<int>(new Dictionary<string, object?>(), "Missing"));

        Assert.Contains("Missing", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Get_WhenCustomTargetIsUnregistered_ThrowsNotSupportedException()
    {
        using var context = NewContext();
        var row = new Dictionary<string, object?> { ["Position"] = new[] { 1f, 2f } };

        NotSupportedException exception = Assert.Throws<NotSupportedException>(() =>
            context.Get<UnregisteredVector>(row, "Position"));

        Assert.Contains(nameof(UnregisteredVector), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RegisterValueConverters_DoesNotLeakAcrossContexts()
    {
        using var configured = NewContext();
        using var isolated = NewContext();
        configured.RegisterValueConverters(CreateTestVectorConverter("DataVo.Tests.TestVector.v1"));
        var row = new Dictionary<string, object?> { ["Position"] = new[] { 1f, 2f } };

        Assert.Equal(new TestVector(1f, 2f), configured.Get<TestVector>(row, "Position"));
        Assert.Throws<NotSupportedException>(() => isolated.Get<TestVector>(row, "Position"));
    }

    [Fact]
    public void RegisterValueConverters_WhenIncomingGroupHasDuplicateType_RejectsAtomically()
    {
        using var context = NewContext();
        var first = CreateTestVectorConverter("DataVo.Tests.TestVector.v1");
        var second = CreateTestVectorConverter("DataVo.Tests.TestVector.v2");

        Assert.Throws<ArgumentException>(() => context.RegisterValueConverters(first, second));

        Assert.Throws<NotSupportedException>(() => context.Get<TestVector>(
            new Dictionary<string, object?> { ["Position"] = new[] { 1f, 2f } },
            "Position"));
    }

    [Fact]
    public void RegisterValueConverters_WhenRegistrationKeyMatches_IsIdempotent()
    {
        using var context = NewContext();
        context.RegisterValueConverters(CreateTestVectorConverter("DataVo.Tests.TestVector.v1"));
        context.RegisterValueConverters(CreateTestVectorConverter("DataVo.Tests.TestVector.v1"));

        Assert.Equal(new TestVector(1f, 2f), context.Get<TestVector>(
            new Dictionary<string, object?> { ["Position"] = new[] { 1f, 2f } },
            "Position"));
    }

    private static DataVoValueConverter<TestVector> CreateTestVectorConverter(string registrationKey) => new(
        registrationKey,
        value => CellValue.FromVector([value.X, value.Y]),
        value => value is float[] vector && vector.Length == 2
            ? new TestVector(vector[0], vector[1])
            : throw new ArgumentException("Expected VECTOR(2).", nameof(value)));

    private static DataVoContext NewContext() => new(new DataVoConfig { StorageMode = StorageMode.InMemory });
}
