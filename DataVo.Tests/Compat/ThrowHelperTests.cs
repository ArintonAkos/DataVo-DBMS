using DataVo.Core.Compat;

namespace DataVo.Tests.Compat;

public sealed class ThrowHelperTests
{
    [Fact]
    public void ThrowIfNull_UsesCallerArgumentName()
    {
        object? value = null;

        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => ThrowHelper.ThrowIfNull(value));

        Assert.Equal(nameof(value), error.ParamName);
    }

    [Fact]
    public void ThrowIfNullOrWhiteSpace_Null_ThrowsArgumentNullExceptionWithCallerName()
    {
        string? path = null;

        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => ThrowHelper.ThrowIfNullOrWhiteSpace(path));

        Assert.Equal(nameof(path), error.ParamName);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t\r\n")]
    public void ThrowIfNullOrWhiteSpace_EmptyOrWhitespace_ThrowsArgumentException(string input)
    {
        string path = input;

        ArgumentException error = Assert.Throws<ArgumentException>(
            () => ThrowHelper.ThrowIfNullOrWhiteSpace(path));

        Assert.Equal(nameof(path), error.ParamName);
    }

    [Fact]
    public void ThrowIfNegative_ReportsCallerArgumentAndActualValue()
    {
        int rowId = -17;

        ArgumentOutOfRangeException error = Assert.Throws<ArgumentOutOfRangeException>(
            () => ThrowHelper.ThrowIfNegative(rowId));

        Assert.Equal(nameof(rowId), error.ParamName);
        Assert.Equal(rowId, error.ActualValue);
    }

    [Fact]
    public void ThrowIfNegative_LongValueReportsCallerArgumentAndActualValue()
    {
        long rowId = -4_294_967_296L;

        ArgumentOutOfRangeException error = Assert.Throws<ArgumentOutOfRangeException>(
            () => ThrowHelper.ThrowIfNegative(rowId));

        Assert.Equal(nameof(rowId), error.ParamName);
        Assert.Equal(rowId, error.ActualValue);
    }
}
