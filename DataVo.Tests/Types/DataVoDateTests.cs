using System.Globalization;
using DataVo.Core;

namespace DataVo.Tests.Types;

public class DataVoDateTests
{
    [Theory]
    [InlineData(0, 1, 1, 1)]
    [InlineData(3652058, 9999, 12, 31)]
    public void FromDayNumber_AcceptsSupportedBounds(
        int dayNumber,
        int year,
        int month,
        int day)
    {
        DataVoDate value = DataVoDate.FromDayNumber(dayNumber);

        Assert.Equal(dayNumber, value.DayNumber);
        Assert.Equal(new DateTime(year, month, day), value.ToDateTime());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(3652059)]
    public void FromDayNumber_RejectsValuesOutsideSupportedRange(int dayNumber)
    {
        ArgumentOutOfRangeException error = Assert.Throws<ArgumentOutOfRangeException>(
            () => DataVoDate.FromDayNumber(dayNumber));

        Assert.Equal(nameof(dayNumber), error.ParamName);
    }

    [Fact]
    public void FromDateTime_NormalizesToTheDateAtMidnight()
    {
        var input = new DateTime(2026, 7, 19, 23, 58, 57, DateTimeKind.Utc);

        DataVoDate value = DataVoDate.FromDateTime(input);

        Assert.Equal(new DateTime(2026, 7, 19), value.ToDateTime());
        Assert.Equal(DateTimeKind.Unspecified, value.ToDateTime().Kind);
    }

    [Fact]
    public void EqualityAndHashCode_AreBasedOnlyOnDayNumber()
    {
        DataVoDate first = DataVoDate.FromDayNumber(123456);
        DataVoDate same = DataVoDate.FromDayNumber(123456);
        DataVoDate different = DataVoDate.FromDayNumber(123457);

        Assert.True(first.Equals(same));
        Assert.True(first.Equals((object)same));
        Assert.False(first.Equals(different));
        Assert.False(first.Equals(null));
        Assert.True(first == same);
        Assert.True(first != different);
        Assert.Equal(first.DayNumber.GetHashCode(), first.GetHashCode());
        Assert.Equal(first.GetHashCode(), same.GetHashCode());
    }

    [Fact]
    public void Comparison_UsesDayNumberAcrossTypedUntypedAndOperatorContracts()
    {
        DataVoDate earlier = DataVoDate.FromDayNumber(10);
        DataVoDate same = DataVoDate.FromDayNumber(10);
        DataVoDate later = DataVoDate.FromDayNumber(11);

        Assert.True(earlier.CompareTo(later) < 0);
        Assert.Equal(0, earlier.CompareTo(same));
        Assert.True(later.CompareTo(earlier) > 0);
        Assert.True(earlier < later);
        Assert.True(earlier <= same);
        Assert.True(later > earlier);
        Assert.True(later >= same);

        IComparable comparable = earlier;
        Assert.True(comparable.CompareTo(null) > 0);
        Assert.Equal(0, comparable.CompareTo(same));
        ArgumentException error = Assert.Throws<ArgumentException>(() => comparable.CompareTo("10"));
        Assert.Equal("obj", error.ParamName);
    }

    [Fact]
    public void DefaultAndRoundTripFormatting_AreInvariantUnderAnotherCurrentCulture()
    {
        CultureInfo originalCulture = CultureInfo.CurrentCulture;
        CultureInfo french = CultureInfo.GetCultureInfo("fr-FR");

        try
        {
            CultureInfo.CurrentCulture = french;
            DataVoDate value = DataVoDate.FromDateTime(new DateTime(2026, 7, 9, 17, 30, 0));

            Assert.Equal("2026-07-09", value.ToString());
            Assert.Equal("2026-07-09", value.ToString(null, french));
            Assert.Equal("2026-07-09", value.ToString("O", french));
            Assert.Equal("2026-07-09", value.ToString("yyyy-MM-dd", french));
            Assert.Equal(
                new DateTime(2026, 7, 9).ToString("D", french),
                value.ToString("D", french));
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
        }
    }

    [Fact]
    public void ParseAndTryParse_NormalizeDatesAndReportInvalidInput()
    {
        DataVoDate parsed = DataVoDate.Parse("2026-07-09T22:15:00");

        Assert.Equal(DataVoDate.FromDateTime(new DateTime(2026, 7, 9)), parsed);
        Assert.True(DataVoDate.TryParse("2026-07-10", out DataVoDate next));
        Assert.Equal(DataVoDate.FromDateTime(new DateTime(2026, 7, 10)), next);
        Assert.False(DataVoDate.TryParse("not-a-date", out DataVoDate invalid));
        Assert.Equal(default, invalid);
        Assert.False(DataVoDate.TryParse(null, out invalid));
        Assert.Equal(default, invalid);
    }

    [Fact]
    public void TryParse_WithProviderAndStyles_UsesBothArguments()
    {
        CultureInfo french = CultureInfo.GetCultureInfo("fr-FR");

        bool parsed = DataVoDate.TryParse(
            " 31/12/2026 ",
            french,
            DateTimeStyles.AllowWhiteSpaces,
            out DataVoDate result);

        Assert.True(parsed);
        Assert.Equal(DataVoDate.FromDateTime(new DateTime(2026, 12, 31)), result);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(739815)]
    [InlineData(3652058)]
    public void DateOnlyAdapters_RoundTripExplicitly(int dayNumber)
    {
        DateOnly original = DateOnly.FromDayNumber(dayNumber);

        DataVoDate value = DataVoDate.FromDateOnly(original);

        Assert.Equal(dayNumber, value.DayNumber);
        Assert.Equal(original, value.ToDateOnly());
    }
}
