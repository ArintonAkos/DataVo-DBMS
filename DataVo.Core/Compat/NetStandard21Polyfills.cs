#if NETSTANDARD2_1
#pragma warning disable CS1591
using System.Globalization;

namespace System.Runtime.CompilerServices
{
    internal static class IsExternalInit;

    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    internal sealed class RequiredMemberAttribute : Attribute;

    [AttributeUsage(AttributeTargets.Constructor, AllowMultiple = false, Inherited = false)]
    internal sealed class SetsRequiredMembersAttribute : Attribute;

    [AttributeUsage(AttributeTargets.All, AllowMultiple = true, Inherited = false)]
    internal sealed class CompilerFeatureRequiredAttribute : Attribute
    {
        public CompilerFeatureRequiredAttribute(string featureName) => FeatureName = featureName;

        public string FeatureName { get; }

        public bool IsOptional { get; init; }
    }
}

namespace System
{
    public readonly struct TimeOnly : IEquatable<TimeOnly>, IComparable<TimeOnly>
    {
        internal readonly long Ticks;

        private TimeOnly(long ticks) => Ticks = ticks;

        public static TimeOnly MinValue { get; } = new(0L);

        public int CompareTo(TimeOnly other) => Ticks.CompareTo(other.Ticks);

        public bool Equals(TimeOnly other) => Ticks == other.Ticks;

        public override bool Equals(object? obj) => obj is TimeOnly other && Equals(other);

        public override int GetHashCode() => Ticks.GetHashCode();
    }

    public readonly struct DateOnly : IEquatable<DateOnly>, IComparable<DateOnly>, IFormattable
    {
        private static readonly DateTime Epoch = new(1, 1, 1);
        private readonly DateTime _date;

        private DateOnly(DateTime date) => _date = date.Date;

        public int DayNumber => (int)(_date - Epoch).TotalDays;

        public static DateOnly FromDayNumber(int dayNumber) => new(Epoch.AddDays(dayNumber));

        public static DateOnly FromDateTime(DateTime dateTime) => new(dateTime);

        public static bool TryParse(string? input, IFormatProvider? provider, DateTimeStyles style, out DateOnly result)
        {
            if (DateTime.TryParse(input, provider, style, out DateTime parsed))
            {
                result = new DateOnly(parsed);
                return true;
            }

            result = default;
            return false;
        }

        public static bool TryParse(string? input, out DateOnly result) =>
            TryParse(input, CultureInfo.CurrentCulture, DateTimeStyles.None, out result);

        public DateTime ToDateTime(TimeOnly time) => _date.Date.AddTicks(time.Ticks);

        public override string ToString() => _date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        public string ToString(string? format, IFormatProvider? formatProvider) =>
            _date.ToString(format, formatProvider);

        public int CompareTo(DateOnly other) => _date.CompareTo(other._date);

        public bool Equals(DateOnly other) => _date.Equals(other._date);

        public override bool Equals(object? obj) => obj is DateOnly other && Equals(other);

        public override int GetHashCode() => _date.GetHashCode();

        public static bool operator ==(DateOnly left, DateOnly right) => left.Equals(right);

        public static bool operator !=(DateOnly left, DateOnly right) => !left.Equals(right);
    }
}
#pragma warning restore CS1591
#endif
