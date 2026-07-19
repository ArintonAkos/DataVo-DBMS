#if NET10_0_OR_GREATER
namespace DataVo.Core.Compat;

internal static class DateObjectAdapter
{
    internal static bool TryConvert(object value, out DataVoDate converted)
    {
        if (value is DateOnly dateOnly)
        {
            converted = DataVoDate.FromDateOnly(dateOnly);
            return true;
        }

        converted = default;
        return false;
    }
}
#endif
