#if !NET8_0_OR_GREATER
namespace DataVo.Core.Compat;

internal static class DateObjectAdapter
{
    internal static bool TryConvert(object value, out DataVoDate converted)
    {
        converted = default;
        return false;
    }
}
#endif
