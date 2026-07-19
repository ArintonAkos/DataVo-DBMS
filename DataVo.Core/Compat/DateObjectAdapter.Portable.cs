#if NETSTANDARD2_1
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
