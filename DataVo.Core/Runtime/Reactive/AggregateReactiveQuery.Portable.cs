#if NETSTANDARD2_1
using System.Globalization;

namespace DataVo.Core.Runtime.Reactive;

internal sealed partial class AggregateReactiveQuery
{
    private void AppendValue(ref int pos, object value)
    {
        switch (value)
        {
            case string text:
                AppendChars(ref pos, text);
                return;

            case bool boolean:
                AppendChars(ref pos, boolean ? "True" : "False");
                return;

            case IFormattable formattable:
                AppendChars(ref pos, formattable.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty);
                return;

            default:
                AppendChars(ref pos, Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
                return;
        }
    }

    private bool TryGetGroup(ReadOnlySpan<char> keySpan, out string? key, out GroupState? state)
    {
        key = new string(keySpan);
        if (_groups.TryGetValue(key, out GroupState? found))
        {
            state = found;
            return true;
        }

        state = null;
        return false;
    }

    private void AppendFormattable<T>(ref int pos, T value)
        where T : IFormattable =>
        AppendChars(ref pos, value.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty);
}
#endif
