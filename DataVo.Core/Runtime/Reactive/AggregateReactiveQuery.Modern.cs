#if NET10_0_OR_GREATER
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

            // bool is not ISpanFormattable; preserve the legacy invariant rendering.
            case bool boolean:
                AppendChars(ref pos, boolean ? "True" : "False");
                return;

            case ISpanFormattable formattable:
            {
                int written;
                while (!formattable.TryFormat(
                           _keyBuffer.AsSpan(pos),
                           out written,
                           default,
                           CultureInfo.InvariantCulture))
                {
                    EnsureKeyCapacity(Math.Max(_keyBuffer.Length * 2, pos + 64));
                }

                pos += written;
                return;
            }

            // Exotic types are off the common group-key path.
            default:
                AppendChars(ref pos, Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
                return;
        }
    }

    private bool TryGetGroup(ReadOnlySpan<char> keySpan, out string? key, out GroupState? state) =>
        _groups.GetAlternateLookup<ReadOnlySpan<char>>().TryGetValue(keySpan, out key, out state);

    private void AppendFormattable<T>(ref int pos, T value)
        where T : ISpanFormattable
    {
        int written;
        while (!value.TryFormat(_keyBuffer.AsSpan(pos), out written, default, CultureInfo.InvariantCulture))
        {
            EnsureKeyCapacity(Math.Max(_keyBuffer.Length * 2, pos + 64));
        }

        pos += written;
    }
}
#endif
