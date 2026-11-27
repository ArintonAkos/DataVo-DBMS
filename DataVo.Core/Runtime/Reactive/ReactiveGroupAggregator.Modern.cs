#if NET8_0_OR_GREATER
using System.Globalization;

namespace DataVo.Core.Runtime.Reactive;

internal sealed partial class ReactiveGroupAggregator
{
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
