#if !NET8_0_OR_GREATER
using System.Globalization;

namespace DataVo.Core.Runtime.Reactive;

internal sealed partial class ReactiveGroupAggregator
{
    private void AppendFormattable<T>(ref int pos, T value)
        where T : IFormattable =>
        AppendChars(ref pos, value.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty);
}
#endif
