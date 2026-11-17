using System.Text;

namespace DataVo.Core.Utils;

/// <summary>
/// Turns stored UTF-8 text into strings, handing out one shared <see cref="string"/> per distinct value. A hashed
/// join table that keeps a low-cardinality text column (a region, a status) for 100,000 rows then holds a handful
/// of strings instead of one per row. A column whose values turn out to be mostly distinct is decoded plainly from
/// then on, since sharing would only add a copy of every value.
/// </summary>
/// <example>
/// <code>
/// var regions = new Utf8StringInterner();
/// string region = regions.Intern(storedBytes); // the same instance for every row holding these bytes
/// </code>
/// </example>
internal sealed class Utf8StringInterner
{
    /// <summary>Values seen before deciding whether sharing pays.</summary>
    private const int Sample = 1024;

    private readonly Utf8KeyTable _keys = new();
    private readonly List<string> _strings = [];
    private int _seen;
    private bool _plain;

    /// <summary>The string of <paramref name="utf8"/>, shared with every earlier value of the same bytes while sharing pays.</summary>
    public string Intern(ReadOnlySpan<byte> utf8)
    {
        if (_plain)
        {
            return Encoding.UTF8.GetString(utf8);
        }

        _seen++;
        if (_keys.TryGetValue(utf8, out int index))
        {
            return _strings[index];
        }

        string value = Encoding.UTF8.GetString(utf8);
        _keys.Add(utf8, _strings.Count);
        _strings.Add(value);
        if (_seen >= Sample && _strings.Count * 2 > _seen)
        {
            _plain = true; // mostly distinct: stop keeping a second copy of every value
        }

        return value;
    }
}
