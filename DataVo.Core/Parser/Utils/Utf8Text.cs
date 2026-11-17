using System.Text;

namespace DataVo.Core.Parser.Utils;

/// <summary>
/// Helpers for comparing stored text as UTF-8 bytes instead of decoding it into a string per row.
/// </summary>
internal static class Utf8Text
{
    private static readonly UTF8Encoding Strict = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>
    /// The UTF-8 bytes of <paramref name="text"/>, or null when it is not valid Unicode (a lone surrogate).
    /// Such text has no exact byte form: an encoder would replace the surrogate, and the bytes would then
    /// equal those of different text.
    /// </summary>
    public static byte[]? TryEncode(string text)
    {
        try
        {
            return Strict.GetBytes(text);
        }
        catch (EncoderFallbackException)
        {
            return null;
        }
    }

    /// <summary>Whether every byte of <paramref name="utf8"/> is an ASCII character, so one byte is one character.</summary>
    public static bool IsAscii(ReadOnlySpan<byte> utf8)
    {
#if NET8_0_OR_GREATER
        return Ascii.IsValid(utf8);
#else
        foreach (byte value in utf8)
        {
            if (value >= 0x80)
            {
                return false;
            }
        }

        return true;
#endif
    }

    /// <summary>
    /// <paramref name="utf8"/> without one pair of surrounding single quotes, as stored text written with its
    /// quotes is compared.
    /// </summary>
    public static ReadOnlySpan<byte> UnquoteSingleQuoted(ReadOnlySpan<byte> utf8) =>
        utf8.Length >= 2 && utf8[0] == (byte)'\'' && utf8[^1] == (byte)'\'' ? utf8[1..^1] : utf8;
}
