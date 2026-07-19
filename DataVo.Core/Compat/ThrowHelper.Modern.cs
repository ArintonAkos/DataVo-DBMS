#if NET10_0_OR_GREATER
using System.Runtime.CompilerServices;

namespace DataVo.Core.Compat;

internal static class ThrowHelper
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowIfNull<T>(
        T? value,
        [CallerArgumentExpression(nameof(value))] string? paramName = null) =>
        ArgumentNullException.ThrowIfNull(value, paramName);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowIfNullOrWhiteSpace(
        string? value,
        [CallerArgumentExpression(nameof(value))] string? paramName = null) =>
        ArgumentException.ThrowIfNullOrWhiteSpace(value, paramName);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowIfDisposed(bool condition, object instance) =>
        ObjectDisposedException.ThrowIf(condition, instance);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowIfNegative(
        int value,
        [CallerArgumentExpression(nameof(value))] string? paramName = null) =>
        ArgumentOutOfRangeException.ThrowIfNegative(value, paramName);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ThrowIfNegative(
        long value,
        [CallerArgumentExpression(nameof(value))] string? paramName = null) =>
        ArgumentOutOfRangeException.ThrowIfNegative(value, paramName);
}
#endif
