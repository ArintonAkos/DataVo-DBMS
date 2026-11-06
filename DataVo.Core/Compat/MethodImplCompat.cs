using System.Runtime.CompilerServices;

namespace DataVo.Core.Compat;

/// <summary>Method compilation hints that not every target framework names.</summary>
internal static class MethodImplCompat
{
    /// <summary>
    /// <c>MethodImplOptions.AggressiveOptimization</c> (512): compile the method optimized from its first
    /// call instead of tiering up. netstandard2.1 does not declare the member; runtimes that know the flag
    /// honour the value, others ignore it.
    /// </summary>
    public const MethodImplOptions AggressiveOptimization = (MethodImplOptions)512;
}
