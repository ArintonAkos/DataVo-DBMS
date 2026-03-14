using System.Threading;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

/// <summary>
/// Ambient execution flags for the DataVo EF bridge.
/// </summary>
internal static class DataVoBridgeExecutionContext
{
    private static readonly AsyncLocal<bool> _suppressDataVoWrites = new();

    /// <summary>
    /// When <c>true</c>, the SaveChanges interceptor skips writing to DataVo.
    /// Used internally while hydrating EF's InMemory store from DataVo rows.
    /// </summary>
    public static bool SuppressDataVoWrites
    {
        get => _suppressDataVoWrites.Value;
        set => _suppressDataVoWrites.Value = value;
    }
}
