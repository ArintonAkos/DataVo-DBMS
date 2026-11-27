using Microsoft.EntityFrameworkCore.Infrastructure;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

/// <summary>
/// Resolves the DataVo provider mode configured on the current context's options.
/// </summary>
internal sealed class DataVoProviderModeResolver(IDbContextOptions options)
{
    public DataVoProviderModeStatus Resolve()
        => options.FindExtension<DataVoOptionsExtension>()?.GetModeStatus()
            ?? new DataVoProviderModeStatus(DataVoProviderMode.BridgeOnly);
}
