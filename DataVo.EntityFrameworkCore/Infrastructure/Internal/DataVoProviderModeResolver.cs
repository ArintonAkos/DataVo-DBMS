using Microsoft.EntityFrameworkCore.Infrastructure;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

internal sealed class DataVoProviderModeResolver
{
    private readonly IDbContextOptions _options;

    public DataVoProviderModeResolver(IDbContextOptions options)
    {
        _options = options;
    }

    public DataVoProviderModeStatus Resolve()
    {
        var extension = _options.FindExtension<DataVoOptionsExtension>();
        if (extension is null)
        {
            return new DataVoProviderModeStatus(
                DataVoProviderMode.BridgeOnly,
                ProviderIdentityPreviewEnabled: false,
                NativeQueryTranslationPreviewEnabled: false);
        }

        DataVoProviderMode mode = extension.NativeQueryTranslationPreviewEnabled
            ? DataVoProviderMode.NativeTranslationPreview
            : extension.ProviderIdentityPreviewEnabled
                ? DataVoProviderMode.ProviderIdentityPreview
                : DataVoProviderMode.BridgeOnly;

        return new DataVoProviderModeStatus(
            mode,
            extension.ProviderIdentityPreviewEnabled,
            extension.NativeQueryTranslationPreviewEnabled);
    }
}
