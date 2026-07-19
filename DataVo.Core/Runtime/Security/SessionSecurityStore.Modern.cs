#if NET10_0_OR_GREATER
namespace DataVo.Core.Runtime.Security;

internal sealed partial class SessionSecurityStore
{
    private static DatabasePermission[] GetDatabasePermissions() =>
        Enum.GetValues<DatabasePermission>();
}
#endif
