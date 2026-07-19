#if NETSTANDARD2_1
namespace DataVo.Core.Runtime.Security;

internal sealed partial class SessionSecurityStore
{
    private static DatabasePermission[] GetDatabasePermissions() =>
        Enum.GetValues(typeof(DatabasePermission)).Cast<DatabasePermission>().ToArray();
}
#endif
