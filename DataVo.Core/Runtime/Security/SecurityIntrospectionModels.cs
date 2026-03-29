namespace DataVo.Core.Runtime.Security;

internal sealed record SecurityUserView(
    string Username,
    IReadOnlyList<string> Roles,
    IReadOnlyList<DatabasePermission> DirectPermissions);

internal sealed record SecurityRoleView(
    string RoleName,
    IReadOnlyList<DatabasePermission> Permissions);

internal sealed record SecurityGrantView(
    string GranteeType,
    string GranteeName,
    string GrantType,
    string GrantValue);
