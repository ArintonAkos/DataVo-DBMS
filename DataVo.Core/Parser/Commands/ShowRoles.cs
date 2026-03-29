using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Security;

namespace DataVo.Core.Parser.Commands;

internal sealed class ShowRoles(ShowRolesStatement _) : BaseDbAction
{
    protected override DatabasePermission RequiredPermission => DatabasePermission.ManageSecurity;

    public override void PerformAction(Guid session)
    {
        Fields.AddRange(["Role", "Permissions"]);

        foreach (SecurityRoleView role in Engine.ListRoles())
        {
            Data.Add(new Dictionary<string, object?>
            {
                ["Role"] = role.RoleName,
                ["Permissions"] = string.Join(", ", role.Permissions.Select(permission => permission.ToString()))
            });
        }
    }
}
