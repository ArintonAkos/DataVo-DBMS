using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Security;

namespace DataVo.Core.Parser.Commands;

internal sealed class ShowUsers(ShowUsersStatement _) : BaseDbAction
{
    protected override DatabasePermission RequiredPermission => DatabasePermission.ManageSecurity;

    public override void PerformAction(Guid session)
    {
        Fields.AddRange(["Username", "Roles", "DirectPermissions"]);

        foreach (SecurityUserView user in Engine.ListUsers())
        {
            Data.Add(new Dictionary<string, object?>
            {
                ["Username"] = user.Username,
                ["Roles"] = string.Join(", ", user.Roles),
                ["DirectPermissions"] = string.Join(", ", user.DirectPermissions.Select(permission => permission.ToString()))
            });
        }
    }
}
