using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Security;

namespace DataVo.Core.Parser.Commands;

internal sealed class Grant(GrantStatement ast) : BaseDbAction
{
    protected override DatabasePermission RequiredPermission => DatabasePermission.ManageSecurity;

    public override void PerformAction(Guid session)
    {
        if (ast.IsRoleGrant)
        {
            if (ast.GrantedRole == null)
            {
                throw new InvalidOperationException("Role grant requires a granted role name.");
            }

            if (ast.TargetIsRole)
            {
                throw new InvalidOperationException("GRANT ROLE ... TO ROLE is not supported.");
            }

            Engine.GrantRoleToUser(ast.GrantedRole.Name, ast.TargetName.Name);
            Messages.Add($"Granted role '{ast.GrantedRole.Name}' to user '{ast.TargetName.Name}'.");
            return;
        }

        List<DatabasePermission> permissions = AuthorizationCommandHelper.ParsePermissions(ast.Permissions);
        if (ast.TargetIsRole)
        {
            Engine.GrantPermissionsToRole(ast.TargetName.Name, permissions);
            Messages.Add($"Granted permissions to role '{ast.TargetName.Name}'.");
            return;
        }

        Engine.GrantPermissionsToUser(ast.TargetName.Name, permissions);
        Messages.Add($"Granted permissions to user '{ast.TargetName.Name}'.");
    }
}
