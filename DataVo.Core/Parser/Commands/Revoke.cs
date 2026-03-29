using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Security;

namespace DataVo.Core.Parser.Commands;

internal sealed class Revoke(RevokeStatement ast) : BaseDbAction
{
    protected override DatabasePermission RequiredPermission => DatabasePermission.ManageSecurity;

    public override void PerformAction(Guid session)
    {
        if (ast.IsRoleRevoke)
        {
            if (ast.RevokedRole == null)
            {
                throw new InvalidOperationException("Role revoke requires a revoked role name.");
            }

            if (ast.TargetIsRole)
            {
                throw new InvalidOperationException("REVOKE ROLE ... FROM ROLE is not supported.");
            }

            Engine.RevokeRoleFromUser(ast.RevokedRole.Name, ast.TargetName.Name);
            Messages.Add($"Revoked role '{ast.RevokedRole.Name}' from user '{ast.TargetName.Name}'.");
            return;
        }

        List<DatabasePermission> permissions = AuthorizationCommandHelper.ParsePermissions(ast.Permissions);
        if (ast.TargetIsRole)
        {
            Engine.RevokePermissionsFromRole(ast.TargetName.Name, permissions);
            Messages.Add($"Revoked permissions from role '{ast.TargetName.Name}'.");
            return;
        }

        Engine.RevokePermissionsFromUser(ast.TargetName.Name, permissions);
        Messages.Add($"Revoked permissions from user '{ast.TargetName.Name}'.");
    }
}
