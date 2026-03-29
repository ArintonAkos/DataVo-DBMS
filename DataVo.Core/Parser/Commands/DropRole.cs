using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Security;

namespace DataVo.Core.Parser.Commands;

internal sealed class DropRole(DropRoleStatement ast) : BaseDbAction
{
    protected override DatabasePermission RequiredPermission => DatabasePermission.ManageSecurity;

    public override void PerformAction(Guid session)
    {
        Engine.DropRole(ast.RoleName.Name);
        Messages.Add($"Role '{ast.RoleName.Name}' dropped.");
    }
}
