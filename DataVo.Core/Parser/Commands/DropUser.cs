using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Security;

namespace DataVo.Core.Parser.Commands;

internal sealed class DropUser(DropUserStatement ast) : BaseDbAction
{
    protected override DatabasePermission RequiredPermission => DatabasePermission.ManageSecurity;

    public override void PerformAction(Guid session)
    {
        Engine.DropUser(ast.Username.Name);
        Messages.Add($"User '{ast.Username.Name}' dropped.");
    }
}
