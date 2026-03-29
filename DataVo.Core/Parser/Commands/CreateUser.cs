using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Security;

namespace DataVo.Core.Parser.Commands;

internal sealed class CreateUser(CreateUserStatement ast) : BaseDbAction
{
    protected override DatabasePermission RequiredPermission => DatabasePermission.ManageSecurity;

    public override void PerformAction(Guid session)
    {
        string password = AuthorizationCommandHelper.UnquoteSqlStringLiteral(ast.PasswordLiteral);
        string? roleName = ast.RoleName?.Name;

        Engine.CreateUser(ast.Username.Name, password, roleName);
        Messages.Add($"User '{ast.Username.Name}' created.");
    }
}
