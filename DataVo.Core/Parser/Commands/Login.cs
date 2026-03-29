using DataVo.Core.Exceptions;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Security;

namespace DataVo.Core.Parser.Commands;

internal sealed class Login(LoginStatement ast) : BaseDbAction
{
    protected override DatabasePermission RequiredPermission => DatabasePermission.Authenticate;

    public override void PerformAction(Guid session)
    {
        string password = AuthorizationCommandHelper.UnquoteSqlStringLiteral(ast.PasswordLiteral);
        bool authenticated = Engine.AuthenticateSession(session, ast.Username.Name, password);
        if (!authenticated)
        {
            throw new AuthorizationException("Invalid username or password.");
        }

        Messages.Add($"Session authenticated as '{ast.Username.Name}'.");
    }
}
