using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Security;

namespace DataVo.Core.Parser.Commands;

internal sealed class Logout : BaseDbAction
{
    public Logout(LogoutStatement _) { }

    protected override DatabasePermission RequiredPermission => DatabasePermission.Authenticate;

    public override void PerformAction(Guid session)
    {
        Engine.LogoutSession(session);
        Messages.Add("Session logged out.");
    }
}
