using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Security;

namespace DataVo.Core.Parser.Commands;

internal sealed class ShowGrants(ShowGrantsStatement ast) : BaseDbAction
{
    protected override DatabasePermission RequiredPermission => DatabasePermission.ManageSecurity;

    public override void PerformAction(Guid session)
    {
        Fields.AddRange(["GranteeType", "Grantee", "GrantType", "GrantValue"]);

        IEnumerable<SecurityGrantView> grants = Engine.ListGrants();

        if (ast.FilterByUser && ast.PrincipalName != null)
        {
            grants = grants.Where(grant =>
                grant.GranteeType.Equals("USER", StringComparison.OrdinalIgnoreCase)
                && grant.GranteeName.Equals(ast.PrincipalName.Name, StringComparison.OrdinalIgnoreCase));
        }
        else if (ast.FilterByRole && ast.PrincipalName != null)
        {
            grants = grants.Where(grant =>
                grant.GranteeType.Equals("ROLE", StringComparison.OrdinalIgnoreCase)
                && grant.GranteeName.Equals(ast.PrincipalName.Name, StringComparison.OrdinalIgnoreCase));
        }

        foreach (SecurityGrantView grant in grants)
        {
            Data.Add(new Dictionary<string, object?>
            {
                ["GranteeType"] = grant.GranteeType,
                ["Grantee"] = grant.GranteeName,
                ["GrantType"] = grant.GrantType,
                ["GrantValue"] = grant.GrantValue
            });
        }
    }
}
