using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using System.Text.RegularExpressions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.Commands;

internal class Go : IDbAction
{
    public Go(Match _)
    {
    }

    public Go(GoStatement ast)
    {
    }

    public QueryResult Perform(Guid session) => QueryResult.Default();
}
