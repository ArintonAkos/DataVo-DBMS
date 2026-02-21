using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using System.Text.RegularExpressions;

namespace DataVo.Core.Parser.Commands;

internal class Go : IDbAction
{
    public Go(Match _)
    {
    }

    public QueryResult Perform(Guid session) => QueryResult.Default();
}
