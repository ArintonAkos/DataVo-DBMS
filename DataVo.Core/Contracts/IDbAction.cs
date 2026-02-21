using DataVo.Core.Contracts.Results;
namespace DataVo.Core.Contracts;

internal interface IDbAction
{
    public QueryResult Perform(Guid session);
}