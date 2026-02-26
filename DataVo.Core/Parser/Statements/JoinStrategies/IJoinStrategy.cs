using DataVo.Core.Models.Statement;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

public interface IJoinStrategy
{
    public const int HashLookupThreshold = 32;

    string JoinType { get; }

    HashedTable Execute(
        HashedTable leftRows,
        JoinModel.JoinCondition? condition,
        JoinStrategyContext context);
}