using static DataVo.Core.Models.Statement.Utils.Node;

namespace DataVo.Core.Utils;

public static class NodeValueTypExtensions
{
    public static bool IsNumeric(this NodeValueType type) => type == NodeValueType.Int || type == NodeValueType.Double;
}