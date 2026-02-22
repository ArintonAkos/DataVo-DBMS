using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Aggregations
{
    internal class Count(Column field) : Aggregation(field)
    {
        protected override dynamic? Apply(ListedTable rows)
        {
            if (_field.TableName == "*")
            {
                return rows.Count;
            }

            return rows.Select(SelectColumn)
                .Where(c => c != null)
                .Count();
        }
    }
}
