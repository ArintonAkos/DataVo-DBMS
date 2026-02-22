using DataVo.Core.Parser.Types;

namespace DataVo.Core.Utils
{
    public static class ListExtensions
    {
        public static ListedTable ToListedTable(this IEnumerable<JoinedRow> list)
        {
            return new ListedTable(list.ToList());
        }

        public static ListedTable ToListedTable(this List<JoinedRow> list)
        {
            return new ListedTable(list);
        }
    }
}
