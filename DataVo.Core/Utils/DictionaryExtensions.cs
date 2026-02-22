using DataVo.Core.Parser.Types;

namespace DataVo.Core.Utils
{
    public static class DictionaryExtensions
    {
        public static Row ToRow(this Dictionary<string, dynamic> dictionary)
        {
            return new Row(dictionary);
        }

        public static HashedTable ToHashedTable(this Dictionary<string, JoinedRow> dictionary)
        {
            return new HashedTable(dictionary);
        }
    }
}
