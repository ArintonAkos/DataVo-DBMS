using Server.Utils;
using System.Text.RegularExpressions;

namespace Server.Models.DQL
{
    internal class DescribeModel
    {
        public String TableName { get; set; }

        public DescribeModel(String tableName)
        {
            this.TableName = tableName;
        }

        public static DescribeModel FromMatch(Match match)
        {
            return new DescribeModel(match.NthGroup(1).Value);
        }
    }
}
