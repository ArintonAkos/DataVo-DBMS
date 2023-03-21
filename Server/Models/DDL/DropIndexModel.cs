using Server.Utils;
using System.Text.RegularExpressions;

namespace Server.Models.DDL
{
    public class DropIndexModel
    {
        public string TableName { get; set; }
        public string IndexName { get; set; }

        public DropIndexModel(string indexName, string tableName)
        {
            this.TableName = tableName;
            this.IndexName = indexName;
        }

        public static DropIndexModel FromMatch(Match match)
        {
            return new DropIndexModel(match.NthGroup(1).Value, match.NthGroup(2).Value);
        }
    }
}
