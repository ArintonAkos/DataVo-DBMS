using Server.Models.Catalog;
using System.Text.RegularExpressions;

namespace Server.Models.DML
{
    internal class InsertIntoModel
    {
        public String DatabaseName { get; set; }
        public List<Dictionary<String, Data>> Values { get; set; }

        public static InsertIntoModel FromMatch(Match match)
        {
            return new InsertIntoModel
            { 
                DatabaseName = match.Groups["DatabaseName"].Value,
                Values = new()
            };
        }
    }
}
