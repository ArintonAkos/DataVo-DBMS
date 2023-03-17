using Server.Models.DDL;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Models.DQL
{
    internal class UseModel
    {
        public String DatabaseName { get; set; }

        public UseModel(String databaseName)
        {
            this.DatabaseName = databaseName;
        }

        public static UseModel FromMatch(Match match)
        {
            return new UseModel(match.Groups["DatabaseName"].Value);
        }
    }
}
