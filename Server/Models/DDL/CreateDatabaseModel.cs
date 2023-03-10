using Server.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Server.Models.DDL
{
    [XmlRoot("Database")]
    [Serializable]
    internal class CreateDatabaseModel
    {
        [XmlAttribute]
        public String DatabaseName { get; set; }

        public CreateDatabaseModel(String databaseName)
        {
            this.DatabaseName = databaseName;
        }

        public static CreateDatabaseModel FromMatch(Match match)
        {
            return new CreateDatabaseModel(match.NthGroup(1).Value);
        }
    }
}
