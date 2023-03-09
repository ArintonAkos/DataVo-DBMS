using Microsoft.Extensions.Logging;
using Server.Logging;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.Responses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Parser.DDL
{
    internal class DropDatabase : DbAction
    {
        private readonly DropDatabaseModel _model;

        public DropDatabase(Match match)
        {
            _model = DropDatabaseModel.FromMatch(match);
        }

        public Response Perform()
        {
            // Drop MongoDb database
            Logger.Info(_model.DatabaseName);


            return new Response();
        }
    }
}
