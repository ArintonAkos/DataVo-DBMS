using Server.Parser.Actions;
using Server.Server.Responses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Commands
{
    internal class Go : DbAction
    {
        public Response Perform()
        {
            // silence...
            return new Response();
        }
    }
}
