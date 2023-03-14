using Server.Server.Responses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Actions
{
    internal interface IDbAction
    {
        public ActionResponse Perform();
    }
}
