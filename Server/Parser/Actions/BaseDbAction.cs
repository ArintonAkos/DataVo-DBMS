using Server.Server.MongoDB;
using Server.Server.Responses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Actions
{
    internal abstract class BaseDbAction : DbAction
    {
        protected DbContext Context;

        public BaseDbAction()
        {
            this.Context = DbContext.Instance;
        }

        public abstract Response Perform();
    }
}
