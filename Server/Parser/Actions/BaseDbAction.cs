using Server.Server.MongoDB;
using Server.Server.Responses;
using Server.Server.Responses.Parts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Actions
{
    internal abstract class BaseDbAction : IDbAction
    {
        protected DbContext Context;

        protected List<String> Messages = new();
        protected List<FieldResponse> Fields = new();
        protected List<List<DataResponse>> Data = new();

        public BaseDbAction()
        {
            this.Context = DbContext.Instance;
        }

        /// <summary>
        /// Do actions on the Messages, Fields, Data fields.
        /// These values will be returned.
        /// </summary>
        public abstract void PerformAction();

        public ActionResponse Perform()
        {
            PerformAction();

            return ActionResponse.FromRaw(Messages, Data, Fields);
        }
    }
}
