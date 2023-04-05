using Server.Models.DML;
using Server.Parser.Actions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Parser.DML
{
    internal class DeleteFrom : BaseDbAction
    {
        private readonly DeleteFromModel _model;

        public DeleteFrom(Match match)
        {
            _model = DeleteFromModel.FromMatch(match);
        }

        public override void PerformAction(Guid session)
        {
            // TO-DO: @Bulcsu - Implement this method
            // We should get the data chunked from the database (2000 chunks maybe?)
            // We should run the _model.WhereModel.Evaluate() method on each element of the chunk.
            // If the result is true, we should keep the element in the chunk
            // So you will have to run a filter on it
            // Append the chunks and the return value will be the concatenateed lists of filtered chunks.

            // We need chunks so we don't oerload the network and the server cpu and memory

            // _model.WhereModel.Evaluate();
        }
    }
}
