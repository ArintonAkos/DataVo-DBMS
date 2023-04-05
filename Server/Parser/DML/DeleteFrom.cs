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
            
        }
    }
}
