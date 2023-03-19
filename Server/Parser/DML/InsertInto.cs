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
    internal class InsertInto : BaseDbAction
    {
        private readonly InsertIntoModel _model;

        public InsertInto(Match match)
        {
            _model = InsertIntoModel.FromMatch(match);
        }

        public override void PerformAction(string session)
        {
            
        }
    }
}
