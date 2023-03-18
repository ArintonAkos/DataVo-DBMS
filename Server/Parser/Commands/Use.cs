using Server.Models.DQL;
using Server.Parser.Actions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Parser.Commands
{
    internal class Use : BaseDbAction
    {
        private readonly UseModel _model;

        public Use(Match match)
        {
            this._model = UseModel.FromMatch(match);
        }

        public override void PerformAction()
        {
            // Use the database

        }
    }
}
