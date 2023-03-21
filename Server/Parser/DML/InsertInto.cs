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

        public override void PerformAction(Guid session)
        {
            // We should get the data from the database
            // Firstly the table scheme
            // Then we link the InsertIntoModel with the database data (the types, columns, etc)
            // Then we convert our input values to the desired types
            // Note: The conversion method is already defined in DataFactory.cs
            // The DataFactory class links the data to a table and supplies the data object
            // with some additional info and makes the value parsing possible.
            // So when we create a new data we should always call the DataFactory method instead of
            // creating a new instance of Data of our own.

            // @bulcsu - Do the tasks above
        }
    }
}
