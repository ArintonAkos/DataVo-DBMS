using Server.Parser.Actions;
using Server.Server.Requests;
using Server.Server.Responses;
using Server.Utils;
using System;


namespace Server.Parser
{
    internal class Parser
    {
        private Request _request { get; set; }

        public Parser(Request request)
        {
            this._request = request;
        }

        public Response Parse()
        {
            DbAction action = RequestMapper.ToAction(this._request);

            return action.Perform();
        }
    }
}
