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
            List<Queue<DbAction>> runnables = RequestMapper.ToRunnables(this._request);

            foreach (var runnable in runnables) 
            {
                while (runnable.Any())
                {
                    runnable.Dequeue().Perform();
                }
            }

            return new Response();
        }
    }
}
