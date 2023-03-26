using Server.Parser.Actions;
using Server.Parser.Utils;
using Server.Server.Requests.Controllers.Parser;
using Server.Server.Responses;
using Server.Server.Responses.Controllers.Parser;

namespace Server.Parser
{
    internal class Parser
    {
        private ParseRequest Request { get; set; }

        public Parser(ParseRequest request)
        {
            this.Request = request;
        }

        public ParseResponse Parse()
        {
            ParseResponse response = new();

            List<Queue<IDbAction>> runnables = RequestMapper.ToRunnables(this.Request);

            foreach (var runnable in runnables) 
            {
                ScriptResponse scriptResponse = new();

                while (runnable.Any())
                {
                    try
                    {
                        scriptResponse.Actions.Add(runnable.Dequeue().Perform(Request.Session));
                    }
                    catch (Exception ex)
                    {
                        scriptResponse.Actions.Add(ActionResponse.Error(ex));
                        scriptResponse.IsSuccess = false;
                        break;
                    }
                }

                response.Data.Add(scriptResponse);
            }

            return response;
        }
    }
}
