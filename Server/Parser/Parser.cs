using Server.Parser.Actions;
using Server.Parser.Utils;
using Server.Server.Requests;
using Server.Server.Responses;

namespace Server.Parser
{
    internal class Parser
    {
        private Request Request { get; set; }

        public Parser(Request request)
        {
            this.Request = request;
        }

        public Response Parse()
        {
            Response response = new();

            List<Queue<IDbAction>> runnables = RequestMapper.ToRunnables(this.Request);

            foreach (var runnable in runnables) 
            {
                ScriptResponse scriptResponse = new();

                while (runnable.Any())
                {
                    try
                    {
                        scriptResponse.Actions.Add(runnable.Dequeue().Perform());
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
