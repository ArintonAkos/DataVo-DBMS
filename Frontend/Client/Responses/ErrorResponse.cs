using Frontend.Client.Responses.Controllers.Parser;
using System;
using System.Collections.Generic;

namespace Frontend.Client.Responses
{
    internal class ErrorResponse : ParseResponse
    {
        public ErrorResponse(Exception e)
        {
            Data = new List<ScriptResponse>()
            {
                new ScriptResponse()
                {
                    IsSuccess = false,
                    Actions = new List<ActionResponse>()
                    {
                        ActionResponse.Error(e)
                    }
                }
            };
        }
    }
}
