using System;
using System.Collections.Generic;
using Frontend.Client.Responses.Controllers.Parser;

namespace Frontend.Client.Responses
{
    public class ErrorResponse : ParseResponse
    {
        public ErrorResponse(Exception e) =>
            Data = new List<ScriptResponse>
            {
                new ScriptResponse
                {
                    IsSuccess = false,
                    Actions = new List<ActionResponse>
                    {
                        ActionResponse.Error(e),
                    },
                },
            };
    }
}