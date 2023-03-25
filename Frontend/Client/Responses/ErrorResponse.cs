using System;
using System.Collections.Generic;

namespace Frontend.Client.Responses
{
    public class ErrorResponse : Response
    {
        public ErrorResponse(Exception e)
        {
            this.Data = new ScriptResponse()
            {
                IsSuccess = false,
                Actions = new List<ActionResponse>() 
                {
                    ActionResponse.Error(e) 
                }
            };
        }
    }
}
