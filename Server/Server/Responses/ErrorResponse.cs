using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Responses
{
    internal class ErrorResponse : Response
    {
        public ErrorResponse(Exception e)
        {
            this.Data = new()
            {
                new ScriptResponse
                {
                    IsSuccess = false,
                    Actions = new() { ActionResponse.Error(e) }
                }
            };
        }
    }
}
