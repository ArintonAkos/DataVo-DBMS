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
            this.Data = new
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
