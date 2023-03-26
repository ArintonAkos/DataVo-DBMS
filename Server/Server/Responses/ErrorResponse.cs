using Server.Server.Responses.Controllers.Parser;

namespace Server.Server.Responses
{
    internal class ErrorResponse : ParseResponse
    {
        public ErrorResponse(Exception e)
        {
            Data = new()
            {
                new()
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
