
namespace Server.Server.Responses
{
    public class ErrorResponse : Response
    {
        public ErrorResponse(Exception e)
        {
            this.Data = new()
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
