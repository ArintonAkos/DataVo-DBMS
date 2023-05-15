using Frontend.Client.Requests;
using Frontend.Client.Responses;
using Frontend.Client.Responses.Controllers.Parser;

namespace Frontend.Services
{
    public static class ParseService
    {
        public static async Task<ParseResponse> GetParseResponse(string query, Guid session)
        {
            var response = await HttpService.Post<ParseResponse>("parser/parse", new ParseRequest()
            {
                Data = query,
                Session = session
            });

            if (response is null)
            {
                var e = new Exception("Error parsing the response!");
                return new ErrorResponse(e);
            }

            return response;
        }
    }
}
