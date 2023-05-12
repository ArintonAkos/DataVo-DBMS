using Frontend.Client.Requests;
using Frontend.Client.Responses;
using Frontend.Client.Responses.Controllers.Parser;
using System.Threading.Tasks;

namespace Frontend.Services
{
    public static class ParseService
    {
        public static async Task<ParseResponse> GetParseResponse(string query)
        {
            var response = await HttpService.Post<ParseResponse>("parser/parse", new Request()
            {
                Data = query
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
