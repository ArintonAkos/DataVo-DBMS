using Frontend.Client.Requests;
using Frontend.Client.Responses.Controllers.Parser;
using System.Threading.Tasks;

namespace Frontend.Services
{
    public static class ParseService
    {
        public static async Task<ParseResponse> GetParseResponse(string query)
        {
            return await HttpService.Post("parser/parse", new Request()
            {
                Data = query
            }) as ParseResponse;
        }
    }
}
