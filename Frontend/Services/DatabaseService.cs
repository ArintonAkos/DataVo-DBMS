using Frontend.Client.Responses;
using Frontend.Client.Responses.Controllers.DatabaseListResponse;
using System.Threading.Tasks;

namespace Frontend.Services
{
    internal class DatabaseService
    {
        public static async Task<DatabaseListResponse> GetDatabaseList()
        {
            var response = await HttpService.Get<DatabaseListResponse>("database/list");

            if (response is null)
            {
                return new()
                {
                    Data = new(),
                };
            }

            return (response as DatabaseListResponse)!;
        }
    }
}
