using Server.Server.Responses.Controllers.Database;
using System.Threading.Tasks;

namespace Frontend.Services
{
    internal class DatabaseService
    {
        public static async Task<DatabaseListResponse> GetDatabaseList()
        {
            return await HttpService.Get("database/list") as DatabaseListResponse;
        }
    }
}
