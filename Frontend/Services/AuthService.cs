using Frontend.Client.Responses.Controllers.Auth;

namespace Frontend.Services
{
    public static class AuthService
    {
        public async static Task<Guid?> CreateSession()
        {
            var response = await HttpService.Get<SessionResponse>("auth/session");

            if (response is null)
            {
                throw new Exception("Error creating the session!");
            }

            return response.Data;
        }
    }
}
