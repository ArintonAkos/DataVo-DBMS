using System.Text;
using Frontend.Client.Requests;
using Frontend.Client.Responses;
using Newtonsoft.Json;

namespace Frontend.Services
{
    internal class HttpService
    {
        public static string URL = "http://localhost:8001/";
        private static readonly HttpClient _httpClient = new();

        public static async Task<T?> Get<T>(string path) where T : Response
        {
            try
            {
                var response = await _httpClient.GetAsync(URL + path);
                var result = await response.Content.ReadAsStringAsync();
                
                return JsonConvert.DeserializeObject<T>(result);
            }
            catch (HttpRequestException ex)
            {
                Console.WriteLine(ex.Message);
                return null;
            }
        }

        public static async Task<T?> Post<T>(string path, Request request) where T : Response
        {
            try
            {
                var jsonObject = JsonConvert.SerializeObject(request);
                var data = new StringContent(jsonObject, Encoding.UTF8, "application/json");

                var response = await _httpClient.PostAsync(URL + path, data);
                var result = await response.Content.ReadAsStringAsync();

                return JsonConvert.DeserializeObject<T>(result);
            }
            catch (HttpRequestException ex)
            {
                Console.WriteLine(ex.Message);
                return null;
            }
        }
    }
}