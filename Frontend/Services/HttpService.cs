using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Frontend.Client.Requests;
using Frontend.Client.Responses;
using Newtonsoft.Json;

namespace Frontend.Services
{
    internal class HttpService
    {
        public static string URL = "http://localhost:8001/";
        private static readonly HttpClient _httpClient = new HttpClient();

        public static async Task<Response> Get(string path)
        {
            try
            {
                var response = await _httpClient.GetAsync(URL + path);
                var result = await response.Content.ReadAsStringAsync();

                return JsonConvert.DeserializeObject<Response>(result);
            }
            catch (HttpRequestException ex)
            {
                Console.WriteLine(ex.Message);
                return new ErrorResponse(ex);
            }
        }

        public static async Task<Response> Post(string path, Request request)
        {
            try
            {
                var jsonObject = JsonConvert.SerializeObject(request);
                var data = new StringContent(jsonObject, Encoding.UTF8, "application/json");

                var response = await _httpClient.PostAsync(URL + path, data);
                var result = await response.Content.ReadAsStringAsync();

                return JsonConvert.DeserializeObject<Response>(result);
            }
            catch (HttpRequestException ex)
            {
                Console.WriteLine(ex.Message);
                return new ErrorResponse(ex);
            }
        }
    }
}