using System.ComponentModel.DataAnnotations;
using Newtonsoft.Json;

namespace Frontend.Client.Requests 
{
    internal class ParseRequest : Request
    {
        [Required(ErrorMessage = "Session parameter is required!")]
        [JsonProperty("session")]
        public Guid Session { get; set; } = Guid.Empty;
    }
};