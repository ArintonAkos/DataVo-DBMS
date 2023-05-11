using System.ComponentModel.DataAnnotations;
using Newtonsoft.Json;

namespace Server.Server.Requests;

public class Request
{
    [Required(ErrorMessage = "Data parameter is required!")]
    [JsonProperty("data")]
    public string Data { get; set; } = string.Empty;
}