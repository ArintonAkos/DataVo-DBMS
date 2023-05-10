using System.ComponentModel.DataAnnotations;
using Newtonsoft.Json;

namespace Server.Server.Requests.Controllers.Parser;

public class ParseRequest : Request
{
    [Required(ErrorMessage = "Session parameter is required!")]
    [JsonProperty("session")]
    public Guid Session { get; set; } = Guid.Empty;
}