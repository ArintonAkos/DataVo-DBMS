using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Requests
{
    public class Request
    {
        [Required(ErrorMessage = "Data parameter is required!")]
        [JsonProperty("data")]
        public string Data { get; set; } = string.Empty;
    }
}
