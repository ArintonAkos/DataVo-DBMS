using Newtonsoft.Json;
using Server.Parser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Requests
{
    internal class Request
    {
        [JsonProperty("command_type")]
        public String CommandType { get; set; }
        [JsonProperty("data")]
        public String Data { get; set; }

        public bool IsValid()
        {
            return RequestMapper.IsValid(CommandType);
        }
    }
}
