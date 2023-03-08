using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models
{
    public class ForeignKey
    {
        [JsonProperty("from_table")]
        public String FromTable { get; set; }
        [JsonProperty("to_table")]
        public String ToTable { get; set; }
        [JsonProperty("source_field")]
        public Data SourceField { get; set; }
        [JsonProperty("destination_field")]
        public Data DestinationField { get; set; }
    }
}
