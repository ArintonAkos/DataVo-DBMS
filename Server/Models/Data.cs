using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models
{
    public class Data
    {
        [JsonProperty("type")]
        public String Type { get; set; }
        [JsonProperty("value")]
        public String Value { get; set; }
        [JsonProperty("is_null")]
        public Boolean? IsNull { get; set; }
        [JsonProperty("length")]
        public Int32? Length { get; set; }
        [JsonProperty("is_primary_key")]
        public Boolean? IsPrimaryKey { get; set; }
    }
}
