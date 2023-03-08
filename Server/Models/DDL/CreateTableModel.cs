using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models.DDL
{
    public class CreateTableModel
    {
        [JsonProperty("table_name")]
        public string TableName { get; set; }
        [JsonProperty("fields")]
        public List<Data> Fields { get; set; }
        [JsonProperty("foreign_keys")]
        public List<ForeignKey>? ForeignKeys { get; set; }
    }
}
