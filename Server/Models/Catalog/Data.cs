using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations;

namespace Server.Models.Catalog
{
    public class Data
    {
        [Required(ErrorMessage = "Data must belong to a table!")]
        public string Table { get; set; }

        [Required(ErrorMessage = "Data must have a type!")]
        public string Type { get; set; }

        [Required(ErrorMessage = "Data must have a value!")]
        public string Value { get; set; }
    }
}
