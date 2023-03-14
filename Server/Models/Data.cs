using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models
{
    public class Data
    {
        [Required(ErrorMessage = "Data must belong to a table!")]
        public string Table { get; set; }
        
        [Required(ErrorMessage = "Data must have a type!")]
        public String Type { get; set; }
        
        [Required(ErrorMessage = "Data must have a value!")]
        public String Value { get; set; }
    }
}
