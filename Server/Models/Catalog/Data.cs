using Newtonsoft.Json;
using Server.Enums;
using System.ComponentModel.DataAnnotations;

namespace Server.Models.Catalog
{
    public class Data
    {
        //[Required(ErrorMessage = "Data must belong to a table!")]
        //public string Table { get; set; }

        [Required(ErrorMessage = "Data must have a type!")]
        public DataTypes Type { get; set; }

        [Required(ErrorMessage = "Data must have a value!")]
        public dynamic Value { get; set; }

        public Data(DataTypes type, dynamic Value)
        {
            this.Type = type;
            this.Value = Value;
        }

        public Data()
        {
        }
    }
}
