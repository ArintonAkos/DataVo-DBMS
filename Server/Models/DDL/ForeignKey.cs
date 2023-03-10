using Newtonsoft.Json;
using Server.Models.DDL;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Server.Models
{
    [Serializable]
    [XmlType("ForeignKey")]
    public class ForeignKey
    {
        [XmlIgnore]
        [Required(ErrorMessage = "Source field attribute is required in Foreign Key!")]
        public Field SourceField { get; set; }
        
        [Required(ErrorMessage = "Destination field attribute is required in Foreign Key!")]
        public Field DestinationField { get; set; }
    }
}
