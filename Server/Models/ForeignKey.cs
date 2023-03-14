using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations;
using System.Xml.Serialization;

namespace Server.Models
{
    [Serializable]
    [XmlRoot("ForeignKey")]
    public class ForeignKey
    {
        [XmlElement("FkAttribute")]
        public string SourceFieldAttributeName { get; set; }

        [XmlElement("Reference")]
        public Reference Reference { get; set; }

        [XmlIgnore]
        [Required(ErrorMessage = "Source field attribute is required in Foreign Key!")]
        public Field SourceField { get; set; }

        [XmlIgnore]
        [Required(ErrorMessage = "Destination field attribute is required in Foreign Key!")]
        public Field DestinationField { get; set; }
    }
}
