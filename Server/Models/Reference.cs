using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Server.Models
{
    [Serializable]
    [XmlRoot("References")]
    public class Reference
    {
        [XmlElement("RefTable")]
        public String ReferenceTableName { get; set; }
        
        [XmlElement("RefAttribute")]
        public String ReferenceAttributeName { get; set; }
    }
}
