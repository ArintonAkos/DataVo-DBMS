using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Server.Models
{
    [Serializable]
    public class PrimaryKey
    {
        [XmlElement("pkAttribute")]
        public string AttributeName { get; set; }
    }
}
