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
