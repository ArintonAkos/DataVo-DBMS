using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Server.Models
{
    [Serializable]
    [XmlRoot("Database")]
    public class Database
    {
        [XmlAttribute]
        public String DatabaseName { get; set; }

        [XmlArray("Tables")]
        [XmlArrayItem("Table")]
        public List<Table> Tables { get; set; }

        public Database() { }
    }
}
