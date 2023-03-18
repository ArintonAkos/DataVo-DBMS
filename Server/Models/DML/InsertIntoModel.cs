using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models.DML
{
    internal class InsertIntoModel
    {
        public String DatabaseName { get; set; }
        public List<Dictionary<String, Data>> Values { get; set; }
    }
}
