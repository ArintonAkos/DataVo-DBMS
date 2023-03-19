using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Http.Attributes
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method)]
    public class Route :  Attribute
    {
        public string Path;

        public Route(string path)
        {
            Path = path;
        }
    }
}
