using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Http.Attributes
{
    [AttributeUsage(AttributeTargets.Method)]
    internal class Method : Attribute
    {
        public readonly string HttpMethod;

        public Method(string httpMethod)
        {
            this.HttpMethod = httpMethod;
        }
    }
}
