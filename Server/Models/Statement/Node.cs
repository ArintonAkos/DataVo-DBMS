using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Models.Statement
{
    public class Node
    {
        public enum NodeType
        {
            Value,
            Column,
            Operator
        }

        public Node Left { get; set; }
        public Node Right { get; set; }
        public NodeType Type { get; set; }
        public string Value { get; set; }
    }
}
