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

        public Node? Left { get; set; } = null;
        public Node? Right { get; set; } = null;
        public NodeType Type { get; set; }
        public string? Value { get; set; } = null;
    }
}
