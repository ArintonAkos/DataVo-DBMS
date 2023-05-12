using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Frontend.Components
{
    public class CollapsibleTreeNode : TreeNode
    {
        public bool Collapsed { get; set; }

        public CollapsibleTreeNode(string text) : base(text)
        {
            Collapsed = false;
        }
    }
}
