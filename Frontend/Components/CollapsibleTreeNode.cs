
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
