using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Frontend.Components
{
    public partial class CollapsibleTreeView : TreeView
    {
        public CollapsibleTreeView()
        {
            InitializeComponent();
            BeforeCollapse += CollapsibleTreeView_BeforeCollapse;
            BeforeExpand += CollapsibleTreeView_BeforeExpand;
        }

        private void CollapsibleTreeView_BeforeCollapse(object sender, TreeViewCancelEventArgs e)
        {
            if (e.Node is CollapsibleTreeNode collapsibleNode)
            {
                collapsibleNode.Collapsed = true;
                UpdateNodeImage(collapsibleNode);
            }
        }

        private void CollapsibleTreeView_BeforeExpand(object sender, TreeViewCancelEventArgs e)
        {
            if (e.Node is CollapsibleTreeNode collapsibleNode)
            {
                collapsibleNode.Collapsed = false;
                UpdateNodeImage(collapsibleNode);
            }
        }

        protected override void OnDrawNode(DrawTreeNodeEventArgs e)
        {
            if (e.Node is CollapsibleTreeNode collapsibleNode)
            {
                UpdateNodeImage(collapsibleNode);
            }

            base.OnDrawNode(e);
        }

        private void UpdateNodeImage(CollapsibleTreeNode node)
        {
            if (node.Collapsed)
            {
                node.ImageIndex = 1;
                node.SelectedImageIndex = 1;
            }
            else
            {
                node.ImageIndex = 0;
                node.SelectedImageIndex = 0;
            }
        }
    }
}
