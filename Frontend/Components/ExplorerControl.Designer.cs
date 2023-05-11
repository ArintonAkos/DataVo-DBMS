namespace Frontend.Components
{
    partial class ExplorerControl
    {
        /// <summary> 
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(ExplorerControl));
            System.Windows.Forms.TreeNode treeNode1 = new System.Windows.Forms.TreeNode("Open folder");
            System.Windows.Forms.TreeNode treeNode2 = new System.Windows.Forms.TreeNode("Databases", 2, 2);
            this.ImageList = new System.Windows.Forms.ImageList(this.components);
            this.SidebarPanel = new System.Windows.Forms.TableLayoutPanel();
            this.ExplorerTree = new System.Windows.Forms.TreeView();
            this.EditorExplorerLabel = new System.Windows.Forms.Label();
            this.SidebarPanel.SuspendLayout();
            this.SuspendLayout();
            // 
            // ImageList
            // 
            this.ImageList.ImageStream = ((System.Windows.Forms.ImageListStreamer)(resources.GetObject("ImageList.ImageStream")));
            this.ImageList.TransparentColor = System.Drawing.Color.Transparent;
            this.ImageList.Images.SetKeyName(0, "folder.png");
            this.ImageList.Images.SetKeyName(1, "file.png");
            this.ImageList.Images.SetKeyName(2, "database.png");
            // 
            // SidebarPanel
            // 
            this.SidebarPanel.ColumnCount = 1;
            this.SidebarPanel.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.SidebarPanel.Controls.Add(this.ExplorerTree, 0, 1);
            this.SidebarPanel.Controls.Add(this.EditorExplorerLabel, 0, 0);
            this.SidebarPanel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.SidebarPanel.Location = new System.Drawing.Point(0, 0);
            this.SidebarPanel.Name = "SidebarPanel";
            this.SidebarPanel.RowCount = 2;
            this.SidebarPanel.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Absolute, 22F));
            this.SidebarPanel.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.SidebarPanel.Size = new System.Drawing.Size(164, 367);
            this.SidebarPanel.TabIndex = 2;
            // 
            // ExplorerTree
            // 
            this.ExplorerTree.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.ExplorerTree.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ExplorerTree.ImageIndex = 0;
            this.ExplorerTree.ImageList = this.ImageList;
            this.ExplorerTree.Location = new System.Drawing.Point(3, 25);
            this.ExplorerTree.Name = "ExplorerTree";
            treeNode1.Name = "openFolder";
            treeNode1.Tag = "None";
            treeNode1.Text = "Open folder";
            treeNode2.ImageIndex = 2;
            treeNode2.Name = "databases";
            treeNode2.SelectedImageIndex = 2;
            treeNode2.Text = "Databases";
            this.ExplorerTree.Nodes.AddRange(new System.Windows.Forms.TreeNode[] {
            treeNode1,
            treeNode2});
            this.ExplorerTree.SelectedImageIndex = 0;
            this.ExplorerTree.Size = new System.Drawing.Size(158, 339);
            this.ExplorerTree.TabIndex = 0;
            this.ExplorerTree.NodeMouseDoubleClick += new System.Windows.Forms.TreeNodeMouseClickEventHandler(this.ExplorerTree_NodeMouseDoubleClick);
            // 
            // EditorExplorerLabel
            // 
            this.EditorExplorerLabel.AutoSize = true;
            this.EditorExplorerLabel.Dock = System.Windows.Forms.DockStyle.Left;
            this.EditorExplorerLabel.Font = new System.Drawing.Font("Consolas", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.EditorExplorerLabel.Location = new System.Drawing.Point(3, 0);
            this.EditorExplorerLabel.Name = "EditorExplorerLabel";
            this.EditorExplorerLabel.Size = new System.Drawing.Size(72, 22);
            this.EditorExplorerLabel.TabIndex = 1;
            this.EditorExplorerLabel.Text = "Explorer";
            this.EditorExplorerLabel.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            // 
            // ExplorerControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.SidebarPanel);
            this.Name = "ExplorerControl";
            this.Size = new System.Drawing.Size(164, 367);
            this.SidebarPanel.ResumeLayout(false);
            this.SidebarPanel.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion
        private System.Windows.Forms.ImageList ImageList;
        private System.Windows.Forms.TableLayoutPanel SidebarPanel;
        private System.Windows.Forms.TreeView ExplorerTree;
        private System.Windows.Forms.Label EditorExplorerLabel;
    }
}
