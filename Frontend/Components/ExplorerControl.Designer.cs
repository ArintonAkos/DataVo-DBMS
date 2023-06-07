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
            components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(ExplorerControl));
            TreeNode treeNode1 = new TreeNode("Open folder");
            TreeNode treeNode2 = new TreeNode("Databases", 2, 2);
            ImageList = new ImageList(components);
            SidebarPanel = new TableLayoutPanel();
            ExplorerTree = new TreeView();
            EditorExplorerLabel = new Label();
            SidebarPanel.SuspendLayout();
            SuspendLayout();
            // 
            // ImageList
            // 
            ImageList.ColorDepth = ColorDepth.Depth8Bit;
            ImageList.ImageStream = (ImageListStreamer)resources.GetObject("ImageList.ImageStream");
            ImageList.TransparentColor = Color.Transparent;
            ImageList.Images.SetKeyName(0, "folder.png");
            ImageList.Images.SetKeyName(1, "file.png");
            ImageList.Images.SetKeyName(2, "database.png");
            ImageList.Images.SetKeyName(3, "primaryKey.png");
            ImageList.Images.SetKeyName(4, "foreignKey.png");
            ImageList.Images.SetKeyName(5, "uniqueKey.png");
            ImageList.Images.SetKeyName(6, "indexFile.png");
            ImageList.Images.SetKeyName(7, "table.png");
            ImageList.Images.SetKeyName(8, "column.png");
            // 
            // SidebarPanel
            // 
            SidebarPanel.ColumnCount = 1;
            SidebarPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
            SidebarPanel.Controls.Add(ExplorerTree, 0, 1);
            SidebarPanel.Controls.Add(EditorExplorerLabel, 0, 0);
            SidebarPanel.Dock = DockStyle.Fill;
            SidebarPanel.Location = new Point(0, 0);
            SidebarPanel.Margin = new Padding(3, 4, 3, 4);
            SidebarPanel.Name = "SidebarPanel";
            SidebarPanel.RowCount = 2;
            SidebarPanel.RowStyles.Add(new RowStyle(SizeType.Absolute, 28F));
            SidebarPanel.RowStyles.Add(new RowStyle());
            SidebarPanel.Size = new Size(164, 459);
            SidebarPanel.TabIndex = 2;
            // 
            // ExplorerTree
            // 
            ExplorerTree.BorderStyle = BorderStyle.None;
            ExplorerTree.Dock = DockStyle.Fill;
            ExplorerTree.ImageIndex = 0;
            ExplorerTree.ImageList = ImageList;
            ExplorerTree.Location = new Point(3, 32);
            ExplorerTree.Margin = new Padding(3, 4, 3, 4);
            ExplorerTree.Name = "ExplorerTree";
            treeNode1.Name = "openFolder";
            treeNode1.Tag = "None";
            treeNode1.Text = "Open folder";
            treeNode2.ImageIndex = 2;
            treeNode2.Name = "databases";
            treeNode2.SelectedImageIndex = 2;
            treeNode2.Text = "Databases";
            ExplorerTree.Nodes.AddRange(new TreeNode[] { treeNode1, treeNode2 });
            ExplorerTree.SelectedImageIndex = 0;
            ExplorerTree.Size = new Size(158, 424);
            ExplorerTree.TabIndex = 0;
            ExplorerTree.NodeMouseDoubleClick += ExplorerTree_NodeMouseDoubleClick;
            // 
            // EditorExplorerLabel
            // 
            EditorExplorerLabel.AutoSize = true;
            EditorExplorerLabel.Dock = DockStyle.Left;
            EditorExplorerLabel.Font = new Font("Consolas", 9F, FontStyle.Regular, GraphicsUnit.Point);
            EditorExplorerLabel.Location = new Point(3, 0);
            EditorExplorerLabel.Name = "EditorExplorerLabel";
            EditorExplorerLabel.Size = new Size(72, 28);
            EditorExplorerLabel.TabIndex = 1;
            EditorExplorerLabel.Text = "Explorer";
            EditorExplorerLabel.TextAlign = ContentAlignment.MiddleLeft;
            // 
            // ExplorerControl
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            Controls.Add(SidebarPanel);
            Margin = new Padding(3, 4, 3, 4);
            Name = "ExplorerControl";
            Size = new Size(164, 459);
            SidebarPanel.ResumeLayout(false);
            SidebarPanel.PerformLayout();
            ResumeLayout(false);
        }

        #endregion
        private ImageList ImageList;
        private TableLayoutPanel SidebarPanel;
        private TreeView ExplorerTree;
        private Label EditorExplorerLabel;
    }
}
