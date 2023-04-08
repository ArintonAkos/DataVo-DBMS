namespace Frontend
{
    partial class Window
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

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Window));
            System.Windows.Forms.TreeNode treeNode1 = new System.Windows.Forms.TreeNode("Open folder");
            this.Menu = new System.Windows.Forms.MenuStrip();
            this.MenuFileOption = new System.Windows.Forms.ToolStripMenuItem();
            this.MenuNewFileOption = new System.Windows.Forms.ToolStripMenuItem();
            this.MenuOpenFileOption = new System.Windows.Forms.ToolStripMenuItem();
            this.MenuSaveFileOption = new System.Windows.Forms.ToolStripMenuItem();
            this.MenuOpenFolderOption = new System.Windows.Forms.ToolStripMenuItem();
            this.MenuExitOption = new System.Windows.Forms.ToolStripMenuItem();
            this.strip = new System.Windows.Forms.ToolStrip();
            this.StripExecuteButton = new System.Windows.Forms.ToolStripButton();
            this.stripSeparator = new System.Windows.Forms.ToolStripSeparator();
            this.ResponseTabPanel = new System.Windows.Forms.TabControl();
            this.ResponseTabOutput = new System.Windows.Forms.TabPage();
            this.ResponseTabOutputText = new System.Windows.Forms.RichTextBox();
            this.ResponseTabMessages = new System.Windows.Forms.TabPage();
            this.ResponseTabMessagesText = new System.Windows.Forms.RichTextBox();
            this.EditorSplitContainer = new System.Windows.Forms.SplitContainer();
            this.SidebarPanel = new System.Windows.Forms.TableLayoutPanel();
            this.EditorTree = new System.Windows.Forms.TreeView();
            this.EditorExplorerLabel = new System.Windows.Forms.Label();
            this.EditorTabControl = new System.Windows.Forms.TabControl();
            this.EditorTab1 = new System.Windows.Forms.TabPage();
            this.EditorTab1Text = new System.Windows.Forms.RichTextBox();
            this.MainSplitContainer = new System.Windows.Forms.SplitContainer();
            this.EditorTreeImageList = new System.Windows.Forms.ImageList(this.components);
            this.Menu.SuspendLayout();
            this.strip.SuspendLayout();
            this.ResponseTabPanel.SuspendLayout();
            this.ResponseTabOutput.SuspendLayout();
            this.ResponseTabMessages.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.EditorSplitContainer)).BeginInit();
            this.EditorSplitContainer.Panel1.SuspendLayout();
            this.EditorSplitContainer.Panel2.SuspendLayout();
            this.EditorSplitContainer.SuspendLayout();
            this.SidebarPanel.SuspendLayout();
            this.EditorTabControl.SuspendLayout();
            this.EditorTab1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.MainSplitContainer)).BeginInit();
            this.MainSplitContainer.Panel1.SuspendLayout();
            this.MainSplitContainer.Panel2.SuspendLayout();
            this.MainSplitContainer.SuspendLayout();
            this.SuspendLayout();
            // 
            // Menu
            // 
            this.Menu.ImageScalingSize = new System.Drawing.Size(20, 20);
            this.Menu.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.MenuFileOption});
            this.Menu.Location = new System.Drawing.Point(0, 0);
            this.Menu.Name = "Menu";
            this.Menu.Size = new System.Drawing.Size(782, 28);
            this.Menu.TabIndex = 0;
            this.Menu.Text = "menuStrip1";
            // 
            // MenuFileOption
            // 
            this.MenuFileOption.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.MenuNewFileOption,
            this.MenuOpenFileOption,
            this.MenuSaveFileOption,
            this.MenuOpenFolderOption,
            this.MenuExitOption});
            this.MenuFileOption.Name = "MenuFileOption";
            this.MenuFileOption.Size = new System.Drawing.Size(46, 24);
            this.MenuFileOption.Text = "File";
            // 
            // MenuNewFileOption
            // 
            this.MenuNewFileOption.Name = "MenuNewFileOption";
            this.MenuNewFileOption.Size = new System.Drawing.Size(172, 26);
            this.MenuNewFileOption.Text = "New file";
            this.MenuNewFileOption.Click += new System.EventHandler(this.MenuNewFileOption_Click);
            // 
            // MenuOpenFileOption
            // 
            this.MenuOpenFileOption.Name = "MenuOpenFileOption";
            this.MenuOpenFileOption.Size = new System.Drawing.Size(172, 26);
            this.MenuOpenFileOption.Text = "Open file";
            this.MenuOpenFileOption.Click += new System.EventHandler(this.MenuOpenFileOption_Click);
            // 
            // MenuSaveFileOption
            // 
            this.MenuSaveFileOption.Name = "MenuSaveFileOption";
            this.MenuSaveFileOption.Size = new System.Drawing.Size(172, 26);
            this.MenuSaveFileOption.Text = "Save file";
            this.MenuSaveFileOption.Click += new System.EventHandler(this.MenuSaveFileOption_Click);
            // 
            // MenuOpenFolderOption
            // 
            this.MenuOpenFolderOption.Name = "MenuOpenFolderOption";
            this.MenuOpenFolderOption.Size = new System.Drawing.Size(172, 26);
            this.MenuOpenFolderOption.Text = "Open folder";
            this.MenuOpenFolderOption.Click += new System.EventHandler(this.MenuOpenFolderOption_Click);
            // 
            // MenuExitOption
            // 
            this.MenuExitOption.Name = "MenuExitOption";
            this.MenuExitOption.Size = new System.Drawing.Size(172, 26);
            this.MenuExitOption.Text = "Exit";
            this.MenuExitOption.Click += new System.EventHandler(this.MenuExitOption_Click);
            // 
            // strip
            // 
            this.strip.ImageScalingSize = new System.Drawing.Size(20, 20);
            this.strip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.StripExecuteButton,
            this.stripSeparator});
            this.strip.Location = new System.Drawing.Point(0, 28);
            this.strip.Name = "strip";
            this.strip.Size = new System.Drawing.Size(782, 27);
            this.strip.TabIndex = 1;
            this.strip.Text = "toolStrip1";
            // 
            // StripExecuteButton
            // 
            this.StripExecuteButton.Image = ((System.Drawing.Image)(resources.GetObject("StripExecuteButton.Image")));
            this.StripExecuteButton.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.StripExecuteButton.Name = "StripExecuteButton";
            this.StripExecuteButton.Size = new System.Drawing.Size(84, 24);
            this.StripExecuteButton.Text = "Execute";
            this.StripExecuteButton.Click += new System.EventHandler(this.StripExecuteButton_Click);
            // 
            // stripSeparator
            // 
            this.stripSeparator.Name = "stripSeparator";
            this.stripSeparator.Size = new System.Drawing.Size(6, 27);
            // 
            // ResponseTabPanel
            // 
            this.ResponseTabPanel.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.ResponseTabPanel.Controls.Add(this.ResponseTabOutput);
            this.ResponseTabPanel.Controls.Add(this.ResponseTabMessages);
            this.ResponseTabPanel.Location = new System.Drawing.Point(3, 3);
            this.ResponseTabPanel.Name = "ResponseTabPanel";
            this.ResponseTabPanel.SelectedIndex = 0;
            this.ResponseTabPanel.Size = new System.Drawing.Size(752, 99);
            this.ResponseTabPanel.TabIndex = 2;
            // 
            // ResponseTabOutput
            // 
            this.ResponseTabOutput.Controls.Add(this.ResponseTabOutputText);
            this.ResponseTabOutput.Location = new System.Drawing.Point(4, 27);
            this.ResponseTabOutput.Name = "ResponseTabOutput";
            this.ResponseTabOutput.Padding = new System.Windows.Forms.Padding(3);
            this.ResponseTabOutput.Size = new System.Drawing.Size(744, 68);
            this.ResponseTabOutput.TabIndex = 0;
            this.ResponseTabOutput.Text = "Output";
            this.ResponseTabOutput.UseVisualStyleBackColor = true;
            // 
            // ResponseTabOutputText
            // 
            this.ResponseTabOutputText.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.ResponseTabOutputText.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ResponseTabOutputText.Location = new System.Drawing.Point(3, 3);
            this.ResponseTabOutputText.Name = "ResponseTabOutputText";
            this.ResponseTabOutputText.ReadOnly = true;
            this.ResponseTabOutputText.Size = new System.Drawing.Size(738, 62);
            this.ResponseTabOutputText.TabIndex = 0;
            this.ResponseTabOutputText.Text = "";
            // 
            // ResponseTabMessages
            // 
            this.ResponseTabMessages.Controls.Add(this.ResponseTabMessagesText);
            this.ResponseTabMessages.Location = new System.Drawing.Point(4, 27);
            this.ResponseTabMessages.Name = "ResponseTabMessages";
            this.ResponseTabMessages.Padding = new System.Windows.Forms.Padding(3);
            this.ResponseTabMessages.Size = new System.Drawing.Size(744, 68);
            this.ResponseTabMessages.TabIndex = 1;
            this.ResponseTabMessages.Text = "Messages";
            this.ResponseTabMessages.UseVisualStyleBackColor = true;
            // 
            // ResponseTabMessagesText
            // 
            this.ResponseTabMessagesText.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.ResponseTabMessagesText.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ResponseTabMessagesText.Location = new System.Drawing.Point(3, 3);
            this.ResponseTabMessagesText.Name = "ResponseTabMessagesText";
            this.ResponseTabMessagesText.ReadOnly = true;
            this.ResponseTabMessagesText.Size = new System.Drawing.Size(738, 62);
            this.ResponseTabMessagesText.TabIndex = 0;
            this.ResponseTabMessagesText.Text = "";
            // 
            // EditorSplitContainer
            // 
            this.EditorSplitContainer.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.EditorSplitContainer.Location = new System.Drawing.Point(3, 3);
            this.EditorSplitContainer.Name = "EditorSplitContainer";
            // 
            // EditorSplitContainer.Panel1
            // 
            this.EditorSplitContainer.Panel1.Controls.Add(this.SidebarPanel);
            // 
            // EditorSplitContainer.Panel2
            // 
            this.EditorSplitContainer.Panel2.Controls.Add(this.EditorTabControl);
            this.EditorSplitContainer.Size = new System.Drawing.Size(751, 268);
            this.EditorSplitContainer.SplitterDistance = 80;
            this.EditorSplitContainer.TabIndex = 3;
            // 
            // SidebarPanel
            // 
            this.SidebarPanel.ColumnCount = 1;
            this.SidebarPanel.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.SidebarPanel.Controls.Add(this.EditorTree, 0, 1);
            this.SidebarPanel.Controls.Add(this.EditorExplorerLabel, 0, 0);
            this.SidebarPanel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.SidebarPanel.Location = new System.Drawing.Point(0, 0);
            this.SidebarPanel.Name = "SidebarPanel";
            this.SidebarPanel.RowCount = 2;
            this.SidebarPanel.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Absolute, 22F));
            this.SidebarPanel.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.SidebarPanel.Size = new System.Drawing.Size(80, 268);
            this.SidebarPanel.TabIndex = 1;
            // 
            // EditorTree
            // 
            this.EditorTree.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.EditorTree.Dock = System.Windows.Forms.DockStyle.Fill;
            this.EditorTree.ImageIndex = 0;
            this.EditorTree.ImageList = this.EditorTreeImageList;
            this.EditorTree.Location = new System.Drawing.Point(3, 25);
            this.EditorTree.Name = "EditorTree";
            treeNode1.Name = "EditorTreeOpenFolder";
            treeNode1.Tag = "None";
            treeNode1.Text = "Open folder";
            this.EditorTree.Nodes.AddRange(new System.Windows.Forms.TreeNode[] {
            treeNode1});
            this.EditorTree.SelectedImageIndex = 0;
            this.EditorTree.Size = new System.Drawing.Size(74, 240);
            this.EditorTree.TabIndex = 0;
            this.EditorTree.NodeMouseDoubleClick += new System.Windows.Forms.TreeNodeMouseClickEventHandler(this.EditorTree_NodeMouseDoubleClick);
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
            // EditorTabControl
            // 
            this.EditorTabControl.Controls.Add(this.EditorTab1);
            this.EditorTabControl.Dock = System.Windows.Forms.DockStyle.Fill;
            this.EditorTabControl.Location = new System.Drawing.Point(0, 0);
            this.EditorTabControl.Name = "EditorTabControl";
            this.EditorTabControl.SelectedIndex = 0;
            this.EditorTabControl.Size = new System.Drawing.Size(667, 268);
            this.EditorTabControl.TabIndex = 0;
            this.EditorTabControl.KeyDown += new System.Windows.Forms.KeyEventHandler(this.EditorTabControl_KeyDown);
            this.EditorTabControl.MouseClick += new System.Windows.Forms.MouseEventHandler(this.EditorTabPage_MouseClicked);
            // 
            // EditorTab1
            // 
            this.EditorTab1.Controls.Add(this.EditorTab1Text);
            this.EditorTab1.Location = new System.Drawing.Point(4, 27);
            this.EditorTab1.Name = "EditorTab1";
            this.EditorTab1.Padding = new System.Windows.Forms.Padding(3);
            this.EditorTab1.Size = new System.Drawing.Size(659, 237);
            this.EditorTab1.TabIndex = 0;
            this.EditorTab1.Text = "path";
            this.EditorTab1.UseVisualStyleBackColor = true;
            // 
            // EditorTab1Text
            // 
            this.EditorTab1Text.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.EditorTab1Text.Dock = System.Windows.Forms.DockStyle.Fill;
            this.EditorTab1Text.Location = new System.Drawing.Point(3, 3);
            this.EditorTab1Text.Name = "EditorTab1Text";
            this.EditorTab1Text.Size = new System.Drawing.Size(653, 231);
            this.EditorTab1Text.TabIndex = 0;
            this.EditorTab1Text.Text = "";
            // 
            // MainSplitContainer
            // 
            this.MainSplitContainer.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.MainSplitContainer.Location = new System.Drawing.Point(12, 58);
            this.MainSplitContainer.Name = "MainSplitContainer";
            this.MainSplitContainer.Orientation = System.Windows.Forms.Orientation.Horizontal;
            // 
            // MainSplitContainer.Panel1
            // 
            this.MainSplitContainer.Panel1.Controls.Add(this.EditorSplitContainer);
            // 
            // MainSplitContainer.Panel2
            // 
            this.MainSplitContainer.Panel2.Controls.Add(this.ResponseTabPanel);
            this.MainSplitContainer.Size = new System.Drawing.Size(758, 383);
            this.MainSplitContainer.SplitterDistance = 274;
            this.MainSplitContainer.TabIndex = 4;
            // 
            // EditorTreeImageList
            // 
            this.EditorTreeImageList.ImageStream = ((System.Windows.Forms.ImageListStreamer)(resources.GetObject("EditorTreeImageList.ImageStream")));
            this.EditorTreeImageList.TransparentColor = System.Drawing.Color.Transparent;
            this.EditorTreeImageList.Images.SetKeyName(0, "folder.png");
            this.EditorTreeImageList.Images.SetKeyName(1, "file.png");
            // 
            // Window
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(120F, 120F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Dpi;
            this.ClientSize = new System.Drawing.Size(782, 453);
            this.Controls.Add(this.MainSplitContainer);
            this.Controls.Add(this.strip);
            this.Controls.Add(this.Menu);
            this.Font = new System.Drawing.Font("Consolas", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Name = "Window";
            this.Text = "Datavo DBMS";
            this.Load += new System.EventHandler(this.Window_Load);
            this.Menu.ResumeLayout(false);
            this.Menu.PerformLayout();
            this.strip.ResumeLayout(false);
            this.strip.PerformLayout();
            this.ResponseTabPanel.ResumeLayout(false);
            this.ResponseTabOutput.ResumeLayout(false);
            this.ResponseTabMessages.ResumeLayout(false);
            this.EditorSplitContainer.Panel1.ResumeLayout(false);
            this.EditorSplitContainer.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.EditorSplitContainer)).EndInit();
            this.EditorSplitContainer.ResumeLayout(false);
            this.SidebarPanel.ResumeLayout(false);
            this.SidebarPanel.PerformLayout();
            this.EditorTabControl.ResumeLayout(false);
            this.EditorTab1.ResumeLayout(false);
            this.MainSplitContainer.Panel1.ResumeLayout(false);
            this.MainSplitContainer.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.MainSplitContainer)).EndInit();
            this.MainSplitContainer.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip Menu;
        private System.Windows.Forms.ToolStripMenuItem MenuFileOption;
        private System.Windows.Forms.ToolStripMenuItem MenuNewFileOption;
        private System.Windows.Forms.ToolStripMenuItem MenuOpenFileOption;
        private System.Windows.Forms.ToolStripMenuItem MenuExitOption;
        private System.Windows.Forms.ToolStrip strip;
        private System.Windows.Forms.ToolStripButton StripExecuteButton;
        private System.Windows.Forms.ToolStripSeparator stripSeparator;
        private System.Windows.Forms.TabControl ResponseTabPanel;
        private System.Windows.Forms.TabPage ResponseTabOutput;
        private System.Windows.Forms.TabPage ResponseTabMessages;
        private System.Windows.Forms.RichTextBox ResponseTabOutputText;
        private System.Windows.Forms.SplitContainer EditorSplitContainer;
        private System.Windows.Forms.TabControl EditorTabControl;
        private System.Windows.Forms.TabPage EditorTab1;
        private System.Windows.Forms.RichTextBox EditorTab1Text;
        private System.Windows.Forms.TreeView EditorTree;
        private System.Windows.Forms.ToolStripMenuItem MenuOpenFolderOption;
        private System.Windows.Forms.Label EditorExplorerLabel;
        private System.Windows.Forms.ToolStripMenuItem MenuSaveFileOption;
        private System.Windows.Forms.SplitContainer MainSplitContainer;
        private System.Windows.Forms.RichTextBox ResponseTabMessagesText;
        private System.Windows.Forms.TableLayoutPanel SidebarPanel;
        private System.Windows.Forms.ImageList EditorTreeImageList;
    }
}

