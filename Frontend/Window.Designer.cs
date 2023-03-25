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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Window));
            this.menu = new System.Windows.Forms.MenuStrip();
            this.MenuFileOption = new System.Windows.Forms.ToolStripMenuItem();
            this.MenuNewFileOption = new System.Windows.Forms.ToolStripMenuItem();
            this.MenuOpenFileOption = new System.Windows.Forms.ToolStripMenuItem();
            this.MenuExitOption = new System.Windows.Forms.ToolStripMenuItem();
            this.strip = new System.Windows.Forms.ToolStrip();
            this.StripExecuteButton = new System.Windows.Forms.ToolStripButton();
            this.stripSeparator = new System.Windows.Forms.ToolStripSeparator();
            this.ResponseTabPanel = new System.Windows.Forms.TabControl();
            this.tabOutput = new System.Windows.Forms.TabPage();
            this.ResponseTabOutputText = new System.Windows.Forms.RichTextBox();
            this.tabMessages = new System.Windows.Forms.TabPage();
            this.ResponseTabMessagesText = new System.Windows.Forms.RichTextBox();
            this.EditorSplitContainer = new System.Windows.Forms.SplitContainer();
            this.EditorTabControl = new System.Windows.Forms.TabControl();
            this.EditorTab1 = new System.Windows.Forms.TabPage();
            this.EditorTab1Text = new System.Windows.Forms.RichTextBox();
            this.EditorTree = new System.Windows.Forms.TreeView();
            this.MenuOpenFolderOption = new System.Windows.Forms.ToolStripMenuItem();
            this.EditorExplorerLabel = new System.Windows.Forms.Label();
            this.menu.SuspendLayout();
            this.strip.SuspendLayout();
            this.ResponseTabPanel.SuspendLayout();
            this.tabOutput.SuspendLayout();
            this.tabMessages.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.EditorSplitContainer)).BeginInit();
            this.EditorSplitContainer.Panel1.SuspendLayout();
            this.EditorSplitContainer.Panel2.SuspendLayout();
            this.EditorSplitContainer.SuspendLayout();
            this.EditorTabControl.SuspendLayout();
            this.EditorTab1.SuspendLayout();
            this.SuspendLayout();
            // 
            // menu
            // 
            this.menu.ImageScalingSize = new System.Drawing.Size(20, 20);
            this.menu.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.MenuFileOption});
            this.menu.Location = new System.Drawing.Point(0, 0);
            this.menu.Name = "menu";
            this.menu.Size = new System.Drawing.Size(782, 28);
            this.menu.TabIndex = 0;
            this.menu.Text = "menuStrip1";
            // 
            // MenuFileOption
            // 
            this.MenuFileOption.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.MenuNewFileOption,
            this.MenuOpenFileOption,
            this.MenuOpenFolderOption,
            this.MenuExitOption});
            this.MenuFileOption.Name = "MenuFileOption";
            this.MenuFileOption.Size = new System.Drawing.Size(46, 24);
            this.MenuFileOption.Text = "File";
            // 
            // MenuNewFileOption
            // 
            this.MenuNewFileOption.Name = "MenuNewFileOption";
            this.MenuNewFileOption.Size = new System.Drawing.Size(224, 26);
            this.MenuNewFileOption.Text = "New file";
            this.MenuNewFileOption.Click += new System.EventHandler(this.MenuNewFileOption_Click);
            // 
            // MenuOpenFileOption
            // 
            this.MenuOpenFileOption.Name = "MenuOpenFileOption";
            this.MenuOpenFileOption.Size = new System.Drawing.Size(224, 26);
            this.MenuOpenFileOption.Text = "Open file";
            this.MenuOpenFileOption.Click += new System.EventHandler(this.MenuOpenFileOption_Click);
            // 
            // MenuExitOption
            // 
            this.MenuExitOption.Name = "MenuExitOption";
            this.MenuExitOption.Size = new System.Drawing.Size(224, 26);
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
            this.ResponseTabPanel.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.ResponseTabPanel.Controls.Add(this.tabOutput);
            this.ResponseTabPanel.Controls.Add(this.tabMessages);
            this.ResponseTabPanel.Location = new System.Drawing.Point(12, 313);
            this.ResponseTabPanel.Name = "ResponseTabPanel";
            this.ResponseTabPanel.SelectedIndex = 0;
            this.ResponseTabPanel.Size = new System.Drawing.Size(758, 133);
            this.ResponseTabPanel.TabIndex = 2;
            // 
            // tabOutput
            // 
            this.tabOutput.Controls.Add(this.ResponseTabOutputText);
            this.tabOutput.Location = new System.Drawing.Point(4, 25);
            this.tabOutput.Name = "tabOutput";
            this.tabOutput.Padding = new System.Windows.Forms.Padding(3);
            this.tabOutput.Size = new System.Drawing.Size(750, 104);
            this.tabOutput.TabIndex = 0;
            this.tabOutput.Text = "Output";
            this.tabOutput.UseVisualStyleBackColor = true;
            // 
            // ResponseTabOutputText
            // 
            this.ResponseTabOutputText.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.ResponseTabOutputText.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.ResponseTabOutputText.Location = new System.Drawing.Point(0, 0);
            this.ResponseTabOutputText.Name = "ResponseTabOutputText";
            this.ResponseTabOutputText.ReadOnly = true;
            this.ResponseTabOutputText.Size = new System.Drawing.Size(754, 104);
            this.ResponseTabOutputText.TabIndex = 0;
            this.ResponseTabOutputText.Text = "";
            // 
            // tabMessages
            // 
            this.tabMessages.Controls.Add(this.ResponseTabMessagesText);
            this.tabMessages.Location = new System.Drawing.Point(4, 25);
            this.tabMessages.Name = "tabMessages";
            this.tabMessages.Padding = new System.Windows.Forms.Padding(3);
            this.tabMessages.Size = new System.Drawing.Size(750, 104);
            this.tabMessages.TabIndex = 1;
            this.tabMessages.Text = "Messages";
            this.tabMessages.UseVisualStyleBackColor = true;
            // 
            // ResponseTabMessagesText
            // 
            this.ResponseTabMessagesText.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.ResponseTabMessagesText.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.ResponseTabMessagesText.Location = new System.Drawing.Point(0, 0);
            this.ResponseTabMessagesText.Name = "ResponseTabMessagesText";
            this.ResponseTabMessagesText.ReadOnly = true;
            this.ResponseTabMessagesText.Size = new System.Drawing.Size(750, 108);
            this.ResponseTabMessagesText.TabIndex = 0;
            this.ResponseTabMessagesText.Text = "";
            // 
            // EditorSplitContainer
            // 
            this.EditorSplitContainer.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.EditorSplitContainer.Location = new System.Drawing.Point(12, 58);
            this.EditorSplitContainer.Name = "EditorSplitContainer";
            // 
            // EditorSplitContainer.Panel1
            // 
            this.EditorSplitContainer.Panel1.Controls.Add(this.EditorExplorerLabel);
            this.EditorSplitContainer.Panel1.Controls.Add(this.EditorTree);
            // 
            // EditorSplitContainer.Panel2
            // 
            this.EditorSplitContainer.Panel2.Controls.Add(this.EditorTabControl);
            this.EditorSplitContainer.Size = new System.Drawing.Size(758, 249);
            this.EditorSplitContainer.SplitterDistance = 75;
            this.EditorSplitContainer.TabIndex = 3;
            // 
            // EditorTabControl
            // 
            this.EditorTabControl.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.EditorTabControl.Controls.Add(this.EditorTab1);
            this.EditorTabControl.Location = new System.Drawing.Point(3, 3);
            this.EditorTabControl.Name = "EditorTabControl";
            this.EditorTabControl.SelectedIndex = 0;
            this.EditorTabControl.Size = new System.Drawing.Size(673, 246);
            this.EditorTabControl.TabIndex = 0;
            // 
            // EditorTab1
            // 
            this.EditorTab1.Controls.Add(this.EditorTab1Text);
            this.EditorTab1.Location = new System.Drawing.Point(4, 25);
            this.EditorTab1.Name = "EditorTab1";
            this.EditorTab1.Padding = new System.Windows.Forms.Padding(3);
            this.EditorTab1.Size = new System.Drawing.Size(665, 217);
            this.EditorTab1.TabIndex = 0;
            this.EditorTab1.Text = "path";
            this.EditorTab1.UseVisualStyleBackColor = true;
            // 
            // EditorTab1Text
            // 
            this.EditorTab1Text.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.EditorTab1Text.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.EditorTab1Text.Location = new System.Drawing.Point(0, 0);
            this.EditorTab1Text.Name = "EditorTab1Text";
            this.EditorTab1Text.Size = new System.Drawing.Size(672, 221);
            this.EditorTab1Text.TabIndex = 0;
            this.EditorTab1Text.Text = "";
            // 
            // EditorTree
            // 
            this.EditorTree.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.EditorTree.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.EditorTree.Location = new System.Drawing.Point(4, 28);
            this.EditorTree.Name = "EditorTree";
            this.EditorTree.Size = new System.Drawing.Size(68, 217);
            this.EditorTree.TabIndex = 0;
            // 
            // MenuOpenFolderOption
            // 
            this.MenuOpenFolderOption.Name = "MenuOpenFolderOption";
            this.MenuOpenFolderOption.Size = new System.Drawing.Size(224, 26);
            this.MenuOpenFolderOption.Text = "Open folder";
            this.MenuOpenFolderOption.Click += new System.EventHandler(this.MenuOpenFolderOption_Click);
            // 
            // EditorExplorerLabel
            // 
            this.EditorExplorerLabel.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.EditorExplorerLabel.AutoSize = true;
            this.EditorExplorerLabel.Location = new System.Drawing.Point(4, 6);
            this.EditorExplorerLabel.Name = "EditorExplorerLabel";
            this.EditorExplorerLabel.Size = new System.Drawing.Size(57, 16);
            this.EditorExplorerLabel.TabIndex = 1;
            this.EditorExplorerLabel.Text = "Explorer";
            this.EditorExplorerLabel.TextAlign = System.Drawing.ContentAlignment.MiddleLeft;
            // 
            // Window
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(120F, 120F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Dpi;
            this.ClientSize = new System.Drawing.Size(782, 453);
            this.Controls.Add(this.EditorSplitContainer);
            this.Controls.Add(this.ResponseTabPanel);
            this.Controls.Add(this.strip);
            this.Controls.Add(this.menu);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.MainMenuStrip = this.menu;
            this.Name = "Window";
            this.Text = "Datavo DBMS";
            this.Load += new System.EventHandler(this.Window_Load);
            this.menu.ResumeLayout(false);
            this.menu.PerformLayout();
            this.strip.ResumeLayout(false);
            this.strip.PerformLayout();
            this.ResponseTabPanel.ResumeLayout(false);
            this.tabOutput.ResumeLayout(false);
            this.tabMessages.ResumeLayout(false);
            this.EditorSplitContainer.Panel1.ResumeLayout(false);
            this.EditorSplitContainer.Panel1.PerformLayout();
            this.EditorSplitContainer.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.EditorSplitContainer)).EndInit();
            this.EditorSplitContainer.ResumeLayout(false);
            this.EditorTabControl.ResumeLayout(false);
            this.EditorTab1.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip menu;
        private System.Windows.Forms.ToolStripMenuItem MenuFileOption;
        private System.Windows.Forms.ToolStripMenuItem MenuNewFileOption;
        private System.Windows.Forms.ToolStripMenuItem MenuOpenFileOption;
        private System.Windows.Forms.ToolStripMenuItem MenuExitOption;
        private System.Windows.Forms.ToolStrip strip;
        private System.Windows.Forms.ToolStripButton StripExecuteButton;
        private System.Windows.Forms.ToolStripSeparator stripSeparator;
        private System.Windows.Forms.TabControl ResponseTabPanel;
        private System.Windows.Forms.TabPage tabOutput;
        private System.Windows.Forms.TabPage tabMessages;
        private System.Windows.Forms.RichTextBox ResponseTabOutputText;
        private System.Windows.Forms.RichTextBox ResponseTabMessagesText;
        private System.Windows.Forms.SplitContainer EditorSplitContainer;
        private System.Windows.Forms.TabControl EditorTabControl;
        private System.Windows.Forms.TabPage EditorTab1;
        private System.Windows.Forms.RichTextBox EditorTab1Text;
        private System.Windows.Forms.TreeView EditorTree;
        private System.Windows.Forms.ToolStripMenuItem MenuOpenFolderOption;
        private System.Windows.Forms.Label EditorExplorerLabel;
    }
}

