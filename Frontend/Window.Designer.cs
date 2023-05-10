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
            this.EditorSplitContainer = new System.Windows.Forms.SplitContainer();
            this.EditorTreeImageList = new System.Windows.Forms.ImageList(this.components);
            this.MainSplitContainer = new System.Windows.Forms.SplitContainer();
            this.EditorPanel = new System.Windows.Forms.Panel();
            this.SidebarPanel = new System.Windows.Forms.Panel();
            this.ResponsePanel = new System.Windows.Forms.Panel();
            this.Menu.SuspendLayout();
            this.strip.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.EditorSplitContainer)).BeginInit();
            this.EditorSplitContainer.Panel1.SuspendLayout();
            this.EditorSplitContainer.Panel2.SuspendLayout();
            this.EditorSplitContainer.SuspendLayout();
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
            this.EditorSplitContainer.Panel2.Controls.Add(this.EditorPanel);
            this.EditorSplitContainer.Size = new System.Drawing.Size(751, 268);
            this.EditorSplitContainer.SplitterDistance = 80;
            this.EditorSplitContainer.TabIndex = 3;
            // 
            // EditorTreeImageList
            // 
            this.EditorTreeImageList.ImageStream = ((System.Windows.Forms.ImageListStreamer)(resources.GetObject("EditorTreeImageList.ImageStream")));
            this.EditorTreeImageList.TransparentColor = System.Drawing.Color.Transparent;
            this.EditorTreeImageList.Images.SetKeyName(0, "folder.png");
            this.EditorTreeImageList.Images.SetKeyName(1, "file.png");
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
            this.MainSplitContainer.Panel2.Controls.Add(this.ResponsePanel);
            this.MainSplitContainer.Size = new System.Drawing.Size(758, 383);
            this.MainSplitContainer.SplitterDistance = 274;
            this.MainSplitContainer.TabIndex = 4;
            // 
            // EditorPanel
            // 
            this.EditorPanel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.EditorPanel.Location = new System.Drawing.Point(0, 0);
            this.EditorPanel.Name = "EditorPanel";
            this.EditorPanel.Size = new System.Drawing.Size(667, 268);
            this.EditorPanel.TabIndex = 0;
            // 
            // SidebarPanel
            // 
            this.SidebarPanel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.SidebarPanel.Location = new System.Drawing.Point(0, 0);
            this.SidebarPanel.Name = "SidebarPanel";
            this.SidebarPanel.Size = new System.Drawing.Size(80, 268);
            this.SidebarPanel.TabIndex = 0;
            // 
            // ResponsePanel
            // 
            this.ResponsePanel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.ResponsePanel.Location = new System.Drawing.Point(0, 0);
            this.ResponsePanel.Name = "ResponsePanel";
            this.ResponsePanel.Size = new System.Drawing.Size(758, 105);
            this.ResponsePanel.TabIndex = 0;
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
            this.EditorSplitContainer.Panel1.ResumeLayout(false);
            this.EditorSplitContainer.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.EditorSplitContainer)).EndInit();
            this.EditorSplitContainer.ResumeLayout(false);
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
        private System.Windows.Forms.SplitContainer EditorSplitContainer;
        private System.Windows.Forms.ToolStripMenuItem MenuOpenFolderOption;
        private System.Windows.Forms.ToolStripMenuItem MenuSaveFileOption;
        private System.Windows.Forms.SplitContainer MainSplitContainer;
        private System.Windows.Forms.ImageList EditorTreeImageList;
        private System.Windows.Forms.Panel SidebarPanel;
        private System.Windows.Forms.Panel EditorPanel;
        private System.Windows.Forms.Panel ResponsePanel;
    }
}

