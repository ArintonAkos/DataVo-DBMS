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
            this.menuFile = new System.Windows.Forms.ToolStripMenuItem();
            this.menuNewFile = new System.Windows.Forms.ToolStripMenuItem();
            this.menuOpenFile = new System.Windows.Forms.ToolStripMenuItem();
            this.menuExit = new System.Windows.Forms.ToolStripMenuItem();
            this.strip = new System.Windows.Forms.ToolStrip();
            this.stripExecute = new System.Windows.Forms.ToolStripButton();
            this.stripSeparator = new System.Windows.Forms.ToolStripSeparator();
            this.stripFilenameLabel = new System.Windows.Forms.ToolStripLabel();
            this.tabPanel = new System.Windows.Forms.TabControl();
            this.tabOutput = new System.Windows.Forms.TabPage();
            this.tabMessages = new System.Windows.Forms.TabPage();
            this.textEditor = new System.Windows.Forms.RichTextBox();
            this.menuSaveFile = new System.Windows.Forms.ToolStripMenuItem();
            this.tabOutputText = new System.Windows.Forms.RichTextBox();
            this.tabMessagesText = new System.Windows.Forms.RichTextBox();
            this.menu.SuspendLayout();
            this.strip.SuspendLayout();
            this.tabPanel.SuspendLayout();
            this.tabOutput.SuspendLayout();
            this.tabMessages.SuspendLayout();
            this.SuspendLayout();
            // 
            // menu
            // 
            this.menu.ImageScalingSize = new System.Drawing.Size(20, 20);
            this.menu.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.menuFile});
            this.menu.Location = new System.Drawing.Point(0, 0);
            this.menu.Name = "menu";
            this.menu.Size = new System.Drawing.Size(782, 28);
            this.menu.TabIndex = 0;
            this.menu.Text = "menuStrip1";
            // 
            // menuFile
            // 
            this.menuFile.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.menuNewFile,
            this.menuOpenFile,
            this.menuSaveFile,
            this.menuExit});
            this.menuFile.Name = "menuFile";
            this.menuFile.Size = new System.Drawing.Size(46, 24);
            this.menuFile.Text = "File";
            // 
            // menuNewFile
            // 
            this.menuNewFile.Name = "menuNewFile";
            this.menuNewFile.Size = new System.Drawing.Size(224, 26);
            this.menuNewFile.Text = "New file";
            this.menuNewFile.Click += new System.EventHandler(this.menuNewFile_Click);
            // 
            // menuOpenFile
            // 
            this.menuOpenFile.Name = "menuOpenFile";
            this.menuOpenFile.Size = new System.Drawing.Size(224, 26);
            this.menuOpenFile.Text = "Open file";
            this.menuOpenFile.Click += new System.EventHandler(this.menuOpenFile_Click);
            // 
            // menuExit
            // 
            this.menuExit.Name = "menuExit";
            this.menuExit.Size = new System.Drawing.Size(224, 26);
            this.menuExit.Text = "Exit";
            this.menuExit.Click += new System.EventHandler(this.menuExit_Click);
            // 
            // strip
            // 
            this.strip.ImageScalingSize = new System.Drawing.Size(20, 20);
            this.strip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.stripExecute,
            this.stripSeparator,
            this.stripFilenameLabel});
            this.strip.Location = new System.Drawing.Point(0, 28);
            this.strip.Name = "strip";
            this.strip.Size = new System.Drawing.Size(782, 27);
            this.strip.TabIndex = 1;
            this.strip.Text = "toolStrip1";
            // 
            // stripExecute
            // 
            this.stripExecute.Image = ((System.Drawing.Image)(resources.GetObject("stripExecute.Image")));
            this.stripExecute.ImageTransparentColor = System.Drawing.Color.Magenta;
            this.stripExecute.Name = "stripExecute";
            this.stripExecute.Size = new System.Drawing.Size(84, 24);
            this.stripExecute.Text = "Execute";
            this.stripExecute.Click += new System.EventHandler(this.stripExecute_Click);
            // 
            // stripSeparator
            // 
            this.stripSeparator.Name = "stripSeparator";
            this.stripSeparator.Size = new System.Drawing.Size(6, 27);
            // 
            // stripFilenameLabel
            // 
            this.stripFilenameLabel.Name = "stripFilenameLabel";
            this.stripFilenameLabel.Size = new System.Drawing.Size(69, 24);
            this.stripFilenameLabel.Text = "./filepath";
            // 
            // tabPanel
            // 
            this.tabPanel.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.tabPanel.Controls.Add(this.tabOutput);
            this.tabPanel.Controls.Add(this.tabMessages);
            this.tabPanel.Location = new System.Drawing.Point(12, 313);
            this.tabPanel.Name = "tabPanel";
            this.tabPanel.SelectedIndex = 0;
            this.tabPanel.Size = new System.Drawing.Size(758, 133);
            this.tabPanel.TabIndex = 2;
            // 
            // tabOutput
            // 
            this.tabOutput.Controls.Add(this.tabOutputText);
            this.tabOutput.Location = new System.Drawing.Point(4, 25);
            this.tabOutput.Name = "tabOutput";
            this.tabOutput.Padding = new System.Windows.Forms.Padding(3);
            this.tabOutput.Size = new System.Drawing.Size(750, 104);
            this.tabOutput.TabIndex = 0;
            this.tabOutput.Text = "Output";
            this.tabOutput.UseVisualStyleBackColor = true;
            // 
            // tabMessages
            // 
            this.tabMessages.Controls.Add(this.tabMessagesText);
            this.tabMessages.Location = new System.Drawing.Point(4, 25);
            this.tabMessages.Name = "tabMessages";
            this.tabMessages.Padding = new System.Windows.Forms.Padding(3);
            this.tabMessages.Size = new System.Drawing.Size(750, 104);
            this.tabMessages.TabIndex = 1;
            this.tabMessages.Text = "Messages";
            this.tabMessages.UseVisualStyleBackColor = true;
            // 
            // textEditor
            // 
            this.textEditor.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.textEditor.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.textEditor.Location = new System.Drawing.Point(12, 69);
            this.textEditor.Name = "textEditor";
            this.textEditor.Size = new System.Drawing.Size(754, 238);
            this.textEditor.TabIndex = 3;
            this.textEditor.Text = "";
            this.textEditor.KeyDown += new System.Windows.Forms.KeyEventHandler(this.textEditor_KeyDown);
            // 
            // menuSaveFile
            // 
            this.menuSaveFile.Name = "menuSaveFile";
            this.menuSaveFile.Size = new System.Drawing.Size(224, 26);
            this.menuSaveFile.Text = "Save file";
            this.menuSaveFile.Click += new System.EventHandler(this.menuSaveFile_Click);
            // 
            // tabOutputText
            // 
            this.tabOutputText.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.tabOutputText.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.tabOutputText.Location = new System.Drawing.Point(0, 0);
            this.tabOutputText.Name = "tabOutputText";
            this.tabOutputText.ReadOnly = true;
            this.tabOutputText.Size = new System.Drawing.Size(750, 104);
            this.tabOutputText.TabIndex = 0;
            this.tabOutputText.Text = "";
            // 
            // tabMessagesText
            // 
            this.tabMessagesText.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.tabMessagesText.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.tabMessagesText.Location = new System.Drawing.Point(0, 0);
            this.tabMessagesText.Name = "tabMessagesText";
            this.tabMessagesText.ReadOnly = true;
            this.tabMessagesText.Size = new System.Drawing.Size(750, 108);
            this.tabMessagesText.TabIndex = 0;
            this.tabMessagesText.Text = "";
            // 
            // Window
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(782, 453);
            this.Controls.Add(this.textEditor);
            this.Controls.Add(this.tabPanel);
            this.Controls.Add(this.strip);
            this.Controls.Add(this.menu);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.MainMenuStrip = this.menu;
            this.Name = "Window";
            this.Text = "VABA ABKR";
            this.Load += new System.EventHandler(this.Window_Load);
            this.menu.ResumeLayout(false);
            this.menu.PerformLayout();
            this.strip.ResumeLayout(false);
            this.strip.PerformLayout();
            this.tabPanel.ResumeLayout(false);
            this.tabOutput.ResumeLayout(false);
            this.tabMessages.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip menu;
        private System.Windows.Forms.ToolStripMenuItem menuFile;
        private System.Windows.Forms.ToolStripMenuItem menuNewFile;
        private System.Windows.Forms.ToolStripMenuItem menuOpenFile;
        private System.Windows.Forms.ToolStripMenuItem menuExit;
        private System.Windows.Forms.ToolStrip strip;
        private System.Windows.Forms.ToolStripButton stripExecute;
        private System.Windows.Forms.ToolStripSeparator stripSeparator;
        private System.Windows.Forms.ToolStripLabel stripFilenameLabel;
        private System.Windows.Forms.TabControl tabPanel;
        private System.Windows.Forms.TabPage tabOutput;
        private System.Windows.Forms.TabPage tabMessages;
        private System.Windows.Forms.RichTextBox textEditor;
        private System.Windows.Forms.ToolStripMenuItem menuSaveFile;
        private System.Windows.Forms.RichTextBox tabOutputText;
        private System.Windows.Forms.RichTextBox tabMessagesText;
    }
}

