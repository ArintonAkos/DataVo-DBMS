namespace Frontend.Components
{
    partial class EditorControl
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
            this.tabControl = new System.Windows.Forms.TabControl();
            this.TextQueryTab = new System.Windows.Forms.TabPage();
            this.VisualQueryTab = new System.Windows.Forms.TabPage();
            this.tabControl.SuspendLayout();
            this.SuspendLayout();
            // 
            // tabControl
            // 
            this.tabControl.Controls.Add(this.TextQueryTab);
            this.tabControl.Controls.Add(this.VisualQueryTab);
            this.tabControl.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tabControl.Location = new System.Drawing.Point(0, 0);
            this.tabControl.Name = "tabControl";
            this.tabControl.SelectedIndex = 0;
            this.tabControl.Size = new System.Drawing.Size(747, 376);
            this.tabControl.TabIndex = 1;
            this.tabControl.KeyDown += new System.Windows.Forms.KeyEventHandler(this.TabControl_KeyDown);
            this.tabControl.MouseClick += new System.Windows.Forms.MouseEventHandler(this.TabControl_MouseClicked);
            // 
            // TextQueryTab
            // 
            this.TextQueryTab.Location = new System.Drawing.Point(4, 25);
            this.TextQueryTab.Name = "TextQueryTab";
            this.TextQueryTab.Padding = new System.Windows.Forms.Padding(3);
            this.TextQueryTab.Size = new System.Drawing.Size(739, 347);
            this.TextQueryTab.TabIndex = 0;
            this.TextQueryTab.Text = "textQuery";
            this.TextQueryTab.UseVisualStyleBackColor = true;
            // 
            // VisualQueryTab
            // 
            this.VisualQueryTab.Location = new System.Drawing.Point(4, 25);
            this.VisualQueryTab.Name = "VisualQueryTab";
            this.VisualQueryTab.Size = new System.Drawing.Size(739, 347);
            this.VisualQueryTab.TabIndex = 1;
            this.VisualQueryTab.Text = "vquery";
            this.VisualQueryTab.UseVisualStyleBackColor = true;
            // 
            // EditorControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.tabControl);
            this.Name = "EditorControl";
            this.Size = new System.Drawing.Size(747, 376);
            this.tabControl.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TabControl tabControl;
        private System.Windows.Forms.TabPage TextQueryTab;
        private System.Windows.Forms.TabPage VisualQueryTab;
    }
}
