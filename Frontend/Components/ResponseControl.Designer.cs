namespace Frontend.Components
{
    partial class ResponseControl
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
            this.OutputTabPanel = new System.Windows.Forms.TabPage();
            this.OutputTextbox = new System.Windows.Forms.RichTextBox();
            this.MessagesTabPanel = new System.Windows.Forms.TabPage();
            this.MessagesTextbox = new System.Windows.Forms.RichTextBox();
            this.tabControl.SuspendLayout();
            this.OutputTabPanel.SuspendLayout();
            this.MessagesTabPanel.SuspendLayout();
            this.SuspendLayout();
            // 
            // tabControl
            // 
            this.tabControl.Controls.Add(this.OutputTabPanel);
            this.tabControl.Controls.Add(this.MessagesTabPanel);
            this.tabControl.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tabControl.Location = new System.Drawing.Point(0, 0);
            this.tabControl.Name = "tabControl";
            this.tabControl.SelectedIndex = 0;
            this.tabControl.Size = new System.Drawing.Size(861, 358);
            this.tabControl.TabIndex = 3;
            // 
            // OutputTabPanel
            // 
            this.OutputTabPanel.Controls.Add(this.OutputTextbox);
            this.OutputTabPanel.Location = new System.Drawing.Point(4, 25);
            this.OutputTabPanel.Name = "OutputTabPanel";
            this.OutputTabPanel.Padding = new System.Windows.Forms.Padding(3);
            this.OutputTabPanel.Size = new System.Drawing.Size(853, 329);
            this.OutputTabPanel.TabIndex = 0;
            this.OutputTabPanel.Text = "Output";
            this.OutputTabPanel.UseVisualStyleBackColor = true;
            // 
            // OutputTextbox
            // 
            this.OutputTextbox.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.OutputTextbox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.OutputTextbox.Location = new System.Drawing.Point(3, 3);
            this.OutputTextbox.Name = "OutputTextbox";
            this.OutputTextbox.ReadOnly = true;
            this.OutputTextbox.Size = new System.Drawing.Size(847, 323);
            this.OutputTextbox.TabIndex = 0;
            this.OutputTextbox.Text = "";
            // 
            // MessagesTabPanel
            // 
            this.MessagesTabPanel.Controls.Add(this.MessagesTextbox);
            this.MessagesTabPanel.Location = new System.Drawing.Point(4, 25);
            this.MessagesTabPanel.Name = "MessagesTabPanel";
            this.MessagesTabPanel.Padding = new System.Windows.Forms.Padding(3);
            this.MessagesTabPanel.Size = new System.Drawing.Size(853, 329);
            this.MessagesTabPanel.TabIndex = 1;
            this.MessagesTabPanel.Text = "Messages";
            this.MessagesTabPanel.UseVisualStyleBackColor = true;
            // 
            // MessagesTextbox
            // 
            this.MessagesTextbox.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.MessagesTextbox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.MessagesTextbox.Location = new System.Drawing.Point(3, 3);
            this.MessagesTextbox.Name = "MessagesTextbox";
            this.MessagesTextbox.ReadOnly = true;
            this.MessagesTextbox.Size = new System.Drawing.Size(847, 323);
            this.MessagesTextbox.TabIndex = 0;
            this.MessagesTextbox.Text = "";
            // 
            // ResponseControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 16F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.Controls.Add(this.tabControl);
            this.Name = "ResponseControl";
            this.Size = new System.Drawing.Size(861, 358);
            this.tabControl.ResumeLayout(false);
            this.OutputTabPanel.ResumeLayout(false);
            this.MessagesTabPanel.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TabControl tabControl;
        private System.Windows.Forms.TabPage OutputTabPanel;
        private System.Windows.Forms.RichTextBox OutputTextbox;
        private System.Windows.Forms.TabPage MessagesTabPanel;
        private System.Windows.Forms.RichTextBox MessagesTextbox;
    }
}
