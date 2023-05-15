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
            tabControl = new TabControl();
            OutputTabPanel = new TabPage();
            MessagesTabPanel = new TabPage();
            MessagesTextbox = new RichTextBox();
            tabControl.SuspendLayout();
            MessagesTabPanel.SuspendLayout();
            SuspendLayout();
            // 
            // tabControl
            // 
            tabControl.Controls.Add(OutputTabPanel);
            tabControl.Controls.Add(MessagesTabPanel);
            tabControl.Dock = DockStyle.Fill;
            tabControl.Location = new Point(0, 0);
            tabControl.Margin = new Padding(3, 4, 3, 4);
            tabControl.Name = "tabControl";
            tabControl.SelectedIndex = 0;
            tabControl.Size = new Size(861, 448);
            tabControl.TabIndex = 3;
            // 
            // OutputTabPanel
            // 
            OutputTabPanel.Location = new Point(4, 29);
            OutputTabPanel.Margin = new Padding(3, 4, 3, 4);
            OutputTabPanel.Name = "OutputTabPanel";
            OutputTabPanel.Padding = new Padding(3, 4, 3, 4);
            OutputTabPanel.Size = new Size(853, 415);
            OutputTabPanel.TabIndex = 0;
            OutputTabPanel.Text = "Output";
            OutputTabPanel.UseVisualStyleBackColor = true;
            // 
            // MessagesTabPanel
            // 
            MessagesTabPanel.Controls.Add(MessagesTextbox);
            MessagesTabPanel.Location = new Point(4, 29);
            MessagesTabPanel.Margin = new Padding(3, 4, 3, 4);
            MessagesTabPanel.Name = "MessagesTabPanel";
            MessagesTabPanel.Padding = new Padding(3, 4, 3, 4);
            MessagesTabPanel.Size = new Size(853, 415);
            MessagesTabPanel.TabIndex = 1;
            MessagesTabPanel.Text = "Messages";
            MessagesTabPanel.UseVisualStyleBackColor = true;
            // 
            // MessagesTextbox
            // 
            MessagesTextbox.BorderStyle = BorderStyle.None;
            MessagesTextbox.Dock = DockStyle.Fill;
            MessagesTextbox.Location = new Point(3, 4);
            MessagesTextbox.Margin = new Padding(3, 4, 3, 4);
            MessagesTextbox.Name = "MessagesTextbox";
            MessagesTextbox.ReadOnly = true;
            MessagesTextbox.Size = new Size(847, 407);
            MessagesTextbox.TabIndex = 0;
            MessagesTextbox.Text = "";
            // 
            // ResponseControl
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            Controls.Add(tabControl);
            Margin = new Padding(3, 4, 3, 4);
            Name = "ResponseControl";
            Size = new Size(861, 448);
            tabControl.ResumeLayout(false);
            MessagesTabPanel.ResumeLayout(false);
            ResumeLayout(false);
        }

        #endregion

        private System.Windows.Forms.TabControl tabControl;
        private System.Windows.Forms.TabPage OutputTabPanel;
        private System.Windows.Forms.TabPage MessagesTabPanel;
        private System.Windows.Forms.RichTextBox MessagesTextbox;
    }
}
