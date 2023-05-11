namespace Frontend.Components
{
    partial class VisualQueryControl
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
            this.VQueryPanel = new System.Windows.Forms.TableLayoutPanel();
            this.TablePanel = new System.Windows.Forms.TableLayoutPanel();
            this.TableData = new System.Windows.Forms.DataGridView();
            this.OutputData = new System.Windows.Forms.DataGridView();
            this.QueryPanel = new System.Windows.Forms.TableLayoutPanel();
            this.Command = new System.Windows.Forms.ComboBox();
            this.QueryLayout = new System.Windows.Forms.FlowLayoutPanel();
            this.VQueryPanel.SuspendLayout();
            this.TablePanel.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)(this.TableData)).BeginInit();
            ((System.ComponentModel.ISupportInitialize)(this.OutputData)).BeginInit();
            this.QueryPanel.SuspendLayout();
            this.SuspendLayout();
            // 
            // VQueryPanel
            // 
            this.VQueryPanel.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.VQueryPanel.ColumnCount = 2;
            this.VQueryPanel.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 29.13505F));
            this.VQueryPanel.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 70.86494F));
            this.VQueryPanel.Controls.Add(this.TablePanel, 1, 0);
            this.VQueryPanel.Controls.Add(this.QueryPanel, 0, 0);
            this.VQueryPanel.Location = new System.Drawing.Point(0, 0);
            this.VQueryPanel.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.VQueryPanel.Name = "VQueryPanel";
            this.VQueryPanel.RowCount = 1;
            this.VQueryPanel.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 50F));
            this.VQueryPanel.Size = new System.Drawing.Size(682, 360);
            this.VQueryPanel.TabIndex = 1;
            // 
            // TablePanel
            // 
            this.TablePanel.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.TablePanel.ColumnCount = 1;
            this.TablePanel.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 50F));
            this.TablePanel.Controls.Add(this.TableData, 0, 0);
            this.TablePanel.Controls.Add(this.OutputData, 0, 1);
            this.TablePanel.Location = new System.Drawing.Point(200, 2);
            this.TablePanel.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.TablePanel.Name = "TablePanel";
            this.TablePanel.RowCount = 2;
            this.TablePanel.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 51.08225F));
            this.TablePanel.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 48.91775F));
            this.TablePanel.Size = new System.Drawing.Size(480, 356);
            this.TablePanel.TabIndex = 0;
            // 
            // TableData
            // 
            this.TableData.BackgroundColor = System.Drawing.SystemColors.ButtonHighlight;
            this.TableData.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.TableData.Dock = System.Windows.Forms.DockStyle.Fill;
            this.TableData.Location = new System.Drawing.Point(2, 2);
            this.TableData.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.TableData.Name = "TableData";
            this.TableData.RowHeadersWidth = 51;
            this.TableData.RowTemplate.Height = 24;
            this.TableData.Size = new System.Drawing.Size(476, 177);
            this.TableData.TabIndex = 0;
            // 
            // OutputData
            // 
            this.OutputData.BackgroundColor = System.Drawing.SystemColors.ButtonHighlight;
            this.OutputData.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.OutputData.Dock = System.Windows.Forms.DockStyle.Fill;
            this.OutputData.Location = new System.Drawing.Point(2, 183);
            this.OutputData.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.OutputData.Name = "OutputData";
            this.OutputData.RowHeadersWidth = 51;
            this.OutputData.RowTemplate.Height = 24;
            this.OutputData.Size = new System.Drawing.Size(476, 171);
            this.OutputData.TabIndex = 1;
            // 
            // QueryPanel
            // 
            this.QueryPanel.ColumnCount = 1;
            this.QueryPanel.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 50F));
            this.QueryPanel.Controls.Add(this.Command, 0, 0);
            this.QueryPanel.Controls.Add(this.QueryLayout, 0, 1);
            this.QueryPanel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.QueryPanel.Location = new System.Drawing.Point(2, 2);
            this.QueryPanel.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.QueryPanel.Name = "QueryPanel";
            this.QueryPanel.RowCount = 2;
            this.QueryPanel.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 8.611111F));
            this.QueryPanel.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 91.38889F));
            this.QueryPanel.Size = new System.Drawing.Size(194, 356);
            this.QueryPanel.TabIndex = 1;
            // 
            // Command
            // 
            this.Command.Dock = System.Windows.Forms.DockStyle.Fill;
            this.Command.FormattingEnabled = true;
            this.Command.Location = new System.Drawing.Point(2, 2);
            this.Command.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.Command.Name = "Command";
            this.Command.Size = new System.Drawing.Size(190, 21);
            this.Command.TabIndex = 0;
            // 
            // QueryLayout
            // 
            this.QueryLayout.Dock = System.Windows.Forms.DockStyle.Fill;
            this.QueryLayout.Location = new System.Drawing.Point(2, 32);
            this.QueryLayout.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.QueryLayout.Name = "QueryLayout";
            this.QueryLayout.Size = new System.Drawing.Size(190, 322);
            this.QueryLayout.TabIndex = 1;
            // 
            // VisualQueryControl
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(96F, 96F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Dpi;
            this.Controls.Add(this.VQueryPanel);
            this.Margin = new System.Windows.Forms.Padding(2, 2, 2, 2);
            this.Name = "VisualQueryControl";
            this.Size = new System.Drawing.Size(682, 360);
            this.VQueryPanel.ResumeLayout(false);
            this.TablePanel.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.TableData)).EndInit();
            ((System.ComponentModel.ISupportInitialize)(this.OutputData)).EndInit();
            this.QueryPanel.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.TableLayoutPanel VQueryPanel;
        private System.Windows.Forms.TableLayoutPanel TablePanel;
        private System.Windows.Forms.DataGridView TableData;
        private System.Windows.Forms.DataGridView OutputData;
        private System.Windows.Forms.TableLayoutPanel QueryPanel;
        private System.Windows.Forms.ComboBox Command;
        private System.Windows.Forms.FlowLayoutPanel QueryLayout;
    }
}
