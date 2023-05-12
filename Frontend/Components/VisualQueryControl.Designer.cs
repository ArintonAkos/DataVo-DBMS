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
            VQueryPanel = new TableLayoutPanel();
            TablePanel = new TableLayoutPanel();
            splitContainer1 = new SplitContainer();
            TableVisualizerPanel = new Panel();
            dataGridView1 = new DataGridView();
            QueryPanel = new TableLayoutPanel();
            Command = new ComboBox();
            collapsibleTreeView1 = new CollapsibleTreeView();
            VQueryPanel.SuspendLayout();
            TablePanel.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)splitContainer1).BeginInit();
            splitContainer1.Panel1.SuspendLayout();
            splitContainer1.Panel2.SuspendLayout();
            splitContainer1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)dataGridView1).BeginInit();
            QueryPanel.SuspendLayout();
            SuspendLayout();
            // 
            // VQueryPanel
            // 
            VQueryPanel.Anchor = AnchorStyles.Top | AnchorStyles.Bottom | AnchorStyles.Left | AnchorStyles.Right;
            VQueryPanel.ColumnCount = 2;
            VQueryPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 150F));
            VQueryPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
            VQueryPanel.Controls.Add(TablePanel, 1, 0);
            VQueryPanel.Controls.Add(QueryPanel, 0, 0);
            VQueryPanel.Location = new Point(0, 0);
            VQueryPanel.Margin = new Padding(2);
            VQueryPanel.Name = "VQueryPanel";
            VQueryPanel.RowCount = 1;
            VQueryPanel.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));
            VQueryPanel.Size = new Size(682, 360);
            VQueryPanel.TabIndex = 1;
            // 
            // TablePanel
            // 
            TablePanel.Anchor = AnchorStyles.Top | AnchorStyles.Bottom | AnchorStyles.Left | AnchorStyles.Right;
            TablePanel.ColumnCount = 1;
            TablePanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
            TablePanel.Controls.Add(splitContainer1, 0, 1);
            TablePanel.Location = new Point(152, 2);
            TablePanel.Margin = new Padding(2);
            TablePanel.Name = "TablePanel";
            TablePanel.RowCount = 2;
            TablePanel.RowStyles.Add(new RowStyle());
            TablePanel.RowStyles.Add(new RowStyle());
            TablePanel.Size = new Size(528, 356);
            TablePanel.TabIndex = 0;
            // 
            // splitContainer1
            // 
            splitContainer1.Dock = DockStyle.Fill;
            splitContainer1.Location = new Point(3, 3);
            splitContainer1.Name = "splitContainer1";
            splitContainer1.Orientation = Orientation.Horizontal;
            // 
            // splitContainer1.Panel1
            // 
            splitContainer1.Panel1.Controls.Add(TableVisualizerPanel);
            // 
            // splitContainer1.Panel2
            // 
            splitContainer1.Panel2.Controls.Add(dataGridView1);
            splitContainer1.Size = new Size(522, 350);
            splitContainer1.SplitterDistance = 174;
            splitContainer1.TabIndex = 0;
            // 
            // TableVisualizerPanel
            // 
            TableVisualizerPanel.BackColor = SystemColors.HighlightText;
            TableVisualizerPanel.Dock = DockStyle.Fill;
            TableVisualizerPanel.Location = new Point(0, 0);
            TableVisualizerPanel.Name = "TableVisualizerPanel";
            TableVisualizerPanel.Size = new Size(522, 174);
            TableVisualizerPanel.TabIndex = 2;
            // 
            // dataGridView1
            // 
            dataGridView1.BackgroundColor = SystemColors.Control;
            dataGridView1.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dataGridView1.Dock = DockStyle.Fill;
            dataGridView1.Location = new Point(0, 0);
            dataGridView1.Name = "dataGridView1";
            dataGridView1.RowTemplate.Height = 25;
            dataGridView1.Size = new Size(522, 172);
            dataGridView1.TabIndex = 0;
            // 
            // QueryPanel
            // 
            QueryPanel.ColumnCount = 1;
            QueryPanel.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 50F));
            QueryPanel.Controls.Add(Command, 0, 0);
            QueryPanel.Controls.Add(collapsibleTreeView1, 0, 1);
            QueryPanel.Dock = DockStyle.Fill;
            QueryPanel.Location = new Point(2, 2);
            QueryPanel.Margin = new Padding(2);
            QueryPanel.Name = "QueryPanel";
            QueryPanel.RowCount = 2;
            QueryPanel.RowStyles.Add(new RowStyle(SizeType.Percent, 8.611111F));
            QueryPanel.RowStyles.Add(new RowStyle(SizeType.Percent, 91.38889F));
            QueryPanel.Size = new Size(146, 356);
            QueryPanel.TabIndex = 1;
            // 
            // Command
            // 
            Command.Dock = DockStyle.Fill;
            Command.FormattingEnabled = true;
            Command.Location = new Point(2, 2);
            Command.Margin = new Padding(2);
            Command.Name = "Command";
            Command.Size = new Size(142, 23);
            Command.TabIndex = 0;
            // 
            // collapsibleTreeView1
            // 
            collapsibleTreeView1.Dock = DockStyle.Fill;
            collapsibleTreeView1.Location = new Point(3, 33);
            collapsibleTreeView1.Name = "collapsibleTreeView1";
            collapsibleTreeView1.Size = new Size(140, 320);
            collapsibleTreeView1.TabIndex = 1;
            // 
            // VisualQueryControl
            // 
            AutoScaleDimensions = new SizeF(96F, 96F);
            AutoScaleMode = AutoScaleMode.Dpi;
            Controls.Add(VQueryPanel);
            Margin = new Padding(2);
            Name = "VisualQueryControl";
            Size = new Size(682, 360);
            VQueryPanel.ResumeLayout(false);
            TablePanel.ResumeLayout(false);
            splitContainer1.Panel1.ResumeLayout(false);
            splitContainer1.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)splitContainer1).EndInit();
            splitContainer1.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)dataGridView1).EndInit();
            QueryPanel.ResumeLayout(false);
            ResumeLayout(false);
        }

        #endregion

        private System.Windows.Forms.TableLayoutPanel VQueryPanel;
        private System.Windows.Forms.TableLayoutPanel TablePanel;
        private System.Windows.Forms.TableLayoutPanel QueryPanel;
        private System.Windows.Forms.ComboBox Command;
        private SplitContainer splitContainer1;
        private Panel TableVisualizerPanel;
        private DataGridView dataGridView1;
        private CollapsibleTreeView collapsibleTreeView1;
    }
}
