namespace Frontend.Components
{
    partial class TextQueryControl
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
            tableLayoutPanel1 = new TableLayoutPanel();
            textBox = new SqlRichTextBox();
            lineNumberTextBox = new RichTextBox();
            tableLayoutPanel1.SuspendLayout();
            SuspendLayout();
            // 
            // tableLayoutPanel1
            // 
            tableLayoutPanel1.ColumnCount = 2;
            tableLayoutPanel1.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 57F));
            tableLayoutPanel1.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100F));
            tableLayoutPanel1.Controls.Add(textBox, 1, 0);
            tableLayoutPanel1.Controls.Add(lineNumberTextBox, 0, 0);
            tableLayoutPanel1.Dock = DockStyle.Fill;
            tableLayoutPanel1.Location = new Point(0, 0);
            tableLayoutPanel1.Margin = new Padding(3, 4, 3, 4);
            tableLayoutPanel1.Name = "tableLayoutPanel1";
            tableLayoutPanel1.RowCount = 1;
            tableLayoutPanel1.RowStyles.Add(new RowStyle(SizeType.Percent, 100F));
            tableLayoutPanel1.Size = new Size(839, 533);
            tableLayoutPanel1.TabIndex = 0;
            // 
            // textBox
            // 
            textBox.BorderStyle = BorderStyle.None;
            textBox.Dock = DockStyle.Fill;
            textBox.Location = new Point(60, 4);
            textBox.Margin = new Padding(3, 4, 3, 4);
            textBox.Name = "textBox";
            textBox.Size = new Size(776, 525);
            textBox.TabIndex = 0;
            textBox.Text = "";
            textBox.SelectionChanged += SqlRichTextBox_SelectionChanged;
            textBox.VScroll += SqlRichTextBox_VScroll;
            textBox.KeyDown += SqlRichTextBox_KeyDown;
            // 
            // lineNumberTextBox
            // 
            lineNumberTextBox.BorderStyle = BorderStyle.None;
            lineNumberTextBox.Dock = DockStyle.Fill;
            lineNumberTextBox.Location = new Point(3, 3);
            lineNumberTextBox.Name = "lineNumberTextBox";
            lineNumberTextBox.ReadOnly = true;
            lineNumberTextBox.ScrollBars = RichTextBoxScrollBars.None;
            lineNumberTextBox.Size = new Size(51, 527);
            lineNumberTextBox.TabIndex = 1;
            lineNumberTextBox.Text = "";
            lineNumberTextBox.MouseDown += LineNumberTextBox_MouseDown;
            // 
            // TextQueryControl
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            Controls.Add(tableLayoutPanel1);
            Margin = new Padding(3, 4, 3, 4);
            Name = "TextQueryControl";
            Size = new Size(839, 533);
            Tag = "TextBox";
            Resize += TextQueryControl_Resize;
            tableLayoutPanel1.ResumeLayout(false);
            ResumeLayout(false);
        }

        #endregion

        private TableLayoutPanel tableLayoutPanel1;
        private SqlRichTextBox textBox;
        private RichTextBox lineNumberTextBox;
    }
}
