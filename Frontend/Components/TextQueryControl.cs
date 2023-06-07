
using System.Windows.Forms;

namespace Frontend.Components
{
    public partial class TextQueryControl : UserControl
    {
        public SqlRichTextBox TextBox { get { return textBox; } }
        public RichTextBox LineNumberTextBox { get { return lineNumberTextBox; } }

        public TextQueryControl()
        {
            InitializeComponent();

            Name = "TextEditor";
        }

        public void AddLineNumbers()
        {
            Point point = new(0, 0);

            int firstIndex = textBox.GetCharIndexFromPosition(point);
            int firstLine = textBox.GetLineFromCharIndex(firstIndex);

            point.X = textBox.Width;
            point.Y = textBox.Height;

            int lastIndex = textBox.GetCharIndexFromPosition(point);
            int lastLine = textBox.GetLineFromCharIndex(lastIndex);

            lineNumberTextBox.SelectionAlignment = HorizontalAlignment.Center;
            lineNumberTextBox.Text = "";

            for (int i = firstLine; i <= lastLine + 1; i++)
            {
                lineNumberTextBox.Text += i + 1 + "\n";
            }
        }

        private void SqlRichTextBox_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Space || e.KeyCode == Keys.Tab)
            {
                textBox.ApplyHighlighting();
            }
        }

        private void SqlRichTextBox_VScroll(object sender, EventArgs e)
        {
            lineNumberTextBox.Text = "";
            AddLineNumbers();
            lineNumberTextBox.Invalidate();
        }

        private void SqlRichTextBox_SelectionChanged(object sender, EventArgs e)
        {
            Point point = textBox.GetPositionFromCharIndex(textBox.SelectionStart);
            if (point.X == 1)
            {
                AddLineNumbers();
            }
        }

        private void LineNumberTextBox_MouseDown(object sender, MouseEventArgs e)
        {
            textBox.Select();
            LineNumberTextBox.DeselectAll();
        }

        private void TextQueryControl_Resize(object sender, EventArgs e)
        {
            AddLineNumbers();
        }
    }
}
