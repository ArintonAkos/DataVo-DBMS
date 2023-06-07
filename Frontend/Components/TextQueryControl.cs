
namespace Frontend.Components
{
    public partial class TextQueryControl : UserControl
    {
        public SqlRichTextBox TextBox { get { return textBox; } }
        public ListBox LineCounter { get { return listBox1; } }

        public TextQueryControl()
        {
            InitializeComponent();

            Name = "TextEditor";
        }

        private void SqlRichTextBox_TextChanged(object sender, EventArgs e)
        {
            LineCounter.Items.Clear();

            int count = TextBox.Lines.Length;
            for (int i = 1; i <= count; i++)
            {
                LineCounter.Items.Add(i.ToString());
            }
        }
    }
}
