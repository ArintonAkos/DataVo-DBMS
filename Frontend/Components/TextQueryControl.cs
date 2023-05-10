using System.Windows.Forms;

namespace Frontend.Components
{
    public partial class TextQueryControl : UserControl
    {
        public RichTextBox TextBox { get { return textBox; } }

        public TextQueryControl()
        {
            InitializeComponent();

            Tag = "TextEditor";
        }
    }
}
