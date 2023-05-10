using System.IO;
using System.Linq;
using System.Windows.Forms;

namespace Frontend.Components
{
    public partial class EditorControl : UserControl
    {
        public TabControl TabControl { get { return tabControl; } }

        public EditorControl()
        {
            InitializeComponent();

            tabControl.TabPages.Clear();
        }

        private RichTextBox GetTextEditorTextbox(int index)
        {
            if (tabControl.TabPages[index].Controls[0].Tag == "TextEditor")
            {
                TextQueryControl txEdCtrl = (TextQueryControl)tabControl.TabPages[index].Controls[0];
                return txEdCtrl.TextBox;
            }

            return null;
        }

        public void CreateTextEditorTab(string filePath)
        {
            TabPage tabPage = new TabPage(Path.GetFileName(filePath));

            tabPage.Controls.Add(new TextQueryControl()
            {
                Name = filePath,
                Dock = DockStyle.Fill
            });

            tabControl.TabPages.Add(tabPage);
        }

        public void CreateTextEditorTabWithContent(string filePath)
        {
            CreateTextEditorTab(filePath);

            RichTextBox textBox = GetTextEditorTextbox(tabControl.TabCount - 1);
            textBox.Text = File.ReadAllText(filePath);
        }

        public string GetActiveTabContent()
        {
            if (tabControl.SelectedIndex >= 0)
            {
                RichTextBox textBox = GetTextEditorTextbox(tabControl.SelectedIndex);
                if (textBox != null)
                {
                    return string.IsNullOrEmpty(textBox.SelectedText) ? textBox.Text : textBox.SelectedText;
                }
            }

            return string.Empty;
        }

        public void SaveActiveTabContent()
        {
            if (tabControl.SelectedIndex >= 0)
            {
                RichTextBox textBox = GetTextEditorTextbox(tabControl.SelectedIndex);
                File.WriteAllText(tabControl.SelectedTab.Name, textBox.Text);
            }
        }

        private void tabControl_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Modifiers == Keys.Control && e.KeyCode == Keys.S)
            {
                SaveActiveTabContent();
            }
        }

        private void tabControl_MouseClicked(object sender, MouseEventArgs e)
        {
            var tabControl = sender as TabControl;
            var tabs = tabControl.TabPages;

            if (e.Button == MouseButtons.Middle)
            {
                tabs.Remove(tabs.Cast<TabPage>()
                        .Where((t, i) => tabControl.GetTabRect(i).Contains(e.Location))
                        .First());
            }
        }
    }
}
