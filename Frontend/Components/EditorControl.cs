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

        private TextQueryControl GetEditorTextControl(int index)
        {
            return (tabControl.TabPages[index].Controls[0].Tag.ToString() == "TextEditor")
                ? (TextQueryControl)tabControl.TabPages[index].Controls[0]
                : null;
        }

        private VisualQueryControl GetEditorVisualQueryControl(int index)
        {
            return (tabControl.TabPages[index].Controls[0].Tag.ToString() == "VQueryEditor")
                ? (VisualQueryControl)tabControl.TabPages[index].Controls[0]
                : null;
        }

        public void CreateTextEditorTab(string filePath)
        {
            TabPage tabPage = new TabPage()
            {
                Text = Path.GetFileName(filePath),
                Name = filePath,
            };

            tabPage.Controls.Add(new TextQueryControl()
            {
                Dock = DockStyle.Fill
            });

            tabControl.TabPages.Add(tabPage);
        }

        public void CreateVisualQueryEditorTab(string database)
        {
            TabPage tabPage = new TabPage()
            {
                Text = $"VQ: {database}",
                Name = database,
            };

            tabPage.Controls.Add(new VisualQueryControl()
            {
                Dock = DockStyle.Fill
            });

            tabControl.TabPages.Add(tabPage);
        }

        public void CreateTextEditorTabWithContent(string filePath)
        {
            CreateTextEditorTab(filePath);

            RichTextBox textBox = GetEditorTextControl(tabControl.TabCount - 1).TextBox;
            textBox.Text = File.ReadAllText(filePath);
        }

        public string GetActiveTabContent()
        {
            if (tabControl.SelectedIndex >= 0)
            {
                TextQueryControl textControl = GetEditorTextControl(tabControl.SelectedIndex);
                if (textControl != null)
                {
                    return string.IsNullOrEmpty(textControl.TextBox.SelectedText)
                        ? textControl.TextBox.Text
                        : textControl.TextBox.SelectedText;
                }
            }

            return string.Empty;
        }

        public void SaveActiveTabContent()
        {
            if (tabControl.SelectedIndex >= 0)
            {
                TextQueryControl textControl = GetEditorTextControl(tabControl.SelectedIndex);
                if (textControl != null)
                {
                    File.WriteAllText(tabControl.SelectedTab.Name, textControl.TextBox.Text);
                }
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
