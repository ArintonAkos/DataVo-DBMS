using Frontend.Services;

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

        private TextQueryControl? GetEditorTextControl(int index)
        {
            return (tabControl.TabPages[index].Controls[0].Name == "TextEditor")
                ? (TextQueryControl)tabControl.TabPages[index].Controls[0]
                : null;
        }

        public async Task<TextQueryControl> CreateTextEditorTab(string filePath)
        {
            TabPage tabPage = new()
            {
                Text = Path.GetFileName(filePath),
                Name = filePath,
                Tag = await AuthService.CreateSession()
            };

            TextQueryControl textControl = new()
            {
                Dock = DockStyle.Fill
            };

            tabPage.Controls.Add(textControl);

            tabControl.TabPages.Add(tabPage);

            return textControl;
        }

        public async Task<VisualQueryControl> CreateVisualQueryEditorTab(string database)
        {
            TabPage tabPage = new()
            {
                Text = $"VQ: {database}",
                Name = database,
                Tag = await AuthService.CreateSession()
            };

            VisualQueryControl vqueryControl = new()
            {
                Dock = DockStyle.Fill
            };

            tabPage.Controls.Add(vqueryControl);

            tabControl.TabPages.Add(tabPage);

            return vqueryControl;
        }

        public async void CreateTextEditorTabWithContent(string filePath)
        {
            TextQueryControl textControl = await CreateTextEditorTab(filePath);

            textControl.TextBox.Text = File.ReadAllText(filePath);
        }

        public string GetActiveTabContent()
        {
            if (tabControl.SelectedIndex >= 0)
            {
                TextQueryControl? textControl = GetEditorTextControl(tabControl.SelectedIndex);

                if (textControl != null)
                {
                    return string.IsNullOrEmpty(textControl.TextBox.SelectedText)
                        ? textControl.TextBox.Text
                        : textControl.TextBox.SelectedText;
                }
            }

            return string.Empty;
        }

        public Guid GetActiveTabSession()
        {
            return tabControl.SelectedIndex >= 0
                ? (Guid)tabControl.SelectedTab.Tag
                : Guid.Empty;
        }

        public void SaveActiveTabContent()
        {
            if (tabControl.SelectedIndex >= 0)
            {
                TextQueryControl? textControl = GetEditorTextControl(tabControl.SelectedIndex);

                if (textControl != null)
                {
                    File.WriteAllText(tabControl.SelectedTab.Name, textControl.TextBox.Text);
                }
            }
        }

        private void TabControl_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Modifiers == Keys.Control && e.KeyCode == Keys.S)
            {
                SaveActiveTabContent();
            }
        }

        private void TabControl_MouseClicked(object sender, MouseEventArgs e)
        {
            var tabControl = sender as TabControl;
            var tabs = tabControl!.TabPages;

            if (e.Button == MouseButtons.Middle)
            {
                tabs.Remove(tabs.Cast<TabPage>()
                        .Where((t, i) => tabControl.GetTabRect(i).Contains(e.Location))
                        .First());
            }
        }
    }
}
